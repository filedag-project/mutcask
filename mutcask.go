package mutcask

import (
	"context"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	fslock "github.com/ipfs/go-fs-lock"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var _ KVStore = (*mutcask)(nil)

type mutcask struct {
	sync.Mutex
	cfg        *Config
	close      func()
	closeChan  chan struct{}
	appendChan chan *pairWithChan
	keys       *leveldb.DB
	ss         *SysState
	w          *os.File
}

func NewMutcask(opts ...Option) (*mutcask, error) {
	m := &mutcask{
		cfg:        defaultConfig(),
		closeChan:  make(chan struct{}),
		appendChan: make(chan *pairWithChan),
	}
	for _, opt := range opts {
		opt(m.cfg)
	}
	// check if repo has been setup
	repoPath := m.cfg.Path
	if repoPath == "" {
		return nil, ErrPathUndefined
	}
	repo, err := os.Stat(repoPath)
	if err == nil && !repo.IsDir() {
		return nil, ErrPath
	}
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.Mkdir(repoPath, 0755); err != nil {
			return nil, err
		}
	}
	// try to get the repo lock
	locked, err := fslock.Locked(repoPath, LOCK_FILE_NAME)
	if err != nil {
		return nil, fmt.Errorf("could not check lock status: %w", err)
	}
	if locked {
		return nil, ErrRepoLocked
	}

	unlockRepo, err := fslock.Lock(repoPath, LOCK_FILE_NAME)
	if err != nil {
		return nil, fmt.Errorf("could not lock the repo: %w", err)
	}
	// try to read sys state or create one
	var ss *SysState
	ss, err = LoadSys(repoPath)
	if err != nil {
		if os.IsNotExist(err) {
			ss = &SysState{
				Cap:      m.cfg.Capacity,
				ActiveID: 0,
				NextID:   1,
			}
			if err := InitSysJson(repoPath, ss); err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	m.ss = ss
	// make sure vlog dir has been created
	if err := os.Mkdir(filepath.Join(repoPath, V_LOG_DIR), 0755); err != nil {
		if !os.IsExist(err) {
			fmt.Println(err)
		}
	}
	// open active vlog file for append action
	m.w, err = os.OpenFile(VLogPath(repoPath, ss.ActiveID), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	// use leveldb record keys
	db, err := leveldb.OpenFile(filepath.Join(repoPath, KEYS_DIR), nil)
	if err != nil {
		return nil, err
	}
	m.keys = db
	// operation before close
	var once sync.Once
	m.close = func() {
		once.Do(func() {
			m.FlushSys()
			close(m.closeChan)
			unlockRepo.Close()
		})
	}
	// setup append channel for put action
	go func(m *mutcask) {
		for {
			select {
			case <-m.closeChan:
				return
			case p := <-m.appendChan:
				res := &response{}
				err := m.append(&p.pair)
				if err != nil {
					res.err = err
				} else {
					res.ok = true
				}
				p.res <- res
			}
		}
	}(m)
	// setup sys state flush check
	go func(m *mutcask) {
		ticker := time.NewTicker(time.Second * 3)
		for {
			select {
			case <-m.closeChan:
				return
			case <-ticker.C:
				m.FlushSys()
			}
		}
	}(m)
	return m, nil
}

func (m *mutcask) append(kv *pair) error {
	// detect update operation
	var oldVL *vLocate
	if bs, err := m.keys.Get(kv.k, nil); err == nil {
		ovl := &vLocate{}
		if err = ovl.Decode(bs); err == nil {
			oldVL = ovl
		}
	}
	activeId := atomic.LoadUint64(&m.ss.ActiveID)
	wsize, err := m.wsize()
	if err != nil {
		return err
	}
	d, ks, vs := kv.Encode()
	wn, err := m.w.Write(d)
	if err != nil {
		return err
	}
	if len(d) != wn {
		return fmt.Errorf("append data size not match %d != %d", len(d), wn)
	}
	vl := &vLocate{
		Id:  activeId,
		Off: uint64(wsize) + V_LOG_HEADER_SIZE + uint64(ks),
		Len: vs,
		Ocu: wn,
	}
	vlbs, err := vl.Bytes()
	if err != nil {
		return err
	}
	batch := new(leveldb.Batch)
	batch.Put(kv.k, vlbs)
	batch.Put([]byte(TimestampPrefix(fmt.Sprintf("%s_%d", V_LOG_PREFIX, activeId))), kv.k)
	if oldVL != nil {
		batch.Put([]byte(TimestampPrefix(fmt.Sprintf("%s_%d", V_LOG_DEL_PREFIX, oldVL.Id))), kv.k)
	}
	if err := m.keys.Write(batch, nil); err != nil {
		return err
	}

	atomic.AddUint64(&m.ss.KTotal, 1)
	atomic.AddUint64(&m.ss.Used, uint64(wn))
	if oldVL != nil {
		atomic.AddUint64(&m.ss.Trash, uint64(oldVL.Ocu))
	}
	atomic.CompareAndSwapUint32(&m.ss.dirty, 0, 1)

	// check if meets log file size limit
	if wsize+int64(wn) >= m.cfg.MaxLogFileSize {
		nextId := atomic.LoadUint64(&m.ss.NextID)
		f, err := os.OpenFile(VLogPath(m.cfg.Path, nextId), os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			return err
		}
		m.w = f
		atomic.CompareAndSwapUint64(&m.ss.ActiveID, activeId, nextId)
		atomic.AddUint64(&m.ss.NextID, 1)
	}
	return nil
}

func (m *mutcask) wsize() (int64, error) {
	finfo, err := m.w.Stat()
	if err != nil {
		return 0, err
	}
	return finfo.Size(), nil
}

func (m *mutcask) Put(key, value []byte) (err error) {
	resc := make(chan *response)
	m.appendChan <- &pairWithChan{
		pair: pair{
			k: key,
			v: value,
		},
		res: resc,
	}
	ret := <-resc

	return ret.err
}

func (m *mutcask) Delete(key []byte) error {
	// detect if we have this key
	bs, err := m.keys.Get(key, nil)
	if err != nil { // key not exist, but just ignore error
		return nil
	}
	vl := &vLocate{}
	if err = vl.Decode(bs); err != nil {
		return err
	}
	pre := fmt.Sprintf("%s_%d", V_LOG_DEL_PREFIX, vl.Id)
	batch := new(leveldb.Batch)
	batch.Delete(key)
	batch.Put([]byte(TimestampPrefix(pre)), key)
	if err := m.keys.Write(batch, nil); err != nil {
		return err
	}

	atomic.AddUint64(&m.ss.Trash, uint64(vl.Ocu))
	atomic.CompareAndSwapUint32(&m.ss.dirty, 0, 1)
	return nil
}

func (m *mutcask) Get(key []byte) ([]byte, error) {
	bs, err := m.keys.Get(key, nil)
	if err != nil {
		return nil, ErrNotFound
	}
	vl := &vLocate{}
	if err = vl.Decode(bs); err != nil {
		return nil, err
	}

	fp := VLogPath(m.cfg.Path, vl.Id)

	fh, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	buf := make([]byte, vl.Len)
	_, err = fh.ReadAt(buf, int64(vl.Off))
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (m *mutcask) CheckSum(key []byte) (uint32, error) {
	v, err := m.Get(key)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(v), nil
}

func (m *mutcask) Size(key []byte) (int, error) {
	bs, err := m.keys.Get(key, nil)
	if err != nil {
		return 0, ErrNotFound
	}
	vl := &vLocate{}
	if err = vl.Decode(bs); err != nil {
		return 0, err
	}

	return int(vl.Len), nil
}

func (m *mutcask) Has(key []byte) (bool, error) {
	return m.keys.Has(key, nil)
}

func (m *mutcask) Scan(prefix []byte, max int) ([]KVPair, error) {
	return nil, ErrNotImpl
}

func (m *mutcask) ScanKeys(prefix []byte, max int) ([][]byte, error) {
	if max <= 0 {
		max = DEFAULT_SCAN_MAX
	}
	keyList := make([][]byte, 0)
	count := 0
	iter := m.keys.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() && count < DEFAULT_SCAN_MAX {
		count++
		keyList = append(keyList, iter.Key())
	}
	return keyList, nil
}

func (m *mutcask) AllKeysChan(context.Context) (chan string, error) {
	return nil, ErrNotImpl
}

func (m *mutcask) Close() error {
	m.close()
	return nil
}

func (m *mutcask) FlushSys() (err error) {
	if atomic.LoadUint32(&m.ss.dirty) == 1 {
		m.Lock()
		defer m.Unlock()
		if err = FlushSys(m.cfg.Path, m.ss); err == nil {
			atomic.CompareAndSwapUint32(&m.ss.dirty, 1, 0)
			return
		}
	}
	return
}
