package mutcask

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	fslock "github.com/ipfs/go-fs-lock"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const lockFileName = "repo.lock"
const keys_dir = "keys"

var _ KVDB = (*mutcask)(nil)

type mutcask struct {
	sync.Mutex
	cfg            *Config
	caskMap        *CaskMap
	createCaskChan chan *createCaskRequst
	close          func()
	closeChan      chan struct{}
	keys           *leveldb.DB
}

func NewMutcask(opts ...Option) (*mutcask, error) {
	m := &mutcask{
		cfg:            defaultConfig(),
		createCaskChan: make(chan *createCaskRequst),
		closeChan:      make(chan struct{}),
	}
	for _, opt := range opts {
		opt(m.cfg)
	}
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
	locked, err := fslock.Locked(repoPath, lockFileName)
	if err != nil {
		return nil, fmt.Errorf("could not check lock status: %w", err)
	}
	if locked {
		return nil, ErrRepoLocked
	}

	unlockRepo, err := fslock.Lock(repoPath, lockFileName)
	if err != nil {
		return nil, fmt.Errorf("could not lock the repo: %w", err)
	}
	// if m.cfg.InitBuf > 0 {
	// 	setInitBuf(m.cfg.InitBuf)
	// }
	db, err := leveldb.OpenFile(filepath.Join(repoPath, keys_dir), nil)
	if err != nil {
		return nil, err
	}
	m.keys = db
	m.caskMap, err = buildCaskMap(m.cfg, db)
	if err != nil {
		return nil, err
	}
	var once sync.Once
	m.close = func() {
		once.Do(func() {
			close(m.closeChan)
			unlockRepo.Close()
		})
	}
	if m.cfg.Migrate {
		doMigrate(m.cfg, m.keys)
	}
	m.handleCreateCask()
	return m, nil
}

func (m *mutcask) handleCreateCask() {
	go func(m *mutcask) {
		ids := []uint32{}
		for {
			select {
			case <-m.closeChan:
				return
			case req := <-m.createCaskChan:
				func() {
					// fmt.Printf("received cask create request, id = %d\n", req.id)
					if hasId(ids, req.id) {
						req.done <- ErrNone
						return
					}
					cask := NewCask(req.id, m.keys)
					var err error
					// create vlog file
					cask.vLog, err = os.OpenFile(filepath.Join(m.cfg.Path, m.vLogName(req.id)), os.O_RDWR|os.O_CREATE, 0644)
					if err != nil {
						req.done <- err
						return
					}
					// // create hintlog file
					// cask.hintLog, err = os.OpenFile(filepath.Join(m.cfg.Path, m.hintLogName(req.id)), os.O_RDWR|os.O_CREATE, 0644)
					// if err != nil {
					// 	req.done <- err
					// 	return
					// }
					m.caskMap.Add(req.id, cask)
					ids = append(ids, req.id)
					req.done <- ErrNone
				}()
			}
		}
	}(m)
}

func (m *mutcask) vLogName(id uint32) string {
	return fmt.Sprintf("%08d%s", id, vLogSuffix)
}

// func (m *mutcask) hintLogName(id uint32) string {
// 	return fmt.Sprintf("%08d%s", id, hintLogSuffix)
// }

func (m *mutcask) Put(key string, value []byte) (err error) {
	id := m.fileID(key)
	var cask *Cask
	var has bool
	cask, has = m.caskMap.Get(id)
	if !has {
		done := make(chan error)
		m.createCaskChan <- &createCaskRequst{
			id:   id,
			done: done,
		}
		if err := <-done; err != ErrNone {
			return err
		}
		cask, _ = m.caskMap.Get(id)
	}

	return cask.Put(key, value)
}

func (m *mutcask) Delete(key string) error {
	id := m.fileID(key)
	cask, has := m.caskMap.Get(id)
	if !has {
		return nil
	}
	return cask.Delete(key)
}

func (m *mutcask) Get(key string) ([]byte, error) {
	hint, err := get_hint(m.keys, key)
	if err != nil {
		return nil, ErrNotFound
	}
	id := m.fileID(key)
	fp := filepath.Join(m.cfg.Path, m.vLogName(id))

	// buf := vBuf.Get().(*vbuffer)
	// buf.size(int(hint.VSize))
	// defer vBuf.Put(buf)
	buf := make([]byte, hint.VSize)

	fh, err := os.Open(fp)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	_, err = fh.ReadAt(buf, int64(hint.VOffset))
	if err != nil {
		return nil, err
	}
	v, err := DecodeValue(buf, true)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (m *mutcask) CheckSum(key string) (string, error) {
	v, err := m.Get(key)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:]), nil
}

func (m *mutcask) Size(key string) (int, error) {
	// id := m.fileID(key)
	// cask, has := m.caskMap.Get(id)
	// if !has {
	// 	return -1, ErrNotFound
	// }
	hint, err := get_hint(m.keys, key)
	if err != nil {
		return -1, ErrNotFound
	}
	return int(hint.VSize - 4), nil
}

func (m *mutcask) Close() error {
	m.caskMap.CloseAll()
	m.close()
	return nil
}
func (m *mutcask) AllKeysChan(ctx context.Context) (chan string, error) {
	iter := m.keys.NewIterator(nil, nil)
	out := make(chan string, 1)
	go func(iter iterator.Iterator, oc chan string) {
		defer iter.Release()
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if !iter.Next() {
				return
			}
			out <- string(iter.Key())
		}
		// Todo: log if has iter.Error()
	}(iter, out)
	return out, nil
	// kc := make(chan string)
	// go func(ctx context.Context, m *mutcask) {
	// 	defer close(kc)
	// 	for _, cask := range m.caskMap.m {
	// 		cask.keyMap.m.Ascend(func(it btree.Item) bool {
	// 			if h, ok := it.(*Hint); ok {
	// 				if h.Deleted {
	// 					return true
	// 				}
	// 				select {
	// 				case <-ctx.Done():
	// 					return false
	// 				default:
	// 					kc <- h.Key
	// 				}
	// 			}
	// 			return false
	// 		})
	// 	}
	// }(ctx, m)
	// return kc, nil
}

func (m *mutcask) fileID(key string) uint32 {
	crc := crc32.ChecksumIEEE([]byte(key))
	return crc % m.cfg.CaskNum
}

type createCaskRequst struct {
	id   uint32
	done chan error
}

func hasId(ids []uint32, id uint32) bool {
	for _, item := range ids {
		if item == id {
			return true
		}
	}
	return false
}
