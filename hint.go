package mutcask

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/google/btree"
)

const vLogSuffix = ".vlog"
const hintLogSuffix = ".hint"

type CaskMap struct {
	sync.RWMutex
	m map[uint32]*Cask
}

func (cm *CaskMap) Add(id uint32, cask *Cask) {
	cm.Lock()
	defer cm.Unlock()
	cm.m[id] = cask
}

func (cm *CaskMap) Get(id uint32) (c *Cask, b bool) {
	cm.RLock()
	defer cm.RUnlock()
	c, b = cm.m[id]
	return
}

func (cm *CaskMap) CloseAll() {
	for _, cask := range cm.m {
		if cask != nil {
			cask.Close()
		}
	}
}

type KeyMap struct {
	sync.RWMutex
	m *btree.BTree
}

func (km *KeyMap) Add(key string, hint *Hint) {
	km.Lock()
	defer km.Unlock()
	//km.m[key] = hint
	km.m.ReplaceOrInsert(hint)
}

func (km *KeyMap) Get(key string) (h *Hint, b bool) {
	km.RLock()
	defer km.RUnlock()
	//h, b = km.m[key]
	h, b = km.m.Get(&Hint{
		Key: key,
	}).(*Hint)
	return
}

// func (km *KeyMap) Remove(key string) {
// 	km.Lock()
// 	defer km.Unlock()
// 	delete(km.m, key)
// }

func buildKeyMap(hint *os.File, hintBootReadNum int) (*KeyMap, error) {
	finfo, err := hint.Stat()
	if err != nil {
		return nil, err
	}
	if finfo.Size()%HintEncodeSize != 0 {
		return nil, ErrHintLogBroken
	}
	km := &KeyMap{
		m: keyMapInit(),
	}
	//km.m = make(map[string]*Hint)
	hint.Seek(0, 0)
	offset := uint64(0)
	//buf := make([]byte, HintEncodeSize*hintBootReadNum)

	buf := vBuf.Get().(*vbuffer)
	buf.size(HintEncodeSize * hintBootReadNum)
	defer vBuf.Put(buf)
	for {
		n, err := hint.Read(*buf)
		if err != nil && err != io.EOF {
			return nil, err
		}

		if err == io.EOF && n == 0 {
			// read end file
			break
		}
		// should never happened
		if n%HintEncodeSize != 0 {
			return nil, fmt.Errorf("hint file maybe broken, expected %d bytes, read %d bytes", HintEncodeSize, n)
		}
		unreadNum := 0
		for unreadNum < n {
			h := &Hint{}
			if err = h.From((*buf)[unreadNum : unreadNum+HintEncodeSize]); err != nil {
				return nil, err
			}
			unreadNum += HintEncodeSize
			h.KOffset = offset
			offset += HintEncodeSize
			km.Add(h.Key, h)
		}
	}

	return km, nil
}

func buildCaskMap(cfg *Config) (*CaskMap, error) {
	var err error
	dirents, err := os.ReadDir(cfg.Path)
	if err != nil {
		return nil, err
	}
	cm := &CaskMap{}
	cm.m = make(map[uint32]*Cask)
	defer func() {
		if err != nil {
			cm.CloseAll()
		}
	}()

	for _, ent := range dirents {
		if !ent.IsDir() && strings.HasSuffix(ent.Name(), hintLogSuffix) {
			name := strings.TrimSuffix(ent.Name(), hintLogSuffix)
			id, err := strconv.ParseUint(name, 10, 32)
			if err != nil {
				return nil, err
			}
			cask := NewCask(uint32(id))
			cm.Add(uint32(id), cask)
			cask.hintLog, err = os.OpenFile(filepath.Join(cfg.Path, ent.Name()), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			cask.hintLogSize, err = fileSize(cask.hintLog)
			if err != nil {
				return nil, err
			}
			cask.keyMap, err = buildKeyMap(cask.hintLog, cfg.HintBootReadNum)
			if err != nil {
				return nil, err
			}

			cask.vLog, err = os.OpenFile(filepath.Join(cfg.Path, name+vLogSuffix), os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}
			cask.vLogSize, err = fileSize(cask.vLog)
			if err != nil {
				return nil, err
			}
		}
	}

	return cm, nil
}

func fileSize(f *os.File) (uint64, error) {
	finfo, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return uint64(finfo.Size()), nil
}

func keyMapInit() *btree.BTree {
	return btree.New(8)
}
