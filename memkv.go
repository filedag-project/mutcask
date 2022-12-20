package mutcask

import (
	"context"
	"hash/crc32"
	"sync"
)

var _ KVStore = (*memkv)(nil)

func NewMemkv() KVStore {
	return &memkv{
		m: make(map[string][]byte),
	}
}

// apply KVStore using map for test case usage
type memkv struct {
	sync.RWMutex
	m map[string][]byte
}

func (mkv *memkv) Put(key []byte, value []byte) error {
	mkv.Lock()
	defer mkv.Unlock()
	mkv.m[string(key)] = clone(value)
	return nil
}

func (mkv *memkv) Delete(key []byte) error {
	mkv.Lock()
	defer mkv.Unlock()
	delete(mkv.m, string(key))
	return nil
}

func (mkv *memkv) Get(key []byte) ([]byte, error) {
	mkv.RLock()
	defer mkv.RUnlock()
	bs, ok := mkv.m[string(key)]
	if ok {
		return clone(bs), nil
	}

	return nil, ErrNotFound
}

func (mkv *memkv) CheckSum(key []byte) (uint32, error) {
	mkv.RLock()
	defer mkv.RUnlock()
	bs, ok := mkv.m[string(key)]
	if ok {
		return crc32.ChecksumIEEE(bs), nil
	}

	return 0, ErrNotFound
}

func (mkv *memkv) Size(key []byte) (int, error) {
	mkv.RLock()
	defer mkv.RUnlock()
	bs, ok := mkv.m[string(key)]
	if ok {
		return len(bs), nil
	}

	return -1, ErrNotFound
}

func (mkv *memkv) Has(key []byte) (bool, error) {
	mkv.RLock()
	defer mkv.RUnlock()
	_, ok := mkv.m[string(key)]
	if ok {
		return true, nil
	}

	return false, nil
}

func (mkv *memkv) Scan([]byte, int) ([]Pair, error) {
	return nil, ErrNotImpl
}

func (mkv *memkv) ScanKeys([]byte, int) ([][]byte, error) {
	return nil, ErrNotImpl
}

func (mkv *memkv) AllKeysChan(context.Context) (chan string, error) {
	return nil, ErrNotImpl
}

func (mkv *memkv) Close() error {
	return nil
}

func clone(src []byte) (cp []byte) {
	cp = make([]byte, len(src))
	copy(cp, src)
	return
}
