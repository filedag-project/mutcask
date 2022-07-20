package mutcask

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

var _ KVDB = (*memkv)(nil)

func NewMemkv() KVDB {
	return &memkv{
		m: make(map[string][]byte),
	}
}

// apply KVDB using map for test case usage
type memkv struct {
	sync.RWMutex
	m map[string][]byte
}

func (mkv *memkv) Put(key string, value []byte) error {
	mkv.Lock()
	defer mkv.Unlock()
	mkv.m[key] = clone(value)
	return nil
}

func (mkv *memkv) Delete(key string) error {
	mkv.Lock()
	defer mkv.Unlock()
	delete(mkv.m, key)
	return nil
}

func (mkv *memkv) Get(key string) ([]byte, error) {
	mkv.RLock()
	defer mkv.RUnlock()
	bs, ok := mkv.m[key]
	if ok {
		return clone(bs), nil
	}

	return nil, ErrNotFound
}

func (mkv *memkv) CheckSum(key string) (string, error) {
	mkv.RLock()
	defer mkv.RUnlock()
	bs, ok := mkv.m[key]
	if ok {
		sum := (sha256.Sum256(bs))
		return hex.EncodeToString(sum[:]), nil
	}

	return "", ErrNotFound
}

func (mkv *memkv) Size(key string) (int, error) {
	mkv.RLock()
	defer mkv.RUnlock()
	bs, ok := mkv.m[key]
	if ok {
		return len(bs), nil
	}

	return -1, ErrNotFound
}

func (mkv *memkv) AllKeysChan(ctx context.Context) (chan string, error) {
	kc := make(chan string)
	go func(ctx context.Context, m *memkv) {
		defer close(kc)
		for key := range m.m {
			select {
			case <-ctx.Done():
				return
			default:
				kc <- key
			}
		}
	}(ctx, mkv)
	return kc, nil
}

func (mkv *memkv) Close() error {
	return nil
}

func clone(src []byte) (cp []byte) {
	cp = make([]byte, len(src))
	copy(cp, src)
	return
}
