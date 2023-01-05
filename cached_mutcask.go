package mutcask

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"

	lru "github.com/hashicorp/golang-lru"
	"github.com/syndtr/goleveldb/leveldb/errors"
)

var _ KVDB = (*cachedMutcask)(nil)

type cachedMutcask struct {
	db    KVDB
	cache *lru.ARCCache
}

func NewCachedMutcask(cache_num int, opts ...Option) (*cachedMutcask, error) {
	cache, err := lru.NewARC(cache_num)
	if err != nil {
		return nil, err
	}
	mutcask, err := NewMutcask(opts...)
	if err != nil {
		return nil, err
	}
	return &cachedMutcask{
		db:    mutcask,
		cache: cache,
	}, nil
}

func (kv *cachedMutcask) Put(key string, value []byte) error {
	return kv.db.Put(key, value)
}

func (kv *cachedMutcask) Get(key string) ([]byte, error) {
	if v, ok := kv.cache.Get(key); ok {
		return v.([]byte), nil
	}
	bs, err := kv.db.Get(key)
	if err == nil {
		kv.cache.Add(key, bs)
		return bs, nil
	}
	if err == errors.ErrNotFound {
		return nil, ErrNotFound
	}
	return nil, err
}

func (kv *cachedMutcask) Read(string, io.Writer) (int, error) {
	return 0, ErrNoSupport
}

func (kv *cachedMutcask) CheckSum(key string) (string, error) {
	v, err := kv.db.Get(key)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:]), nil
}

func (kv *cachedMutcask) Size(key string) (int, error) {
	return kv.db.Size(key)
}

func (kv *cachedMutcask) Delete(key string) error {
	return kv.db.Delete(key)
}

func (kv *cachedMutcask) AllKeysChan(ctx context.Context) (chan string, error) {
	return kv.db.AllKeysChan(ctx)
}

func (kv *cachedMutcask) Close() error {
	return kv.db.Close()
}
