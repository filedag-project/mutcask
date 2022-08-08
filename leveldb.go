package mutcask

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var _ KVDB = (*levedbKV)(nil)

type levedbKV struct {
	db *leveldb.DB
}

func NewLevedbKV(dir string) (*levedbKV, error) {
	db, err := leveldb.OpenFile(dir, nil)
	if err != nil {
		return nil, err
	}
	return &levedbKV{
		db: db,
	}, nil
}

func (kv *levedbKV) Put(key string, value []byte) error {
	return kv.db.Put([]byte(key), value, nil)
}

func (kv *levedbKV) Get(key string) ([]byte, error) {
	bs, err := kv.db.Get([]byte(key), nil)
	if err == nil {
		return bs, nil
	}
	if err == errors.ErrNotFound {
		return nil, ErrNotFound
	}
	return nil, err
}

func (kv *levedbKV) CheckSum(key string) (string, error) {
	v, err := kv.Get(key)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(v)
	return hex.EncodeToString(sum[:]), nil
}

func (kv *levedbKV) Size(key string) (int, error) {
	v, err := kv.Get(key)
	if err != nil {
		return -1, err
	}

	return len(v), nil
}

func (kv *levedbKV) Delete(key string) error {
	return kv.db.Delete([]byte(key), nil)
}

func (kv *levedbKV) AllKeysChan(ctx context.Context) (chan string, error) {
	iter := kv.db.NewIterator(nil, nil)
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
}

func (kv *levedbKV) Close() error {
	return kv.db.Close()
}
