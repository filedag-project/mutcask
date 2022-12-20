package mutcask

import (
	"context"
	"hash/crc32"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var _ KVStore = (*levedbKV)(nil)

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

func (kv *levedbKV) Put(key []byte, value []byte) error {
	return kv.db.Put(key, value, nil)
}

func (kv *levedbKV) Get(key []byte) ([]byte, error) {
	bs, err := kv.db.Get(key, nil)
	if err == nil {
		return bs, nil
	}
	if err == errors.ErrNotFound {
		return nil, ErrNotFound
	}
	return nil, err
}

func (kv *levedbKV) CheckSum(key []byte) (uint32, error) {
	v, err := kv.Get(key)
	if err != nil {
		return 0, err
	}
	return crc32.ChecksumIEEE(v), nil
}

func (kv *levedbKV) Size(key []byte) (int, error) {
	v, err := kv.Get(key)
	if err != nil {
		return -1, err
	}

	return len(v), nil
}

func (kv *levedbKV) Has(key []byte) (bool, error) {
	has, _ := kv.db.Has(key, nil)
	if has {
		return has, nil
	}

	return false, nil
}

func (kv *levedbKV) Delete(key []byte) error {
	return kv.db.Delete(key, nil)
}

func (kv *levedbKV) Scan(prefix []byte, max int) ([]Pair, error) {
	if max <= 0 {
		max = DEFAULT_SCAN_MAX
	}
	pairList := make([]Pair, 0)
	count := 0
	iter := kv.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() && count < DEFAULT_SCAN_MAX {
		count++
		pairList = append(pairList, Pair{
			K: iter.Key(),
			V: iter.Value(),
		})
	}
	return pairList, nil
}

func (kv *levedbKV) ScanKeys(prefix []byte, max int) ([][]byte, error) {
	if max <= 0 {
		max = DEFAULT_SCAN_MAX
	}
	keyList := make([][]byte, 0)
	count := 0
	iter := kv.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() && count < DEFAULT_SCAN_MAX {
		count++
		keyList = append(keyList, iter.Key())
	}
	return keyList, nil
}

func (kv *levedbKV) AllKeysChan(context.Context) (chan string, error) {
	return nil, ErrNotImpl
}

func (kv *levedbKV) Close() error {
	return kv.db.Close()
}
