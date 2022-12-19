package mutcask

import (
	"context"
	"fmt"
)

var (
	ErrNotFound = fmt.Errorf("kv: key not found")
	ErrNotImpl  = fmt.Errorf("kv: method not implemented")
)

type KVStore interface {
	KVBasic
	KVScanner
	KVAdvance

	Close() error
}

type KVBasic interface {
	Get([]byte) ([]byte, error)
	Put([]byte, []byte) error
	Has([]byte) (bool, error)
	Size([]byte) (int, error)
	Delete([]byte) error
}

type KVScanner interface {
	Scan(prefix []byte, max int) ([]KVPair, error)
	ScanKeys(prefix []byte, max int) ([][]byte, error)
}

type KVAdvance interface {
	CheckSum([]byte) (uint32, error)
	AllKeysChan(context.Context) (chan string, error)
}

type KVPair interface {
	Key() []byte
	Value() []byte
}
