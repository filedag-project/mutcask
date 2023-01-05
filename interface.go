package mutcask

import (
	"context"
	"io"

	"golang.org/x/xerrors"
)

var ErrNotFound = xerrors.New("kv: key not found")

type KVDB interface {
	Put(string, []byte) error
	Delete(string) error
	Get(string) ([]byte, error)
	Size(string) (int, error)
	CheckSum(string) (string, error)
	Read(string, io.Writer) (int, error)

	AllKeysChan(context.Context) (chan string, error)
	Close() error
}
