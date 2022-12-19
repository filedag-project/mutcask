package mutcask

import (
	"encoding/binary"

	"github.com/fxamacker/cbor/v2"
)

const DEFAULT_SCAN_MAX = 1000
const LOCK_FILE_NAME = "repo.lock"
const KEYS_DIR = "keys"
const V_LOG_DIR = "vlog"
const SYS_STATE_PATH = "sys.json"
const V_LOG_PREFIX = "$vl"
const V_LOG_SUFFIX = ".vlog"
const V_LOG_DEL_PREFIX = "$vld"
const V_LOG_HEADER_SIZE = 8
const SL = 4

type SysState struct {
	Cap      uint64
	Used     uint64
	Trash    uint64
	KTotal   uint64
	ActiveID uint64
	NextID   uint64
	dirty    uint32
}

type response struct {
	err error
	ok  bool
}

type pair struct {
	k []byte
	v []byte
}

func NewPair(k, v []byte) *pair {
	return &pair{k, v}
}

func (p *pair) Key() []byte {
	return p.k
}

func (p *pair) Value() []byte {
	return p.v
}

func (p *pair) Encode() ([]byte, int, int) {
	ks := len(p.k)
	vs := len(p.v)
	buf := make([]byte, V_LOG_HEADER_SIZE+ks+vs)
	binary.LittleEndian.PutUint32(buf[:SL], uint32(ks))
	binary.LittleEndian.PutUint32(buf[SL:V_LOG_HEADER_SIZE], uint32(vs))
	copy(buf[8:V_LOG_HEADER_SIZE+ks], p.k[:])
	copy(buf[V_LOG_HEADER_SIZE+ks:], p.v[:])
	return buf, ks, vs
}

func (p *pair) Decode(buf []byte) error {
	bl := len(buf)
	if bl < V_LOG_HEADER_SIZE {
		return ErrBufSize
	}
	ks := binary.LittleEndian.Uint32(buf[:SL])
	vs := binary.LittleEndian.Uint32(buf[SL:V_LOG_HEADER_SIZE])
	if uint32(bl) != V_LOG_HEADER_SIZE+ks+vs {
		return ErrBufSize
	}
	p.k = make([]byte, ks)
	p.v = make([]byte, vs)
	copy(p.k, buf[V_LOG_HEADER_SIZE:ks])
	copy(p.v, buf[V_LOG_HEADER_SIZE+ks:])
	return nil
}

type pairWithChan struct {
	pair
	res chan *response
}

type vLocate struct {
	Id  uint64
	Off uint64
	Len int
	Ocu int
}

func (v *vLocate) Bytes() ([]byte, error) {
	return cbor.Marshal(v)
}

func (v *vLocate) Decode(d []byte) (err error) {
	err = cbor.Unmarshal(d, v)
	return
}
