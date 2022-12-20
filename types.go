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

type Pair struct {
	K []byte
	V []byte
}

func NewPair(k, v []byte) *Pair {
	return &Pair{
		K: k,
		V: v,
	}
}

func (p *Pair) Encode() ([]byte, int, int) {
	ks := len(p.K)
	vs := len(p.V)
	buf := make([]byte, V_LOG_HEADER_SIZE+ks+vs)
	binary.LittleEndian.PutUint32(buf[:SL], uint32(ks))
	binary.LittleEndian.PutUint32(buf[SL:V_LOG_HEADER_SIZE], uint32(vs))
	copy(buf[8:V_LOG_HEADER_SIZE+ks], p.K[:])
	copy(buf[V_LOG_HEADER_SIZE+ks:], p.V[:])
	return buf, ks, vs
}

func (p *Pair) Decode(buf []byte) error {
	bl := len(buf)
	if bl < V_LOG_HEADER_SIZE {
		return ErrBufSize
	}
	ks := binary.LittleEndian.Uint32(buf[:SL])
	vs := binary.LittleEndian.Uint32(buf[SL:V_LOG_HEADER_SIZE])
	if uint32(bl) != V_LOG_HEADER_SIZE+ks+vs {
		return ErrBufSize
	}
	p.K = make([]byte, ks)
	p.V = make([]byte, vs)
	copy(p.K, buf[V_LOG_HEADER_SIZE:ks])
	copy(p.V, buf[V_LOG_HEADER_SIZE+ks:])
	return nil
}

type pairWithChan struct {
	Pair
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
