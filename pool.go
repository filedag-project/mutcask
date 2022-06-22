package mutcask

import "sync"

const SLICE_SCALE = 4 << 10
const VBUF_4M = 4 << 20
const VBUF_1M = 1 << 20

var init_vbuf_size = VBUF_1M + 4

var (
	hintBuf sync.Pool
	vBuf    sync.Pool
)

func init() {
	hintBuf.New = func() interface{} {
		b := make([]byte, HintEncodeSize)
		return &b
	}
	vBuf.New = func() interface{} {
		b := vbuffer(make([]byte, init_vbuf_size))
		return &b
	}
}

type vbuffer []byte

func (v *vbuffer) size(size int) {
	if cap(*v) < size {
		old := *v
		*v = make([]byte, size, size+SLICE_SCALE)
		copy(*v, old)
	}
	*v = (*v)[:size]
}

func setInitBuf(size int) {
	init_vbuf_size = size + 4
}
