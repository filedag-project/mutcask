package mutcask

import "sync"

var init_vbuf_size = 4<<20 + 4

var (
	hintBuf sync.Pool
	vBuf    sync.Pool
)

func init() {
	hintBuf.New = func() interface{} {
		return make([]byte, HintEncodeSize)
	}
	vBuf.New = func() interface{} {
		return vbuffer(make([]byte, init_vbuf_size))
	}
}

type vbuffer []byte

func (v *vbuffer) size(size int) {
	if cap(*v) < size {
		old := *v
		*v = make([]byte, size, 2*size)
		copy(*v, old)
	}
	*v = (*v)[:size]
}
