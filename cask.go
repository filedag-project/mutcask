package mutcask

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"github.com/fxamacker/cbor/v2"
	"github.com/google/btree"
	"github.com/syndtr/goleveldb/leveldb"
)

const MaxKeySize = 128

// max key size 128 byte +  1 byte which record the key size + 1 byte delete flag
const HintKeySize = MaxKeySize + 1 + 1

// HintKeySize + 8 bytes value offset + 4 bytes value size
const HintEncodeSize = HintKeySize + 8 + 4

const (
	HintDeletedFlag = byte(1)
)

type HintLV struct {
	VOffset uint64
	VSize   uint32
}

func (h *HintLV) Bytes() (ret []byte, err error) {
	return cbor.Marshal(h)
}

func HintLVFromBytes(b []byte) (h *HintLV, err error) {
	h = &HintLV{}
	err = cbor.Unmarshal(b, h)
	return
}

func get_hint(keys *leveldb.DB, key string) (h *Hint, err error) {
	d, err := keys.Get([]byte(key), nil)
	if err != nil {
		return nil, err
	}
	hlv, err := HintLVFromBytes(d)
	if err != nil {
		return nil, err
	}

	return &Hint{
		Key:     key,
		VOffset: hlv.VOffset,
		VSize:   hlv.VSize,
	}, nil
}

type Hint struct {
	Key     string
	KOffset uint64
	VOffset uint64
	VSize   uint32
	Deleted bool
}

/**
		key		:	value offset	:	value size
		128+1   :   8   			:   4
**/
func (h *Hint) Encode() (ret []byte, err error) {
	kl := len(h.Key)
	if kl > MaxKeySize {
		return nil, ErrKeySizeTooLong
	}
	//ret = *(hintBuf.Get().(*([]byte)))
	ret = make([]byte, HintEncodeSize)
	if h.Deleted {
		ret[0] = HintDeletedFlag
	}
	ret[1] = uint8(kl)
	copy(ret[2:HintKeySize], []byte(h.Key))
	binary.LittleEndian.PutUint64(ret[HintKeySize:HintKeySize+8], h.VOffset)
	binary.LittleEndian.PutUint32(ret[HintKeySize+8:], h.VSize)
	return
}

func (h *Hint) From(buf []byte) (err error) {
	if len(buf) != HintEncodeSize {
		return ErrHintFormat
	}
	if buf[0] == HintDeletedFlag {
		h.Deleted = true
	}
	keylen := uint8(buf[1])
	key := make([]byte, keylen)
	copy(key, buf[2:2+keylen])
	h.Key = string(key)
	h.VOffset = binary.LittleEndian.Uint64(buf[HintKeySize : HintKeySize+8])
	h.VSize = binary.LittleEndian.Uint32(buf[HintKeySize+8:])
	return
}

func (h *Hint) Less(than btree.Item) bool {
	if other, ok := than.(*Hint); ok {
		return bytes.Compare([]byte(h.Key), []byte(other.Key)) < 0
	}
	return false
}

/**
		crc32	:	value
		4 		: 	xxxx
**/
func EncodeValue(v []byte) []byte {
	//buf := vBuf.Get().(*vbuffer)
	buf := make([]byte, 4+len(v))
	//buf.size(4 + len(v))
	c32 := crc32.ChecksumIEEE(v)
	binary.LittleEndian.PutUint32((buf)[0:4], c32)
	copy((buf)[4:], v)
	return buf
}

func DecodeValue(buf []byte, verify bool) (v []byte, err error) {
	if len(buf) <= 4 {
		return nil, ErrValueFormat
	}
	if verify {
		vcheck := binary.LittleEndian.Uint32(buf[:4])

		c32 := crc32.ChecksumIEEE(buf[4:])
		// make sure data not rotted
		if vcheck != c32 {
			return nil, ErrDataRotted
		}
	}
	v = make([]byte, len(buf)-4)
	copy(v, buf[4:])
	return
}

const (
	opread = iota
	opwrite
	opdelete
)

type action struct {
	optype   int
	hint     *Hint
	key      string
	value    []byte
	retvchan chan retv
}

type retv struct {
	err error
}

type Cask struct {
	id        uint32
	close     func()
	closeChan chan struct{}
	actChan   chan *action
	vLog      *os.File
	vLogSize  uint64
	keys      *leveldb.DB
	path      string
	// hintLog     *os.File
	// hintLogSize uint64
	// keyMap      *KeyMap
}

func NewCask(id uint32, kdb *leveldb.DB) *Cask {
	cc := make(chan struct{})
	cask := &Cask{
		id:        id,
		closeChan: cc,
		actChan:   make(chan *action),
		keys:      kdb,
	}
	var once sync.Once
	cask.close = func() {
		once.Do(func() {
			close(cc)
		})
	}
	go func(cask *Cask) {
		for {
			select {
			case <-cask.closeChan:
				return
			case act := <-cask.actChan:
				switch act.optype {
				// case opread:
				// 	cask.doread(act)
				case opdelete:
					cask.dodelete(act)
				case opwrite:
					cask.dowrite(act)
				default:
					fmt.Printf("unkown op type %d\n", act.optype)
				}
			}

		}

	}(cask)
	return cask
}

func (c *Cask) Close() {
	c.close()
	if c.vLog != nil {
		c.vLog.Close()
	}
}

func (c *Cask) Put(key string, value []byte) (err error) {
	retvc := make(chan retv)
	c.actChan <- &action{
		optype:   opwrite,
		key:      key,
		value:    value,
		retvchan: retvc,
	}
	ret := <-retvc

	return ret.err
}

func (c *Cask) Delete(key string) (err error) {
	hint, err := get_hint(c.keys, key)
	if err != nil {
		return nil
	}
	retvc := make(chan retv)
	c.actChan <- &action{
		optype:   opdelete,
		key:      key,
		hint:     hint,
		retvchan: retvc,
	}
	ret := <-retvc

	return ret.err
}

// func (c *Cask) Read(key string) (v []byte, err error) {
// 	hint, err := get_hint(c.keys, key)
// 	if err != nil {
// 		return nil, ErrNotFound
// 	}

// 	retvc := make(chan retv)
// 	c.actChan <- &action{
// 		optype:   opread,
// 		hint:     hint,
// 		retvchan: retvc,
// 	}
// 	ret := <-retvc
// 	if ret.err != nil {
// 		return nil, ret.err
// 	}
// 	return ret.data, nil

// }

func (c *Cask) Size(key string) (int, error) {
	hint, err := get_hint(c.keys, key)
	if err != nil {
		return -1, ErrNotFound
	}
	return int(hint.VSize - 4), nil
}

// func (c *Cask) doread(act *action) {
// 	var err error
// 	defer func() {
// 		if err != nil {
// 			act.retvchan <- retv{err: err}
// 		}
// 	}()
// 	//buf := make([]byte, act.hint.VSize)
// 	buf := vBuf.Get().(*vbuffer)
// 	buf.size(int(act.hint.VSize))
// 	defer vBuf.Put(buf)

// 	_, err = c.vLog.ReadAt(*buf, int64(act.hint.VOffset))
// 	if err != nil {
// 		return
// 	}
// 	v, err := DecodeValue(*buf, true)
// 	if err != nil {
// 		return
// 	}

// 	act.retvchan <- retv{data: v}
// }

func (c *Cask) dodelete(act *action) {
	var err error
	defer func() {
		if err != nil {
			act.retvchan <- retv{err: err}
		}
	}()
	// current only delete key not the data
	c.keys.Delete([]byte(act.hint.Key), nil)
	act.retvchan <- retv{}
}

func (c *Cask) dowrite(act *action) {
	var err error
	defer func() {
		if err != nil {
			act.retvchan <- retv{err: err}
		}
	}()

	var hint = &HintLV{}

	// record file size as value offset
	voffset := c.vLogSize
	// encode value
	encbytes := EncodeValue(act.value)
	// defer vBuf.Put((*vbuffer)(&encbytes))
	// record encoded value size
	vsize := uint32(len(encbytes))
	// write to vlog file
	_, err = c.vLog.Write(encbytes)
	if err != nil {
		return
	}

	// operations for one cask actually did in a sync style, so there is no need to use actomic
	// update vlog file size
	//atomic.AddUint64(&c.vLogSize, uint64(vsize))
	c.vLogSize += uint64(vsize)

	hint.VOffset = voffset
	hint.VSize = vsize

	hd, err := hint.Bytes()
	if err != nil {
		return
	}

	err = c.keys.Put([]byte(act.key), hd, nil)
	if err != nil {
		return
	}

	act.retvchan <- retv{}
}
