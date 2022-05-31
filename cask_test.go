package mutcask

import (
	"bytes"
	"testing"
)

func TestHintEncode(t *testing.T) {
	h1 := &Hint{
		Key:     "QmYs2ezGBk63nzf3vD4EHejWfN5ZkDfTVroS7rwY2JTbnQ",
		VOffset: 4 << 10,
		VSize:   512,
	}
	bs, err := h1.Encode()
	if err != nil {
		t.Fatal()
	}
	h2 := &Hint{}
	err = h2.From(bs)
	if err != nil {
		t.Fatal()
	}
	if h1.Key != h2.Key || h1.VOffset != h2.VOffset || h1.VSize != h2.VSize {
		t.Fatal()
	}
}

func TestValueEncodeDecode(t *testing.T) {
	value := []byte("mutation of bitcask")
	encoded := EncodeValue(value)
	v, err := DecodeValue(encoded, true)
	if err != nil {
		t.Fatal()
	}
	if !bytes.Equal(value, v) {
		t.Fatal()
	}
}

// func TestPool(t *testing.T) {
// 	var s1 = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
// 	fmt.Printf("len: %d, cap: %d\n", len(s1), cap(s1))
// 	s2 := s1[:3]
// 	fmt.Printf("len: %d, cap: %d\n", len(s2), cap(s2))
// 	fmt.Println(s2)
// 	s2 = s2[:cap(s2)]
// 	fmt.Println(s2)
// 	copy(s2[1:], []byte{'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i'})
// 	fmt.Println(s2)
// 	fmt.Printf("len: %d, cap: %d\n", len(s2), cap(s2))
// 	t.Fail()
// }
