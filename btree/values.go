package btree

import (
	"github.com/pkg/errors"
	"github.com/ugorji/go/codec"
)

type Values []interface{}

var handle codec.CborHandle

var ErrNotComparable = errors.New("not comparable")

func (v Values) Compare(o Values) int {
	if len(v) != len(o) {
		panic(ErrNotComparable)
	}
	for i := range v {
		switch v := v[i].(type) {
		case int:
			o, ok := o[i].(int)
			if !ok {
				panic(ErrNotComparable)
			}
			if d := v - o; d != 0 {
				return d
			}
		case string:
			o, ok := o[i].(string)
			if !ok {
				panic(ErrNotComparable)
			}
			switch {
			case v < o:
				return -1
			case v == o:
				return 0
			case v > o:
				return 1
			}
		default:
			panic(ErrNotComparable)
		}
	}
	return 0
}
