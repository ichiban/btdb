package btree

import (
	"fmt"
	"strings"

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
			var w int
			switch o := o[i].(type) {
			case int:
				w = o
			case uint64:
				w = int(o)
			default:
				panic(fmt.Errorf("not comparable: index=%d, left=%T, right=%T", i, v, o))
			}
			if d := v - w; d != 0 {
				return d
			}
		case uint64:
			var w uint64
			switch o := o[i].(type) {
			case int:
				w = uint64(o)
			case uint64:
				w = o
			default:
				panic(fmt.Errorf("not comparable: index=%d, left=%T, right=%T", i, v, o))
			}
			if d := v - w; d != 0 {
				return int(d)
			}
		case string:
			w, ok := o[i].(string)
			if !ok {
				panic(fmt.Errorf("not comparable: index=%d, left=%T, right=%T", i, v, o[i]))
			}
			switch {
			case v < w:
				return -1
			case v == w:
				return 0
			case v > w:
				return 1
			}
		default:
			panic(fmt.Errorf("not comparable: index=%d, left=%T, right=%T", i, v, o[i]))
		}
	}
	return 0
}

func (v Values) GoString() string {
	ret := make([]string, len(v))
	for i, v := range v {
		ret[i] = fmt.Sprintf("%#v", v)
	}
	return fmt.Sprintf("[%s]", strings.Join(ret, ", "))
}
