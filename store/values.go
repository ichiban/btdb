package store

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/ugorji/go/codec"
)

type values []interface{}

var handle codec.CborHandle

var errNotComparable = errors.New("not comparable")

func (v values) compare(o values) int {
	if len(v) != len(o) {
		panic(errNotComparable)
	}
	for i := range v {
		switch v := v[i].(type) {
		case int:
			var w int
			switch o := o[i].(type) {
			case int:
				w = o
			case int64:
				w = int(o)
			case uint64:
				w = int(o)
			default:
				panic(fmt.Errorf("not comparable: index=%d, left=%T, right=%T", i, v, o))
			}
			if d := v - w; d != 0 {
				return d
			}
		case int64:
			var w int64
			switch o := o[i].(type) {
			case int:
				w = int64(o)
			case int64:
				w = o
			case uint64:
				w = int64(o)
			default:
				panic(fmt.Errorf("not comparable: index=%d, left=%T, right=%T", i, v, o))
			}
			if d := v - w; d != 0 {
				return int(d)
			}
		case uint64:
			var w uint64
			switch o := o[i].(type) {
			case int:
				w = uint64(o)
			case int64:
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

func (v values) GoString() string {
	ret := make([]string, len(v))
	for i, v := range v {
		ret[i] = fmt.Sprintf("%#v", v)
	}
	return fmt.Sprintf("[%s]", strings.Join(ret, ", "))
}
