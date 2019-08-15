package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/ugorji/go/codec"

	"github.com/pkg/errors"
)

type cell struct {
	size int

	overflow pageNo // Points to the overflow page if it's not large enough. otherwise zero-value.
	Payload
}

const cellHeaderSize = 4 + 4 // overflow + Payload Size

func (c *cell) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, &c.overflow); err != nil {
		return 0, errors.Wrap(err, "failed to read cell overflow")
	}

	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, errors.Wrap(err, "failed to read key size")
	}

	b := bytes.NewBuffer(make([]byte, 0, size))
	if _, err := io.CopyN(b, r, int64(size)); err != nil {
		return 0, err
	}

	d := codec.NewDecoder(b, &handle)
	if err := d.Decode(&c.Payload); err != nil {
		return 0, err
	}

	// TODO: overflow
	if _, err := io.CopyN(ioutil.Discard, r, int64(c.size)-int64(cellHeaderSize)-int64(size)); err != nil {
		return 0, err
	}

	return int64(c.size), nil
}

func (c *cell) WriteTo(w io.Writer) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, c.size))
	if err := binary.Write(buf, binary.BigEndian, c.overflow); err != nil {
		return 0, err
	}

	b := bytes.NewBuffer(nil)
	e := codec.NewEncoder(b, &handle)
	if err := e.Encode(&c.Payload); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(b.Bytes()))); err != nil {
		return 0, err
	}
	if _, err := b.WriteTo(buf); err != nil {
		return 0, err
	}

	n, err := w.Write(buf.Bytes()[:c.size])
	return int64(n), err
}

func (c cell) GoString() string {
	if c.Value == nil {
		return fmt.Sprintf("%#v->%d", c.Key, c.Right)
	} else {
		return fmt.Sprintf("%#v:%#v", c.Key, c.Value)
	}
}

type Payload struct {
	_struct bool   `codec:",uint"`
	Key     values `codec:"1,omitempty"`
	Value   values `codec:"2,omitempty"`
	Right   pageNo `codec:"3,omitempty"`
}
