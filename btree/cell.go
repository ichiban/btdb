package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/ugorji/go/codec"

	"github.com/pkg/errors"
)

type Cell struct {
	size int

	Overflow PageNo // Points to the overflow page if it's not large enough. otherwise zero-value.
	Payload
}

const CellHeaderSize = 4 + 4 // Overflow + Payload Size

func NewCell(size int) *Cell {
	return &Cell{
		size: size,
	}
}

func (c *Cell) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, &c.Overflow); err != nil {
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
	if _, err := io.CopyN(ioutil.Discard, r, int64(c.size)-int64(CellHeaderSize)-int64(size)); err != nil {
		return 0, err
	}

	return int64(c.size), nil
}

func (c *Cell) WriteTo(w io.Writer) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, c.size))
	if err := binary.Write(buf, binary.BigEndian, c.Overflow); err != nil {
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

func (c *Cell) GoString() string {
	return fmt.Sprintf("%#v", *c)
}

type Payload struct {
	_struct bool   `codec:",uint"`
	Key     Values `codec:"1,omitempty"`
	Value   Values `codec:"2,omitempty"`
	Left    PageNo `codec:"3,omitempty"`
}
