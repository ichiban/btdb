package btree

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"

	"github.com/pkg/errors"
)

type Cell struct {
	size int

	Overflow PageNo // Points to the overflow page if it's not large enough. otherwise zero-value.
	Left     PageNo
	Key      []byte
	Value    []byte
}

const CellHeaderSize = 4 + 4 + 4 + 4 // Overflow + Left + Key Size + Value Size

func NewCell(size int) *Cell {
	return &Cell{
		size: size,
	}
}

func (c *Cell) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, &c.Overflow); err != nil {
		return 0, errors.Wrap(err, "failed to read cell overflow")
	}

	if err := binary.Read(r, binary.BigEndian, &c.Left); err != nil {
		return 0, errors.Wrap(err, "failed to read cell left")
	}

	var keySize uint32
	if err := binary.Read(r, binary.BigEndian, &keySize); err != nil {
		return 0, errors.Wrap(err, "failed to read key size")
	}

	var valSize uint32
	if err := binary.Read(r, binary.BigEndian, &valSize); err != nil {
		return 0, errors.Wrap(err, "failed to read value size")
	}

	c.Key = make([]byte, keySize)
	if err := binary.Read(r, binary.BigEndian, &c.Key); err != nil {
		return 0, err
	}

	c.Value = make([]byte, valSize)
	if err := binary.Read(r, binary.BigEndian, &c.Value); err != nil {
		return 0, err
	}

	if _, err := io.CopyN(ioutil.Discard, r, int64(c.size)-int64(CellHeaderSize)-int64(keySize)-int64(valSize)); err != nil {
		return 0, err
	}

	return int64(c.size), nil
}

func (c *Cell) WriteTo(w io.Writer) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, c.size))
	if err := binary.Write(buf, binary.BigEndian, c.Overflow); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.Left); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.Key))); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(c.Value))); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.Key); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.BigEndian, c.Value); err != nil {
		return 0, err
	}
	n, err := w.Write(buf.Bytes()[:c.size])
	return int64(n), err
}
