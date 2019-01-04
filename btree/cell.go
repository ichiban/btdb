package btree

import (
	"encoding/binary"
	"io"
)

type Cell struct {
	Overflow PageNo // Points to the overflow page if it's not large enough. otherwise zero-value.
	Left     PageNo
	Payload  []byte
}

const CellHeaderSize = 4 + 4 + 4

func NewCell(size int) Cell {
	return Cell{
		Payload: make([]byte, 0, size-CellHeaderSize),
	}
}

func (c *Cell) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, &c.Overflow); err != nil {
		return 0, err
	}
	if err := binary.Read(r, binary.BigEndian, &c.Left); err != nil {
		return 0, err
	}
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, err
	}
	c.Payload = c.Payload[:cap(c.Payload)]
	if err := binary.Read(r, binary.BigEndian, &c.Payload); err != nil {
		return 0, err
	}
	c.Payload = c.Payload[:size]
	return int64(CellHeaderSize + cap(c.Payload)), nil
}

func (c *Cell) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, c.Overflow); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, c.Left); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(c.Payload))); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, c.Payload[:cap(c.Payload)]); err != nil {
		return 0, err
	}
	return int64(CellHeaderSize + cap(c.Payload)), nil
}
