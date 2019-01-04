package btree

import (
	"encoding/binary"
	"io"
	"io/ioutil"
)

type PageNo uint32

type PageType int8

const (
	Free PageType = iota
	Branch
	Leaf
	Overflow
)

type Page struct {
	Type  PageType
	Next  PageNo // rightmost pointer in Branch page. next in Overflow page.
	Cells []Cell
}

const PageHeaderSize = 1 + 3 + 4 + 4 + 4

func NewPage(size, cellSize int) Page {
	cells := make([]Cell, (size-PageHeaderSize)/cellSize)
	for i := range cells {
		cells[i] = NewCell(cellSize)
	}
	return Page{
		Cells: cells[:0],
	}
}

func (p *Page) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, &p.Type); err != nil {
		return 0, err
	}

	if _, err := io.CopyN(ioutil.Discard, r, 3); err != nil {
		return 0, err
	}

	if err := binary.Read(r, binary.BigEndian, &p.Next); err != nil {
		return 0, err
	}

	if _, err := io.CopyN(ioutil.Discard, r, 4); err != nil {
		return 0, err
	}

	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return 0, err
	}

	p.Cells = p.Cells[:cap(p.Cells)]

	var nc int64
	for i := range p.Cells {
		n, err := p.Cells[i].ReadFrom(r)
		if err != nil {
			return 0, err
		}
		nc += n
	}
	p.Cells = p.Cells[:size]

	return int64(PageHeaderSize) + nc, nil
}

func (p *Page) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, &p.Type); err != nil {
		return 0, err
	}

	if _, err := w.Write(make([]byte, 3)); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.BigEndian, &p.Next); err != nil {
		return 0, err
	}

	if _, err := w.Write(make([]byte, 4)); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(p.Cells))); err != nil {
		return 0, err
	}

	var nc int64
	for _, c := range p.Cells[:cap(p.Cells)] {
		n, err := c.WriteTo(w)
		if err != nil {
			return 0, err
		}
		nc += n
	}

	return int64(PageHeaderSize) + nc, nil
}
