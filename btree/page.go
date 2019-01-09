package btree

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"sort"

	"github.com/pkg/errors"
)

type PageNo uint32

type PageType int8

const (
	Free PageType = iota
	Branch
	Leaf
	Overflow
)

func (t PageType) String() string {
	switch t {
	case Free:
		return "free"
	case Branch:
		return "branch"
	case Leaf:
		return "leaf"
	case Overflow:
		return "overflow"
	default:
		return "unknown"
	}
}

type Page struct {
	size int

	PageNo PageNo
	Type   PageType
	Next   PageNo // rightmost pointer in Branch page. next in Overflow page.
	Cells  Cells
}

const PageHeaderSize = 1 + 3 + 4 + 4 + 4

func NewPage(size, cellSize int) *Page {
	cells := make([]*Cell, (size-PageHeaderSize)/cellSize)
	for i := range cells {
		cells[i] = NewCell(cellSize)
	}
	return &Page{
		size:  size,
		Cells: cells[:0],
	}
}

func (p *Page) ReadFrom(r io.Reader) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, p.size))

	n, err := io.CopyN(buf, r, int64(p.size))
	if err != nil {
		return 0, errors.Wrap(err, "failed to read page")
	}

	if err := binary.Read(buf, binary.BigEndian, &p.Type); err != nil {
		return 0, errors.Wrap(err, "failed to read page type")
	}

	if _, err := io.CopyN(ioutil.Discard, buf, 3); err != nil {
		return 0, errors.Wrap(err, "failed to skip 3 bytes")
	}

	if err := binary.Read(buf, binary.BigEndian, &p.Next); err != nil {
		return 0, errors.Wrap(err, "failed to read next page no")
	}

	if _, err := io.CopyN(ioutil.Discard, buf, 4); err != nil {
		return 0, errors.Wrap(err, "failed to skip 4 bytes")
	}

	var size uint32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return 0, errors.Wrap(err, "failed to read size")
	}

	p.Cells = p.Cells[:size]
	for i := range p.Cells {
		if _, err := p.Cells[i].ReadFrom(buf); err != nil {
			return 0, errors.Wrapf(err, "failed to read cell: %d", i)
		}
	}

	return int64(n), nil
}

func (p *Page) WriteTo(w io.Writer) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, p.size))

	if err := binary.Write(buf, binary.BigEndian, &p.Type); err != nil {
		return 0, err
	}

	if _, err := buf.Write(make([]byte, 3)); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, &p.Next); err != nil {
		return 0, err
	}

	if _, err := buf.Write(make([]byte, 4)); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, uint32(len(p.Cells))); err != nil {
		return 0, err
	}

	for _, c := range p.Cells {
		if _, err := c.WriteTo(buf); err != nil {
			return 0, err
		}
	}

	n, err := w.Write(buf.Bytes()[:p.size])
	return int64(n), err
}

var ErrDuplicateKey = errors.New("duplicate key")

func (p *Page) Insert(c *Cell) error {
	i := sort.Search(len(p.Cells), func(i int) bool {
		return bytes.Compare(c.Key, p.Cells[i].Key) >= 0
	})
	if len(p.Cells) > 0 && i < len(p.Cells) && bytes.Equal(p.Cells[i].Key, c.Key) {
		return ErrDuplicateKey
	}
	p.Cells = p.Cells[:len(p.Cells)+1]
	copy(p.Cells[i+1:], p.Cells[i:])
	p.Cells[i] = c
	return nil
}

func (p *Page) Full() bool {
	return len(p.Cells) == cap(p.Cells)
}

func (p *Page) Contains(key []byte) bool {
	if len(p.Cells) == 0 {
		return false
	}

	i := sort.Search(len(p.Cells), func(i int) bool {
		return bytes.Compare(key, p.Cells[i].Key) >= 0
	})

	return i < len(p.Cells) && bytes.Equal(key, p.Cells[0].Key)
}

type Cells []*Cell

func (c *Cells) Set(s []*Cell) {
	*c = (*c)[:len(s)]
	copy(*c, s)
}
