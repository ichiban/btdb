package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"strings"

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
	size     int
	cellSize int

	PageNo PageNo
	Type   PageType
	Next   PageNo
	Prev   PageNo
	Left   PageNo // leftmost pointer in Branch page
	Cells  []Cell
}

const PageHeaderSize = 1 + 3 + 4 + 4 + 4

func NewPage(size, cellSize int) *Page {
	return &Page{
		size:     size,
		cellSize: cellSize,
		Cells:    make([]Cell, 0, (size-PageHeaderSize)/cellSize),
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

	if err := binary.Read(buf, binary.BigEndian, &p.Prev); err != nil {
		return 0, errors.Wrap(err, "failed to read prev page no")
	}

	if err := binary.Read(buf, binary.BigEndian, &p.Left); err != nil {
		return 0, errors.Wrap(err, "failed to read next page no")
	}

	var size uint32
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return 0, errors.Wrap(err, "failed to read size")
	}

	p.Cells = p.Cells[:size]
	for i := range p.Cells {
		p.Cells[i].size = p.cellSize
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

	if _, err := buf.Write(make([]byte, 4)); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, &p.Left); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, uint32(len(p.Cells))); err != nil {
		return 0, err
	}

	for _, c := range p.Cells {
		c.size = p.cellSize
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
		return p.Cells[i].Key.Compare(c.Key) >= 0
	})
	if len(p.Cells) > 0 && i < len(p.Cells) && p.Cells[i].Key.Compare(c.Key) == 0 {
		return ErrDuplicateKey
	}
	p.Cells = p.Cells[:len(p.Cells)+1]
	copy(p.Cells[i+1:], p.Cells[i:])
	p.Cells[i] = *c
	return nil
}

func (p *Page) WillOverflow() bool {
	return len(p.Cells)+1 > cap(p.Cells)
}

func (p *Page) Contains(key Values) bool {
	if len(p.Cells) == 0 {
		return false
	}

	i := sort.Search(len(p.Cells), func(i int) bool {
		return key.Compare(p.Cells[i].Key) >= 0
	})

	return i < len(p.Cells) && key.Compare(p.Cells[0].Key) == 0
}

func (p *Page) Delete(key Values) error {
	i := sort.Search(len(p.Cells), func(i int) bool {
		return key.Compare(p.Cells[i].Key) >= 0
	})
	if len(p.Cells) == 0 || i >= len(p.Cells) || p.Cells[i].Key.Compare(key) != 0 {
		return ErrNotFound
	}
	p.Cells = p.Cells[:i+copy(p.Cells[i:], p.Cells[i+1:])]
	return nil
}

func (p *Page) InsertSplit(c *Cell) (*Page, error) {
	cells := make([]Cell, len(p.Cells)+1)
	i := sort.Search(len(p.Cells), func(i int) bool {
		return c.Key.Compare(p.Cells[i].Key) <= 0
	})
	if i < len(p.Cells) && c.Key.Compare(p.Cells[i].Key) == 0 {
		return nil, ErrDuplicateKey
	}
	copy(cells[:i], p.Cells[:i])
	cells[i] = *c
	copy(cells[i+1:], p.Cells[i:])

	m := len(cells) / 2

	p.Cells = p.Cells[:m]
	copy(p.Cells, cells[:m])

	r := NewPage(p.size, p.cellSize)
	r.Type = p.Type
	r.Cells = r.Cells[:m]
	copy(r.Cells, cells[m:])

	return r, nil
}

func (p *Page) InsertSplitMiddle(c *Cell) (*Page, Values, error) {
	cells := make([]Cell, len(p.Cells)+1)
	i := sort.Search(len(p.Cells), func(i int) bool {
		return c.Key.Compare(p.Cells[i].Key) <= 0
	})
	if i < len(p.Cells) && c.Key.Compare(p.Cells[i].Key) == 0 {
		return nil, nil, ErrDuplicateKey
	}
	copy(cells[:i], p.Cells[:i])
	cells[i] = *c
	copy(cells[i+1:], p.Cells[i:])

	m := len(cells) / 2

	p.Cells = p.Cells[:m]
	copy(p.Cells, cells[:m])

	r := NewPage(p.size, p.cellSize)
	r.Type = p.Type
	r.Left = cells[m].Right
	r.Cells = r.Cells[:m-1]
	copy(r.Cells, cells[m+1:])

	return r, cells[m].Key, nil
}

func (p *Page) GoString() string {
	ret := make([]string, len(p.Cells))
	for i, c := range p.Cells {
		ret[i] = fmt.Sprintf("%#v", c)
	}
	if p.Type == Leaf {
		return fmt.Sprintf("(%d)%s{%s}", p.PageNo, p.Type, strings.Join(ret, ", "))
	} else {
		return fmt.Sprintf("(%d)%s{%s}->%d", p.PageNo, p.Type, strings.Join(ret, ", "), p.Left)
	}
}

func (p *Page) child(key Values) PageNo {
	i := sort.Search(len(p.Cells), func(i int) bool {
		return key.Compare(p.Cells[i].Key) < 0
	})
	i -= 1
	if i < 0 {
		return p.Left
	}
	return p.Cells[i].Right
}
