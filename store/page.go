package store

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

type pageNo uint32

type pageType int8

const (
	free pageType = iota
	branch
	leaf
	overflow
)

func (t pageType) String() string {
	switch t {
	case free:
		return "free"
	case branch:
		return "branch"
	case leaf:
		return "leaf"
	case overflow:
		return "overflow"
	default:
		return "unknown"
	}
}

type Page struct {
	size     int
	cellSize int

	pageNo   pageNo
	pageType pageType
	next     pageNo
	prev     pageNo
	left     pageNo // leftmost pointer in branch page
	cells    []cell
}

const pageHeaderSize = 1 + 3 + 4 + 4 + 4

func NewPage(size, cellSize int) *Page {
	return &Page{
		size:     size,
		cellSize: cellSize,
		cells:    make([]cell, 0, (size-pageHeaderSize)/cellSize),
	}
}

func (p *Page) ReadFrom(r io.Reader) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, p.size))

	n, err := io.CopyN(buf, r, int64(p.size))
	if err != nil {
		return 0, errors.Wrap(err, "failed to read page")
	}

	if err := binary.Read(buf, binary.BigEndian, &p.pageType); err != nil {
		return 0, errors.Wrap(err, "failed to read page type")
	}

	if _, err := io.CopyN(ioutil.Discard, buf, 1); err != nil {
		return 0, errors.Wrap(err, "failed to skip 1 byte")
	}

	var size uint16
	if err := binary.Read(buf, binary.BigEndian, &size); err != nil {
		return 0, errors.Wrap(err, "failed to read size")
	}

	if err := binary.Read(buf, binary.BigEndian, &p.next); err != nil {
		return 0, errors.Wrap(err, "failed to read next page no")
	}

	if err := binary.Read(buf, binary.BigEndian, &p.prev); err != nil {
		return 0, errors.Wrap(err, "failed to read prev page no")
	}

	if err := binary.Read(buf, binary.BigEndian, &p.left); err != nil {
		return 0, errors.Wrap(err, "failed to read left page no")
	}

	p.cells = p.cells[:size]
	for i := range p.cells {
		p.cells[i].size = p.cellSize
		if _, err := p.cells[i].ReadFrom(buf); err != nil {
			return 0, errors.Wrapf(err, "failed to read cell: %d", i)
		}
	}

	return int64(n), nil
}

func (p *Page) WriteTo(w io.Writer) (int64, error) {
	buf := bytes.NewBuffer(make([]byte, 0, p.size))

	if err := binary.Write(buf, binary.BigEndian, &p.pageType); err != nil {
		return 0, err
	}

	if _, err := buf.Write(make([]byte, 1)); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, uint16(len(p.cells))); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, &p.next); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, &p.prev); err != nil {
		return 0, err
	}

	if err := binary.Write(buf, binary.BigEndian, &p.left); err != nil {
		return 0, err
	}

	for _, c := range p.cells {
		c.size = p.cellSize
		if _, err := c.WriteTo(buf); err != nil {
			return 0, err
		}
	}

	n, err := w.Write(buf.Bytes()[:p.size])
	return int64(n), err
}

var ErrDuplicateKey = errors.New("duplicate key")

func (p *Page) Insert(c *cell) error {
	i := sort.Search(len(p.cells), func(i int) bool {
		return p.cells[i].Key.compare(c.Key) >= 0
	})
	if len(p.cells) > 0 && i < len(p.cells) && p.cells[i].Key.compare(c.Key) == 0 {
		return ErrDuplicateKey
	}
	p.cells = p.cells[:len(p.cells)+1]
	copy(p.cells[i+1:], p.cells[i:])
	p.cells[i] = *c
	return nil
}

func (p *Page) willOverflow() bool {
	return len(p.cells)+1 > cap(p.cells)
}

func (p *Page) Contains(key values) bool {
	if len(p.cells) == 0 {
		return false
	}

	i := sort.Search(len(p.cells), func(i int) bool {
		return key.compare(p.cells[i].Key) >= 0
	})

	return i < len(p.cells) && key.compare(p.cells[0].Key) == 0
}

func (p *Page) Delete(key values) error {
	i := sort.Search(len(p.cells), func(i int) bool {
		return key.compare(p.cells[i].Key) >= 0
	})
	if len(p.cells) == 0 || i >= len(p.cells) || p.cells[i].Key.compare(key) != 0 {
		return ErrNotFound
	}
	p.cells = p.cells[:i+copy(p.cells[i:], p.cells[i+1:])]
	return nil
}

func (p *Page) InsertSplit(c *cell) (*Page, error) {
	cells := make([]cell, len(p.cells)+1)
	i := sort.Search(len(p.cells), func(i int) bool {
		return c.Key.compare(p.cells[i].Key) <= 0
	})
	if i < len(p.cells) && c.Key.compare(p.cells[i].Key) == 0 {
		return nil, ErrDuplicateKey
	}
	copy(cells[:i], p.cells[:i])
	cells[i] = *c
	copy(cells[i+1:], p.cells[i:])

	m := len(cells) / 2

	p.cells = p.cells[:m]
	copy(p.cells, cells[:m])

	r := NewPage(p.size, p.cellSize)
	r.pageType = p.pageType
	r.cells = r.cells[:m]
	copy(r.cells, cells[m:])

	return r, nil
}

func (p *Page) InsertSplitMiddle(c *cell) (*Page, values, error) {
	cells := make([]cell, len(p.cells)+1)
	i := sort.Search(len(p.cells), func(i int) bool {
		return c.Key.compare(p.cells[i].Key) <= 0
	})
	if i < len(p.cells) && c.Key.compare(p.cells[i].Key) == 0 {
		return nil, nil, ErrDuplicateKey
	}
	copy(cells[:i], p.cells[:i])
	cells[i] = *c
	copy(cells[i+1:], p.cells[i:])

	m := len(cells) / 2

	p.cells = p.cells[:m]
	copy(p.cells, cells[:m])

	r := NewPage(p.size, p.cellSize)
	r.pageType = p.pageType
	r.left = cells[m].Right
	r.cells = r.cells[:m-1]
	copy(r.cells, cells[m+1:])

	return r, cells[m].Key, nil
}

func (p *Page) GoString() string {
	ret := make([]string, len(p.cells))
	for i, c := range p.cells {
		ret[i] = fmt.Sprintf("%#v", c)
	}
	if p.pageType == leaf {
		return fmt.Sprintf("(%d)%s{%s}", p.pageNo, p.pageType, strings.Join(ret, ", "))
	} else {
		return fmt.Sprintf("(%d)%s{%s}->%d", p.pageNo, p.pageType, strings.Join(ret, ", "), p.left)
	}
}

func (p *Page) child(key values) pageNo {
	i := sort.Search(len(p.cells), func(i int) bool {
		return key.compare(p.cells[i].Key) < 0
	})
	i -= 1
	if i < 0 {
		return p.left
	}
	return p.cells[i].Right
}
