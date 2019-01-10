package btree

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/pkg/errors"
)

type BTree struct {
	File io.ReadWriteSeeker

	PageSize uint32
	CellSize uint32
}

var ErrNotFound = errors.New("not found")

func (t *BTree) Search(key []byte) ([]byte, error) {
	p, err := t.Get(0)
	if err != nil {
		return nil, err
	}
	l, err := t.searchLeaf(key, p)
	if err != nil {
		return nil, err
	}
	if len(l.Cells) == 0 {
		return nil, ErrNotFound
	}
	i := sort.Search(len(l.Cells), func(i int) bool {
		return bytes.Compare(key, l.Cells[i].Key) >= 0
	})
	if i < len(l.Cells) && bytes.Equal(l.Cells[i].Key, key) {
		return l.Cells[i].Value, nil
	}
	return nil, ErrNotFound
}

var ErrWrongSize = errors.New("wrong size")

// TODO: cache
func (t *BTree) Get(i PageNo) (*Page, error) {
	p := NewPage(int(t.PageSize), int(t.CellSize))
	p.PageNo = i
	if _, err := t.File.Seek(int64(uint32(i)*t.PageSize), io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "failed to seek start")
	}
	n, err := p.ReadFrom(t.File)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read page")
	}
	if n != int64(t.PageSize) {
		return nil, ErrWrongSize
	}
	return p, nil
}

func (t *BTree) Update(p *Page) error {
	if _, err := t.File.Seek(int64(uint32(p.PageNo)*t.PageSize), io.SeekStart); err != nil {
		return errors.Wrap(err, "failed to seek start")
	}
	n, err := p.WriteTo(t.File)
	if err != nil {
		return errors.Wrap(err, "failed to write page")
	}
	if n != int64(t.PageSize) {
		return ErrWrongSize
	}
	return nil
}

func (t *BTree) Create(p *Page) error {
	offset, err := t.File.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "failed to seek end")
	}
	n, err := p.WriteTo(t.File)
	if err != nil {
		return errors.Wrap(err, "failed to write page")
	}
	if n != int64(t.PageSize) {
		return ErrWrongSize
	}
	p.PageNo = PageNo(offset / int64(t.PageSize))
	return nil
}

func (t *BTree) searchLeaf(key []byte, p *Page) (*Page, error) {
	switch p.Type {
	case Leaf:
		return p, nil
	case Branch:
		for _, c := range p.Cells {
			if bytes.Compare(key, c.Key) <= 0 {
				q, err := t.Get(c.Left)
				if err != nil {
					return nil, err
				}
				l, err := t.searchLeaf(key, q)
				if err != nil {
					return nil, err
				}
				return l, nil
			}
		}
		q, err := t.Get(p.Next)
		if err != nil {
			return nil, err
		}
		l, err := t.searchLeaf(key, q)
		if err != nil {
			return nil, err
		}
		return l, nil
	default:
		return nil, errors.New("invalid page type")
	}
}

func (t *BTree) Insert(key, value []byte) error {
	p, err := t.Get(0)
	if err != nil {
		return errors.Wrap(err, "failed to Get root page")
	}

	c := NewCell(int(t.CellSize))
	c.Key = key
	c.Value = value

	m, err := t.insert(p, c)
	if err != nil {
		return errors.Wrap(err, "failed to insert")
	}
	if m != nil {
		// needs a root page
		p := NewPage(int(t.PageSize), int(t.CellSize))
		p.Type = Branch
		p.Cells = p.Cells[:1]
		p.Cells[0] = m
		if err := t.Create(p); err != nil {
			return errors.Wrap(err, "failed to create root page")
		}
	}
	return nil
}

func (t *BTree) split(p *Page) (*Page, *Cell, *Page, error) {
	i := len(p.Cells) / 2
	key := p.Cells[i].Key

	left := NewPage(int(t.PageSize), int(t.CellSize))
	left.Type = p.Type
	left.Next = p.PageNo
	left.Cells.Set(p.Cells[:i])
	if err := t.Create(left); err != nil {
		return nil, nil, nil, err
	}

	right := p
	right.Prev = left.PageNo
	right.Cells.Set(p.Cells[i:])
	if err := t.Update(right); err != nil {
		return nil, nil, nil, err
	}

	c := NewCell(int(t.CellSize))
	c.Key = key
	c.Left = left.PageNo

	return left, c, right, nil
}

func (t *BTree) insert(p *Page, c *Cell) (*Cell, error) {
	var middle *Cell
	if p.Full() {
		l, m, r, err := t.split(p)
		if err != nil {
			return nil, errors.Wrap(err, "failed to split")
		}
		switch {
		case l.Contains(c.Key):
			p = l
		case r.Contains(c.Key):
			p = r
		}
		middle = m
	}
	switch p.Type {
	case Leaf:
		if err := p.Insert(c); err != nil {
			return nil, errors.Wrap(err, "failed to insert")
		}
		if err := t.Update(p); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
		return middle, nil
	case Branch:
		m, err := t.insert(p, c)
		if err != nil {
			return nil, errors.Wrap(err, "failed to insert")
		}
		if m != nil {
			if err := p.Insert(m); err != nil {
				return nil, errors.Wrap(err, "failed to insert")
			}
			if err := t.Update(p); err != nil {
				return nil, errors.Wrap(err, "failed to update")
			}
		}
		return middle, nil
	default:
		return nil, fmt.Errorf("invalid page type: %s", p.Type)
	}
}

// TODO: merge and redistribute
func (t *BTree) Delete(key []byte) error {
	root, err := t.Get(0)
	if err != nil {
		return err
	}
	l, err := t.searchLeaf(key, root)
	if err != nil {
		return err
	}
	if err := l.Delete(key); err != nil {
		return err
	}
	return t.Update(l)
}
