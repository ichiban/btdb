package btree

import (
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
var ErrWrongSize = errors.New("wrong size")

func (t *BTree) Iterator(root PageNo, key Values) *Iterator {
	p, err := t.get(root)
	if err != nil {
		return &Iterator{
			err: err,
		}
	}
	switch p.Type {
	case Leaf:
		i := sort.Search(len(p.Cells), func(i int) bool {
			return key.Compare(p.Cells[i].Key) <= 0
		})
		return &Iterator{
			btree: t,
			page:  p,
			index: i - 1,
		}
	case Branch:
		i := sort.Search(len(p.Cells), func(i int) bool {
			return key.Compare(p.Cells[i].Key) < 0
		})
		i--
		if i < 0 {
			return t.Iterator(p.Left, key)
		}
		return t.Iterator(p.Cells[i].Right, key)
	default:
		return &Iterator{
			err: errors.New("invalid page type"),
		}
	}
}

func (t *BTree) Search(root PageNo, key Values) (Values, error) {
	iter := t.Iterator(root, key)
	if !iter.Next() {
		return nil, iter.Err()
	}
	if iter.Key.Compare(key) != 0 {
		return nil, ErrNotFound
	}
	return iter.Value, nil
}

// TODO: cache
func (t *BTree) get(i PageNo) (*Page, error) {
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

func (t *BTree) update(p *Page) error {
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

func (t *BTree) create(p *Page) error {
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

func (t *BTree) searchLeaf(root PageNo, key Values) (*Page, error) {
	p, err := t.get(root)
	if err != nil {
		return nil, err
	}
	switch p.Type {
	case Leaf:
		return p, nil
	case Branch:
		for _, c := range p.Cells {
			if key.Compare(c.Key) <= 0 {
				return t.searchLeaf(c.Right, key)
			}
		}
		return t.searchLeaf(p.Left, key)
	default:
		return nil, errors.New("invalid page type")
	}
}

func (t *BTree) Insert(root PageNo, key, value Values) (PageNo, error) {
	p, err := t.get(root)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get root page")
	}

	m, err := t.insert(p, &Cell{Payload: Payload{Key: key, Value: value}})
	if err != nil {
		return 0, errors.Wrap(err, "failed to insert")
	}

	if m != nil {
		r := NewPage(int(t.PageSize), int(t.CellSize))
		r.Type = Branch
		r.Left = root
		r.Cells = r.Cells[:1]
		r.Cells[0] = *m
		if err := t.create(r); err != nil {
			return 0, errors.Wrap(err, "failed to create new root")
		}
		p = r
	}

	return p.PageNo, nil
}

func (t *BTree) insert(p *Page, c *Cell) (*Cell, error) {
	switch p.Type {
	case Leaf:
		if !p.WillOverflow() {
			if err := p.Insert(c); err != nil {
				return nil, errors.Wrap(err, "failed to insert")
			}
			if err := t.update(p); err != nil {
				return nil, errors.Wrap(err, "failed to update")
			}
			return nil, nil
		}

		r, err := p.InsertSplit(c)
		if err != nil {
			return nil, errors.Wrap(err, "failed to insert and split")
		}
		r.Next = p.Next
		if err := t.create(r); err != nil {
			return nil, errors.Wrap(err, "failed to create right")
		}
		p.Next = r.PageNo
		if err := t.update(p); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
		return &Cell{Payload: Payload{Key: r.Cells[0].Key, Right: r.PageNo}}, nil
	case Branch:
		n, err := t.get(p.child(c.Key))
		if err != nil {
			return nil, errors.Wrap(err, "failed to get child")
		}
		m, err := t.insert(n, c)
		if err != nil {
			return nil, errors.Wrap(err, "failed to insert")
		}
		if m == nil {
			return nil, nil
		}

		if !p.WillOverflow() {
			if err := p.Insert(m); err != nil {
				return nil, errors.Wrap(err, "failed to insert")
			}
			if err := t.update(p); err != nil {
				return nil, errors.Wrap(err, "failed to update")
			}
			return nil, nil
		}
		r, k, err := p.InsertSplitMiddle(m)
		if err := t.create(r); err != nil {
			return nil, errors.Wrap(err, "failed to create right")
		}
		if err := t.update(p); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
		return &Cell{Payload: Payload{Key: k, Right: r.PageNo}}, nil
	default:
		return nil, fmt.Errorf("invalid page type: %s", p.Type)
	}
}
