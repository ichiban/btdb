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

func (b *BTree) Iterator(root PageNo, key Values) *Iterator {
	p, err := b.get(root)
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
			btree: b,
			page:  p,
			index: i - 1,
		}
	case Branch:
		i := sort.Search(len(p.Cells), func(i int) bool {
			return key.Compare(p.Cells[i].Key) < 0
		})
		i--
		if i < 0 {
			return b.Iterator(p.Left, key)
		}
		return b.Iterator(p.Cells[i].Right, key)
	default:
		return &Iterator{
			err: errors.New("invalid page type"),
		}
	}
}

func (b *BTree) Search(root PageNo, key Values) (Values, error) {
	iter := b.Iterator(root, key)
	if !iter.Next() {
		return nil, iter.Err()
	}
	if iter.Key.Compare(key) != 0 {
		return nil, ErrNotFound
	}
	return iter.Value, nil
}

// TODO: cache
func (b *BTree) get(i PageNo) (*Page, error) {
	if i == 0 {
		return nil, errors.New("invalid page number")
	}
	p := NewPage(int(b.PageSize), int(b.CellSize))
	p.PageNo = i
	if _, err := b.File.Seek(int64(uint32(i-1)*b.PageSize), io.SeekStart); err != nil {
		return nil, errors.Wrap(err, "failed to seek start")
	}
	n, err := p.ReadFrom(b.File)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read page")
	}
	if n != int64(b.PageSize) {
		return nil, ErrWrongSize
	}
	return p, nil
}

func (b *BTree) update(p *Page) error {
	if _, err := b.File.Seek(int64(uint32(p.PageNo-1)*b.PageSize), io.SeekStart); err != nil {
		return errors.Wrap(err, "failed to seek start")
	}
	n, err := p.WriteTo(b.File)
	if err != nil {
		return errors.Wrap(err, "failed to write page")
	}
	if n != int64(b.PageSize) {
		return ErrWrongSize
	}
	return nil
}

func (b *BTree) create(p *Page) error {
	offset, err := b.File.Seek(0, io.SeekEnd)
	if err != nil {
		return errors.Wrap(err, "failed to seek end")
	}
	n, err := p.WriteTo(b.File)
	if err != nil {
		return errors.Wrap(err, "failed to write page")
	}
	if n != int64(b.PageSize) {
		return ErrWrongSize
	}
	p.PageNo = PageNo(offset/int64(b.PageSize)) + 1
	return nil
}

func (b *BTree) Insert(root PageNo, key, value Values) (PageNo, error) {
	p, err := b.get(root)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get root page")
	}

	m, err := b.insert(p, &Cell{Payload: Payload{Key: key, Value: value}})
	if err != nil {
		return 0, errors.Wrap(err, "failed to insert")
	}

	if m != nil {
		r := NewPage(int(b.PageSize), int(b.CellSize))
		r.Type = Branch
		r.Left = root
		r.Cells = r.Cells[:1]
		r.Cells[0] = *m
		if err := b.create(r); err != nil {
			return 0, errors.Wrap(err, "failed to create new root")
		}
		p = r
	}

	return p.PageNo, nil
}

func (b *BTree) insert(p *Page, c *Cell) (*Cell, error) {
	switch p.Type {
	case Leaf:
		if !p.WillOverflow() {
			if err := p.Insert(c); err != nil {
				return nil, errors.Wrap(err, "failed to insert")
			}
			if err := b.update(p); err != nil {
				return nil, errors.Wrap(err, "failed to update")
			}
			return nil, nil
		}

		r, err := p.InsertSplit(c)
		if err != nil {
			return nil, errors.Wrap(err, "failed to insert and split")
		}
		r.Next = p.Next
		r.Prev = p.PageNo
		if err := b.create(r); err != nil {
			return nil, errors.Wrap(err, "failed to create right")
		}
		p.Next = r.PageNo
		if err := b.update(p); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
		if r.Next != 0 {
			n, err := b.get(r.Next)
			if err != nil {
				return nil, errors.Wrap(err, "failed to get next")
			}
			n.Prev = r.PageNo
			if err := b.update(n); err != nil {
				return nil, errors.Wrap(err, "failed to update next")
			}
		}
		return &Cell{Payload: Payload{Key: r.Cells[0].Key, Right: r.PageNo}}, nil
	case Branch:
		n, err := b.get(p.child(c.Key))
		if err != nil {
			return nil, errors.Wrap(err, "failed to get child")
		}
		m, err := b.insert(n, c)
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
			if err := b.update(p); err != nil {
				return nil, errors.Wrap(err, "failed to update")
			}
			return nil, nil
		}
		r, k, err := p.InsertSplitMiddle(m)
		if err := b.create(r); err != nil {
			return nil, errors.Wrap(err, "failed to create right")
		}
		if err := b.update(p); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
		return &Cell{Payload: Payload{Key: k, Right: r.PageNo}}, nil
	default:
		return nil, fmt.Errorf("invalid page type: %s", p.Type)
	}
}
