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

func (t *BTree) Search(key Values) (Values, error) {
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
		return key.Compare(l.Cells[i].Key) >= 0
	})
	if i < len(l.Cells) && l.Cells[i].Key.Compare(key) == 0 {
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

func (t *BTree) searchLeaf(key Values, p *Page) (*Page, error) {
	switch p.Type {
	case Leaf:
		return p, nil
	case Branch:
		for _, c := range p.Cells {
			if key.Compare(c.Key) <= 0 {
				q, err := t.Get(c.Right)
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
		q, err := t.Get(p.Left)
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

func (t *BTree) Insert(root PageNo, key, value Values) (PageNo, error) {
	p, err := t.Get(root)
	if err != nil {
		return 0, errors.Wrap(err, "failed to Get root page")
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
		if err := t.Create(r); err != nil {
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
			if err := t.Update(p); err != nil {
				return nil, errors.Wrap(err, "failed to update")
			}
			return nil, nil
		}

		r, err := p.InsertSplit(c)
		if err != nil {
			return nil, errors.Wrap(err, "failed to insert and split")
		}
		if err := t.Create(r); err != nil {
			return nil, errors.Wrap(err, "failed to create right")
		}
		p.Next = r.PageNo
		if err := t.Update(p); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
		return &Cell{Payload: Payload{Key: r.Cells[0].Key, Right: r.PageNo}}, nil
	case Branch:
		n, err := t.Get(p.child(c.Key))
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
			if err := t.Update(p); err != nil {
				return nil, errors.Wrap(err, "failed to update")
			}
			return nil, nil
		}
		r, k, err := p.InsertSplitMiddle(m)
		if err := t.Create(r); err != nil {
			return nil, errors.Wrap(err, "failed to create right")
		}
		if err := t.Update(p); err != nil {
			return nil, errors.Wrap(err, "failed to update")
		}
		return &Cell{Payload: Payload{Key: k, Right: r.PageNo}}, nil
	default:
		return nil, fmt.Errorf("invalid page type: %s", p.Type)
	}
}
