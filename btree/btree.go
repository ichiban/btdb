package btree

import (
	"bytes"
	"io"
	"log"
	"sort"

	"github.com/pkg/errors"
)

type BTree struct {
	File io.ReadWriteSeeker

	PageSize uint32
	CellSize uint32
}

func (t *BTree) Search(key []byte) ([]byte, error) {
	p := NewPage(int(t.PageSize), int(t.CellSize))
	if err := t.Get(&p, 0); err != nil {
		return nil, err
	}
	if err := t.searchLeaf(key, &p); err != nil {
		return nil, err
	}
	for _, c := range p.Cells {
		if bytes.HasPrefix(c.Payload, key) {
			return c.Payload[len(key):], nil
		}
	}
	return nil, nil
}

// TODO: cache
func (t *BTree) Get(p *Page, i PageNo) error {
	if _, err := t.File.Seek(int64(uint32(i)*t.PageSize), io.SeekStart); err != nil {
		return errors.Wrap(err, "failed to seek start")
	}
	_, err := p.ReadFrom(t.File)
	return errors.Wrap(err, "failed to read page")
}

func (t *BTree) Set(p *Page, i PageNo) error {
	if _, err := t.File.Seek(int64(uint32(i)*t.PageSize), io.SeekStart); err != nil {
		return errors.Wrap(err, "failed to seek start")
	}
	_, err := p.WriteTo(t.File)
	return errors.Wrap(err, "failed to write page")
}

func (t *BTree) searchLeaf(key []byte, p *Page) error {
	if p.Type == Leaf {
		return nil
	}
	for _, c := range p.Cells {
		if bytes.Compare(key, c.Payload) <= 0 {
			if err := t.Get(p, c.Left); err != nil {
				return err
			}
			return t.searchLeaf(key, p)
		}
	}
	if err := t.Get(p, p.Next); err != nil {
		return err
	}
	return t.searchLeaf(key, p)
}

var ErrOverflow = errors.New("overflow")

func (t *BTree) Insert(key, value []byte) error {
	p := NewPage(int(t.PageSize), int(t.CellSize))
	if err := t.Get(&p, 0); err != nil {
		return errors.Wrap(err, "failed to Get root page")
	}
	if err := t.searchLeaf(key, &p); err != nil {
		return errors.Wrap(err, "failed to search leaf page")
	}
	if len(p.Cells) == cap(p.Cells) {
		// TODO: split
		log.Print("split!")
	}
	i := sort.Search(len(p.Cells), func(i int) bool {
		return bytes.Compare(key, p.Cells[i].Payload) > 0
	})
	p.Cells = p.Cells[:len(p.Cells)+1]
	copy(p.Cells[i+1:], p.Cells[i:])
	payload := append(key, value...)
	if len(payload) > cap(p.Cells[i].Payload) {
		return ErrOverflow
	}
	p.Cells[i].Payload = p.Cells[i].Payload[:len(payload)]
	copy(p.Cells[i].Payload, payload)
	return t.Set(&p, 0)
}
