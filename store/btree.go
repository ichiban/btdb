package store

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"os"
	"sort"

	"golang.org/x/xerrors"
)

var ErrNotFound = xerrors.New("not found")

var errWrongSize = xerrors.New("wrong size")

type BTree struct {
	header
	file io.ReadWriteSeeker
}

// follows PNG file signature http://www.libpng.org/pub/png/spec/1.2/PNG-Rationale.html#R.PNG-file-signature
var validSignature = [8]byte{
	0x89, // non-ascii
	byte('1'),
	byte('D'),
	byte('B'),
	byte('\r'), // CR
	byte('\n'), // LF
	0x26,       // ctrl-Z
	byte('\n'), // LF
}

const headerSize = 8 + 4 + 4 + 4

var defaultHeader = header{
	Signature: validSignature,
	PageSize:  4096,
	CellSize:  256,
}

type header struct {
	Signature  [8]byte
	PageSize   uint32
	CellSize   uint32
	RootPageNo pageNo
}

func (h *header) Root() int {
	return int(h.RootPageNo)
}

func (h *header) validate() error {
	if !bytes.Equal(h.Signature[:], validSignature[:]) {
		return xerrors.New("invalid signature")
	}
	return nil
}

func (h *header) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, h); err != nil {
		return 0, err
	}
	if err := h.validate(); err != nil {
		return 0, err
	}
	if _, err := io.CopyN(ioutil.Discard, r, int64(h.PageSize)-headerSize); err != nil {
		return 0, err
	}
	return int64(h.PageSize), nil
}

func (h *header) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, h); err != nil {
		return 0, err
	}
	if err := binary.Write(w, binary.BigEndian, make([]byte, h.PageSize-headerSize)); err != nil {
		return 0, err
	}
	return int64(h.PageSize), nil
}

func Create(name string, opts ...createOption) (*BTree, error) {
	f, err := os.Create(name)
	if err != nil {
		return nil, err
	}
	b := BTree{
		header: defaultHeader,
		file:   f,
	}
	for _, o := range opts {
		o(&b)
	}
	if err := b.updateHeader(); err != nil {
		return nil, err
	}
	return &b, nil
}

type createOption func(*BTree)

func PageSize(size uint32) createOption {
	return func(b *BTree) {
		b.PageSize = size
	}
}

func CellSize(size uint32) createOption {
	return func(b *BTree) {
		b.CellSize = size
	}
}

func Open(name string) (*BTree, error) {
	f, err := os.OpenFile(name, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	var h header
	if _, err := h.ReadFrom(f); err != nil {
		return nil, err
	}
	b := BTree{
		header: h,
		file:   f,
	}
	return &b, nil
}

func (b *BTree) Close() error {
	if f, ok := b.file.(io.Closer); ok {
		return f.Close()
	}
	return nil
}

func (b *BTree) UpdateRoot(r int) error {
	b.RootPageNo = pageNo(r)
	return b.updateHeader()
}

func (b *BTree) updateHeader() error {
	if _, err := b.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := b.header.WriteTo(b.file); err != nil {
		return err
	}
	return nil
}

func (b *BTree) First(root int) (*Iterator, error) {
	p, err := b.get(pageNo(root))
	if err != nil {
		return nil, xerrors.Errorf("failed to get root: %w", err)
	}
	switch p.pageType {
	case leaf:
		return &Iterator{
			btree: b,
			page:  p,
			index: -1,
		}, nil
	case branch:
		return b.First(int(p.left))
	default:
		return nil, xerrors.New("invalid page type")
	}
}

func (b *BTree) Iterator(root int, key []interface{}) (*Iterator, error) {
	p, err := b.get(pageNo(root))
	if err != nil {
		return nil, xerrors.Errorf("failed to get root: %w", err)
	}
	switch p.pageType {
	case leaf:
		i := sort.Search(len(p.cells), func(i int) bool {
			return values(key).compare(p.cells[i].Key) <= 0
		})
		return &Iterator{
			btree: b,
			page:  p,
			index: i - 1,
		}, nil
	case branch:
		i := sort.Search(len(p.cells), func(i int) bool {
			return values(key).compare(p.cells[i].Key) < 0
		})
		i--
		if i < 0 {
			return b.Iterator(int(p.left), key)
		}
		return b.Iterator(int(p.cells[i].Right), key)
	default:
		return nil, xerrors.New("invalid page type")
	}
}

func (b *BTree) Search(root int, key []interface{}) ([]interface{}, error) {
	iter, err := b.Iterator(root, key)
	if err != nil {
		return nil, err
	}
	if err := iter.Next(); err != nil {
		return nil, err
	}
	if iter.Key.compare(key) != 0 {
		return nil, ErrNotFound
	}
	return iter.Value, nil
}

func (b *BTree) Update(root int, key, val []interface{}) error {
	iter, err := b.Iterator(root, key)
	if err != nil {
		return err
	}
	if err := iter.Next(); err != nil {
		return err
	}
	if iter.Key.compare(key) != 0 {
		return ErrNotFound
	}
	iter.Value = val
	return b.update(iter.page)
}

// TODO: cache
func (b *BTree) get(i pageNo) (*Page, error) {
	if i == 0 {
		return nil, xerrors.Errorf("invalid page number: %d", i)
	}
	p := NewPage(int(b.PageSize), int(b.CellSize))
	p.pageNo = i
	if _, err := b.file.Seek(int64(uint32(i)*b.PageSize), io.SeekStart); err != nil {
		return nil, xerrors.Errorf("failed to seek start: %w", err)
	}
	n, err := p.ReadFrom(b.file)
	if err != nil {
		return nil, xerrors.Errorf("failed to read page: %v", err)
	}
	if n != int64(b.PageSize) {
		return nil, errWrongSize
	}
	return p, nil
}

func (b *BTree) update(p *Page) error {
	if _, err := b.file.Seek(int64(uint32(p.pageNo)*b.PageSize), io.SeekStart); err != nil {
		return xerrors.Errorf("failed to seek start: %w", err)
	}
	n, err := p.WriteTo(b.file)
	if err != nil {
		return xerrors.Errorf("failed to write page: %w", err)
	}
	if n != int64(b.PageSize) {
		return errWrongSize
	}
	return nil
}

func (b *BTree) create(p *Page) error {
	offset, err := b.file.Seek(0, io.SeekEnd)
	if err != nil {
		return xerrors.Errorf("failed to seek end: %w", err)
	}
	n, err := p.WriteTo(b.file)
	if err != nil {
		return xerrors.Errorf("failed to write page: %w", err)
	}
	if n != int64(b.PageSize) {
		return errWrongSize
	}
	p.pageNo = pageNo(offset / int64(b.PageSize))
	return nil
}

func (b *BTree) CreateRoot() (int, error) {
	r := NewPage(int(b.PageSize), int(b.CellSize))
	r.pageType = leaf
	if err := b.create(r); err != nil {
		return 0, xerrors.Errorf("failed to create new root: %w", err)
	}
	return int(r.pageNo), nil
}

func (b *BTree) Insert(root int, key, value []interface{}) (int, error) {
	p, err := b.get(pageNo(root))
	if err != nil {
		return 0, xerrors.Errorf("failed to get root page: %w", err)
	}

	m, err := b.insert(p, &cell{Payload: Payload{Key: key, Value: value}})
	if err != nil {
		return 0, xerrors.Errorf("failed to insert: %w", err)
	}

	if m != nil {
		r := NewPage(int(b.PageSize), int(b.CellSize))
		r.pageType = branch
		r.left = pageNo(root)
		r.cells = r.cells[:1]
		r.cells[0] = *m
		if err := b.create(r); err != nil {
			return 0, xerrors.Errorf("failed to create new root: %w", err)
		}
		p = r
	}

	return int(p.pageNo), nil
}

func (b *BTree) insert(p *Page, c *cell) (*cell, error) {
	switch p.pageType {
	case leaf:
		if !p.willOverflow() {
			if err := p.Insert(c); err != nil {
				return nil, xerrors.Errorf("failed to insert: %w", err)
			}
			if err := b.update(p); err != nil {
				return nil, xerrors.Errorf("failed to update: %w", err)
			}
			return nil, nil
		}

		r, err := p.InsertSplit(c)
		if err != nil {
			return nil, xerrors.Errorf("failed to insert and split: %w", err)
		}
		r.next = p.next
		r.prev = p.pageNo
		if err := b.create(r); err != nil {
			return nil, xerrors.Errorf("failed to create right: %w", err)
		}
		p.next = r.pageNo
		if err := b.update(p); err != nil {
			return nil, xerrors.Errorf("failed to update: %w", err)
		}
		if r.next != 0 {
			n, err := b.get(r.next)
			if err != nil {
				return nil, xerrors.Errorf("failed to get next: %w", err)
			}
			n.prev = r.pageNo
			if err := b.update(n); err != nil {
				return nil, xerrors.Errorf("failed to update next: %w", err)
			}
		}
		return &cell{Payload: Payload{Key: r.cells[0].Key, Right: r.pageNo}}, nil
	case branch:
		n, err := b.get(p.child(c.Key))
		if err != nil {
			return nil, xerrors.Errorf("failed to get child: %w", err)
		}
		m, err := b.insert(n, c)
		if err != nil {
			return nil, xerrors.Errorf("failed to insert: %w", err)
		}
		if m == nil {
			return nil, nil
		}

		if !p.willOverflow() {
			if err := p.Insert(m); err != nil {
				return nil, xerrors.Errorf("failed to insert: %w", err)
			}
			if err := b.update(p); err != nil {
				return nil, xerrors.Errorf("failed to update: %w", err)
			}
			return nil, nil
		}
		r, k, err := p.InsertSplitMiddle(m)
		if err := b.create(r); err != nil {
			return nil, xerrors.Errorf("failed to create right: %w", err)
		}
		if err := b.update(p); err != nil {
			return nil, xerrors.Errorf("failed to update: %w", err)
		}
		return &cell{Payload: Payload{Key: k, Right: r.pageNo}}, nil
	default:
		return nil, xerrors.Errorf("invalid page type: %s", p.pageType)
	}
}
