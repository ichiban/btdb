package store

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	assert := assert.New(t)

	dir, err := ioutil.TempDir("", "test")
	assert.NoError(err)
	defer func() { assert.NoError(os.RemoveAll(dir)) }()

	_, err = Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
	assert.NoError(err)

	f, err := os.Open(filepath.Join(dir, "test.db"))
	assert.NoError(err)

	b, err := ioutil.ReadAll(f)
	assert.NoError(err)
	assert.Len(b, 128)
	assert.Equal([]byte{
		0x89, 0x31, 0x44, 0x42, // signature
		0x0d, 0x0a, 0x26, 0x0a, // signature (cont)
		0x00, 0x00, 0x00, 0x80, // page size
		0x00, 0x00, 0x00, 0x20, // cell size

		0x00, 0x00, 0x00, 0x00, // root page
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,

		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00,
	}, b)
}

func TestBTree_Iterator(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(dir)) }()

	b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
	assert.NoError(t, err)

	l1 := NewPage(128, 32)
	l1.pageType = leaf
	l1.next = 2
	l1.prev = 0
	assert.Equal(t, 3, cap(l1.cells))
	l1.cells = l1.cells[:2]
	l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
	l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
	assert.NoError(t, b.create(l1))
	assert.Equal(t, pageNo(1), l1.pageNo)

	l2 := NewPage(128, 32)
	l2.pageType = leaf
	l2.next = 3
	l2.prev = 1
	assert.Equal(t, 3, cap(l2.cells))
	l2.cells = l2.cells[:2]
	l2.cells[0] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
	l2.cells[1] = cell{Payload: Payload{Key: values{10}, Value: values{"10"}}}
	assert.NoError(t, b.create(l2))
	assert.Equal(t, pageNo(2), l2.pageNo)

	l3 := NewPage(128, 32)
	l3.pageType = leaf
	l3.next = 4
	l3.prev = 2
	assert.Equal(t, 3, cap(l3.cells))
	l3.cells = l3.cells[:2]
	l3.cells[0] = cell{Payload: Payload{Key: values{11}, Value: values{"11"}}}
	l3.cells[1] = cell{Payload: Payload{Key: values{12}, Value: values{"12"}}}
	assert.NoError(t, b.create(l3))
	assert.Equal(t, pageNo(3), l3.pageNo)

	l4 := NewPage(128, 32)
	l4.pageType = leaf
	l4.next = 5
	l4.prev = 3
	assert.Equal(t, 3, cap(l4.cells))
	l4.cells = l4.cells[:2]
	l4.cells[0] = cell{Payload: Payload{Key: values{13}, Value: values{"13"}}}
	l4.cells[1] = cell{Payload: Payload{Key: values{15}, Value: values{"15"}}}
	assert.NoError(t, b.create(l4))
	assert.Equal(t, pageNo(4), l4.pageNo)

	l5 := NewPage(128, 32)
	l5.pageType = leaf
	l5.next = 0
	l5.prev = 4
	assert.Equal(t, 3, cap(l5.cells))
	l5.cells = l5.cells[:3]
	l5.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
	l5.cells[1] = cell{Payload: Payload{Key: values{20}, Value: values{"20"}}}
	l5.cells[2] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
	assert.NoError(t, b.create(l5))
	assert.Equal(t, pageNo(5), l5.pageNo)

	i1 := NewPage(128, 32)
	i1.pageType = branch
	i1.left = l1.pageNo
	assert.Equal(t, 3, cap(i1.cells))
	i1.cells = i1.cells[:2]
	i1.cells[0] = cell{Payload: Payload{Key: values{9}, Right: l2.pageNo}}
	i1.cells[1] = cell{Payload: Payload{Key: values{11}, Right: l3.pageNo}}
	assert.NoError(t, b.create(i1))
	assert.Equal(t, pageNo(6), i1.pageNo)

	i2 := NewPage(128, 32)
	i2.pageType = branch
	i2.left = l4.pageNo
	assert.Equal(t, 3, cap(i2.cells))
	i2.cells = i2.cells[:1]
	i2.cells[0] = cell{Payload: Payload{Key: values{16}, Right: l5.pageNo}}
	assert.NoError(t, b.create(i2))
	assert.Equal(t, pageNo(7), i2.pageNo)

	r := NewPage(128, 32)
	r.pageType = branch
	r.left = i1.pageNo
	assert.Equal(t, 3, cap(r.cells))
	r.cells = r.cells[:1]
	r.cells[0] = cell{Payload: Payload{Key: values{13}, Right: i2.pageNo}}
	assert.NoError(t, b.create(r))
	assert.Equal(t, pageNo(8), r.pageNo)

	t.Run("iterate from 1", func(t *testing.T) {
		assert := assert.New(t)

		iter, err := b.Iterator(int(r.pageNo), values{1})
		assert.NoError(err)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(1)}, iter.Key)
		assert.Equal(values{"1"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(4)}, iter.Key)
		assert.Equal(values{"4"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(9)}, iter.Key)
		assert.Equal(values{"9"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(10)}, iter.Key)
		assert.Equal(values{"10"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(11)}, iter.Key)
		assert.Equal(values{"11"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(12)}, iter.Key)
		assert.Equal(values{"12"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(13)}, iter.Key)
		assert.Equal(values{"13"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(15)}, iter.Key)
		assert.Equal(values{"15"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(16)}, iter.Key)
		assert.Equal(values{"16"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(20)}, iter.Key)
		assert.Equal(values{"20"}, iter.Value)

		assert.NoError(iter.Next())
		assert.Equal(values{uint64(25)}, iter.Key)
		assert.Equal(values{"25"}, iter.Value)

		assert.Equal(ErrNotFound, iter.Next())
	})
}

func TestBTree_Search(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(dir)) }()

	b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
	assert.NoError(t, err)

	l1 := NewPage(128, 32)
	l1.pageType = leaf
	l1.next = 2
	l1.prev = 0
	assert.Equal(t, 3, cap(l1.cells))
	l1.cells = l1.cells[:2]
	l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
	l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
	assert.NoError(t, b.create(l1))
	assert.Equal(t, pageNo(1), l1.pageNo)

	l2 := NewPage(128, 32)
	l2.pageType = leaf
	l2.next = 3
	l2.prev = 1
	assert.Equal(t, 3, cap(l2.cells))
	l2.cells = l2.cells[:2]
	l2.cells[0] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
	l2.cells[1] = cell{Payload: Payload{Key: values{10}, Value: values{"10"}}}
	assert.NoError(t, b.create(l2))
	assert.Equal(t, pageNo(2), l2.pageNo)

	l3 := NewPage(128, 32)
	l3.pageType = leaf
	l3.next = 4
	l3.prev = 2
	assert.Equal(t, 3, cap(l3.cells))
	l3.cells = l3.cells[:2]
	l3.cells[0] = cell{Payload: Payload{Key: values{11}, Value: values{"11"}}}
	l3.cells[1] = cell{Payload: Payload{Key: values{12}, Value: values{"12"}}}
	assert.NoError(t, b.create(l3))
	assert.Equal(t, pageNo(3), l3.pageNo)

	l4 := NewPage(128, 32)
	l4.pageType = leaf
	l4.next = 5
	l4.prev = 3
	assert.Equal(t, 3, cap(l4.cells))
	l4.cells = l4.cells[:2]
	l4.cells[0] = cell{Payload: Payload{Key: values{13}, Value: values{"13"}}}
	l4.cells[1] = cell{Payload: Payload{Key: values{15}, Value: values{"15"}}}
	assert.NoError(t, b.create(l4))
	assert.Equal(t, pageNo(4), l4.pageNo)

	l5 := NewPage(128, 32)
	l5.pageType = leaf
	l5.next = 0
	l5.prev = 4
	assert.Equal(t, 3, cap(l5.cells))
	l5.cells = l5.cells[:3]
	l5.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
	l5.cells[1] = cell{Payload: Payload{Key: values{20}, Value: values{"20"}}}
	l5.cells[2] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
	assert.NoError(t, b.create(l5))
	assert.Equal(t, pageNo(5), l5.pageNo)

	i1 := NewPage(128, 32)
	i1.pageType = branch
	i1.left = l1.pageNo
	assert.Equal(t, 3, cap(i1.cells))
	i1.cells = i1.cells[:2]
	i1.cells[0] = cell{Payload: Payload{Key: values{9}, Right: l2.pageNo}}
	i1.cells[1] = cell{Payload: Payload{Key: values{11}, Right: l3.pageNo}}
	assert.NoError(t, b.create(i1))
	assert.Equal(t, pageNo(6), i1.pageNo)

	i2 := NewPage(128, 32)
	i2.pageType = branch
	i2.left = l4.pageNo
	assert.Equal(t, 3, cap(i2.cells))
	i2.cells = i2.cells[:1]
	i2.cells[0] = cell{Payload: Payload{Key: values{16}, Right: l5.pageNo}}
	assert.NoError(t, b.create(i2))
	assert.Equal(t, pageNo(7), i2.pageNo)

	r := NewPage(128, 32)
	r.pageType = branch
	r.left = i1.pageNo
	assert.Equal(t, 3, cap(r.cells))
	r.cells = r.cells[:1]
	r.cells[0] = cell{Payload: Payload{Key: values{13}, Right: i2.pageNo}}
	assert.NoError(t, b.create(r))
	assert.Equal(t, pageNo(8), r.pageNo)

	t.Run("select 1", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{1})
		assert.NoError(err)
		assert.Equal([]interface{}{"1"}, v)
	})

	t.Run("select 4", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{4})
		assert.NoError(err)
		assert.Equal([]interface{}{"4"}, v)
	})

	t.Run("select 9", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{9})
		assert.NoError(err)
		assert.Equal([]interface{}{"9"}, v)
	})

	t.Run("select 10", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{10})
		assert.NoError(err)
		assert.Equal([]interface{}{"10"}, v)
	})

	t.Run("select 11", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{11})
		assert.NoError(err)
		assert.Equal([]interface{}{"11"}, v)
	})

	t.Run("select 12", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{12})
		assert.NoError(err)
		assert.Equal([]interface{}{"12"}, v)
	})

	t.Run("select 13", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{13})
		assert.NoError(err)
		assert.Equal([]interface{}{"13"}, v)
	})

	t.Run("select 15", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{15})
		assert.NoError(err)
		assert.Equal([]interface{}{"15"}, v)
	})

	t.Run("select 16", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{16})
		assert.NoError(err)
		assert.Equal([]interface{}{"16"}, v)
	})

	t.Run("select 20", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{20})
		assert.NoError(err)
		assert.Equal([]interface{}{"20"}, v)
	})

	t.Run("select 25", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.pageNo), []interface{}{25})
		assert.NoError(err)
		assert.Equal([]interface{}{"25"}, v)
	})
}

func TestBTree_Insert(t *testing.T) {
	// example from http://www.cburch.com/cs/340/reading/btree/index.html

	t.Run("insert 20", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.pageType = leaf
		l1.next = 2
		l1.prev = 0
		assert.Equal(3, cap(l1.cells))
		l1.cells = l1.cells[:3]
		l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
		l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
		l1.cells[2] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
		assert.NoError(b.create(l1))
		assert.Equal(pageNo(1), l1.pageNo)

		l2 := NewPage(128, 32)
		l2.pageType = leaf
		l2.next = 0
		l2.prev = 1
		assert.Equal(3, cap(l2.cells))
		l2.cells = l2.cells[:2]
		l2.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
		l2.cells[1] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
		assert.NoError(b.create(l2))
		assert.Equal(pageNo(2), l2.pageNo)

		r := NewPage(128, 32)
		r.pageType = branch
		r.left = l1.pageNo
		assert.Equal(3, cap(r.cells))
		r.cells = r.cells[:1]
		r.cells[0] = cell{Payload: Payload{Key: values{16}, Right: l2.pageNo}}
		assert.NoError(b.create(r))
		assert.Equal(pageNo(3), r.pageNo)

		n, err := b.Insert(int(r.pageNo), values{20}, values{"20"})
		assert.NoError(err)
		assert.Equal(r.pageNo, pageNo(n))

		r, err = b.get(pageNo(3))
		assert.NoError(err)
		assert.Equal(pageNo(1), r.left)
		assert.Len(r.cells, 1)
		assert.Equal(values{uint64(16)}, r.cells[0].Key)
		assert.Equal(pageNo(2), r.cells[0].Right)

		l1, err = b.get(pageNo(1))
		assert.NoError(err)
		assert.Equal(leaf, l1.pageType)
		assert.Equal(pageNo(2), l1.next)
		assert.Equal(pageNo(0), l1.prev)
		assert.Len(l1.cells, 3)
		assert.Equal(values{uint64(1)}, l1.cells[0].Key)
		assert.Equal(values{"1"}, l1.cells[0].Value)
		assert.Equal(values{uint64(4)}, l1.cells[1].Key)
		assert.Equal(values{"4"}, l1.cells[1].Value)
		assert.Equal(values{uint64(9)}, l1.cells[2].Key)
		assert.Equal(values{"9"}, l1.cells[2].Value)

		l2, err = b.get(pageNo(2))
		assert.NoError(err)
		assert.Equal(leaf, l2.pageType)
		assert.Equal(pageNo(0), l2.next)
		assert.Equal(pageNo(1), l2.prev)
		assert.Len(l2.cells, 3)
		assert.Equal(values{uint64(16)}, l2.cells[0].Key)
		assert.Equal(values{"16"}, l2.cells[0].Value)
		assert.Equal(values{uint64(20)}, l2.cells[1].Key)
		assert.Equal(values{"20"}, l2.cells[1].Value)
		assert.Equal(values{uint64(25)}, l2.cells[2].Key)
		assert.Equal(values{"25"}, l2.cells[2].Value)
	})

	t.Run("insert 13", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.pageType = leaf
		l1.next = 2
		l1.prev = 0
		assert.Equal(3, cap(l1.cells))
		l1.cells = l1.cells[:3]
		l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
		l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
		l1.cells[2] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
		assert.NoError(b.create(l1))
		assert.Equal(pageNo(1), l1.pageNo)

		l2 := NewPage(128, 32)
		l2.pageType = leaf
		l2.next = 0
		l2.prev = 1
		assert.Equal(3, cap(l2.cells))
		l2.cells = l2.cells[:3]
		l2.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
		l2.cells[1] = cell{Payload: Payload{Key: values{20}, Value: values{"20"}}}
		l2.cells[2] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
		assert.NoError(b.create(l2))
		assert.Equal(pageNo(2), l2.pageNo)

		r := NewPage(128, 32)
		r.pageType = branch
		r.left = l1.pageNo
		assert.Equal(3, cap(r.cells))
		r.cells = r.cells[:1]
		r.cells[0] = cell{Payload: Payload{Key: values{16}, Right: l2.pageNo}}
		assert.NoError(b.create(r))
		assert.Equal(pageNo(3), r.pageNo)

		n, err := b.Insert(int(r.pageNo), values{13}, values{"13"})
		assert.NoError(err)
		assert.Equal(r.pageNo, pageNo(n))

		r, err = b.get(pageNo(3))
		assert.NoError(err)
		assert.Equal(pageNo(1), r.left)
		assert.Len(r.cells, 2)
		assert.Equal(values{uint64(9)}, r.cells[0].Key)
		assert.Equal(pageNo(4), r.cells[0].Right)
		assert.Equal(values{uint64(16)}, r.cells[1].Key)
		assert.Equal(pageNo(2), r.cells[1].Right)

		l1, err = b.get(pageNo(1))
		assert.NoError(err)
		assert.Equal(leaf, l1.pageType)
		assert.Equal(pageNo(4), l1.next)
		assert.Equal(pageNo(0), l1.prev)
		assert.Len(l1.cells, 2)
		assert.Equal(values{uint64(1)}, l1.cells[0].Key)
		assert.Equal(values{"1"}, l1.cells[0].Value)
		assert.Equal(values{uint64(4)}, l1.cells[1].Key)
		assert.Equal(values{"4"}, l1.cells[1].Value)

		l2, err = b.get(pageNo(4))
		assert.NoError(err)
		assert.Equal(leaf, l2.pageType)
		assert.Equal(pageNo(2), l2.next)
		assert.Equal(pageNo(1), l2.prev)
		assert.Len(l2.cells, 2)
		assert.Equal(values{uint64(9)}, l2.cells[0].Key)
		assert.Equal(values{"9"}, l2.cells[0].Value)
		assert.Equal(values{uint64(13)}, l2.cells[1].Key)
		assert.Equal(values{"13"}, l2.cells[1].Value)

		l3, err := b.get(pageNo(2))
		assert.NoError(err)
		assert.Equal(leaf, l3.pageType)
		assert.Equal(pageNo(0), l3.next)
		assert.Equal(pageNo(4), l3.prev)
		assert.Len(l3.cells, 3)
		assert.Equal(values{uint64(16)}, l3.cells[0].Key)
		assert.Equal(values{"16"}, l3.cells[0].Value)
		assert.Equal(values{uint64(20)}, l3.cells[1].Key)
		assert.Equal(values{"20"}, l3.cells[1].Value)
		assert.Equal(values{uint64(25)}, l3.cells[2].Key)
		assert.Equal(values{"25"}, l3.cells[2].Value)
	})

	t.Run("insert 15", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.pageType = leaf
		l1.next = 2
		l1.prev = 0
		assert.Equal(3, cap(l1.cells))
		l1.cells = l1.cells[:2]
		l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
		l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(pageNo(1), l1.pageNo)

		l2 := NewPage(128, 32)
		l2.pageType = leaf
		l2.next = 3
		l2.prev = 1
		assert.Equal(3, cap(l2.cells))
		l2.cells = l2.cells[:2]
		l2.cells[0] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
		l2.cells[1] = cell{Payload: Payload{Key: values{13}, Value: values{"13"}}}
		assert.NoError(b.create(l2))
		assert.Equal(pageNo(2), l2.pageNo)

		l3 := NewPage(128, 32)
		l3.pageType = leaf
		l3.next = 0
		l3.prev = 2
		assert.Equal(3, cap(l3.cells))
		l3.cells = l3.cells[:3]
		l3.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
		l3.cells[1] = cell{Payload: Payload{Key: values{20}, Value: values{"20"}}}
		l3.cells[2] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
		assert.NoError(b.create(l3))
		assert.Equal(pageNo(3), l3.pageNo)

		r := NewPage(128, 32)
		r.pageType = branch
		r.left = l1.pageNo
		assert.Equal(3, cap(r.cells))
		r.cells = r.cells[:2]
		r.cells[0] = cell{Payload: Payload{Key: values{9}, Right: l2.pageNo}}
		r.cells[1] = cell{Payload: Payload{Key: values{16}, Right: l3.pageNo}}
		assert.NoError(b.create(r))
		assert.Equal(pageNo(4), r.pageNo)

		n, err := b.Insert(int(r.pageNo), values{15}, values{"15"})
		assert.NoError(err)
		assert.Equal(r.pageNo, pageNo(n))

		r, err = b.get(pageNo(4))
		assert.NoError(err)
		assert.Equal(pageNo(1), r.left)
		assert.Len(r.cells, 2)
		assert.Equal(values{uint64(9)}, r.cells[0].Key)
		assert.Equal(pageNo(2), r.cells[0].Right)
		assert.Equal(values{uint64(16)}, r.cells[1].Key)
		assert.Equal(pageNo(3), r.cells[1].Right)

		l1, err = b.get(pageNo(1))
		assert.NoError(err)
		assert.Equal(leaf, l1.pageType)
		assert.Equal(pageNo(2), l1.next)
		assert.Equal(pageNo(0), l1.prev)
		assert.Len(l1.cells, 2)
		assert.Equal(values{uint64(1)}, l1.cells[0].Key)
		assert.Equal(values{"1"}, l1.cells[0].Value)
		assert.Equal(values{uint64(4)}, l1.cells[1].Key)
		assert.Equal(values{"4"}, l1.cells[1].Value)

		l2, err = b.get(pageNo(2))
		assert.NoError(err)
		assert.Equal(leaf, l2.pageType)
		assert.Equal(pageNo(3), l2.next)
		assert.Equal(pageNo(1), l2.prev)
		assert.Len(l2.cells, 3)
		assert.Equal(values{uint64(9)}, l2.cells[0].Key)
		assert.Equal(values{"9"}, l2.cells[0].Value)
		assert.Equal(values{uint64(13)}, l2.cells[1].Key)
		assert.Equal(values{"13"}, l2.cells[1].Value)
		assert.Equal(values{uint64(15)}, l2.cells[2].Key)
		assert.Equal(values{"15"}, l2.cells[2].Value)

		l3, err = b.get(pageNo(3))
		assert.NoError(err)
		assert.Equal(leaf, l3.pageType)
		assert.Equal(pageNo(0), l3.next)
		assert.Equal(pageNo(2), l3.prev)
		assert.Len(l3.cells, 3)
		assert.Equal(values{uint64(16)}, l3.cells[0].Key)
		assert.Equal(values{"16"}, l3.cells[0].Value)
		assert.Equal(values{uint64(20)}, l3.cells[1].Key)
		assert.Equal(values{"20"}, l3.cells[1].Value)
		assert.Equal(values{uint64(25)}, l3.cells[2].Key)
		assert.Equal(values{"25"}, l3.cells[2].Value)
	})

	t.Run("insert 10", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.pageType = leaf
		l1.next = 2
		l1.prev = 0
		assert.Equal(3, cap(l1.cells))
		l1.cells = l1.cells[:2]
		l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
		l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(pageNo(1), l1.pageNo)

		l2 := NewPage(128, 32)
		l2.pageType = leaf
		l2.next = 3
		l2.prev = 1
		assert.Equal(3, cap(l2.cells))
		l2.cells = l2.cells[:3]
		l2.cells[0] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
		l2.cells[1] = cell{Payload: Payload{Key: values{13}, Value: values{"13"}}}
		l2.cells[2] = cell{Payload: Payload{Key: values{15}, Value: values{"15"}}}
		assert.NoError(b.create(l2))
		assert.Equal(pageNo(2), l2.pageNo)

		l3 := NewPage(128, 32)
		l3.pageType = leaf
		l3.next = 0
		l3.prev = 2
		assert.Equal(3, cap(l3.cells))
		l3.cells = l3.cells[:3]
		l3.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
		l3.cells[1] = cell{Payload: Payload{Key: values{20}, Value: values{"20"}}}
		l3.cells[2] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
		assert.NoError(b.create(l3))
		assert.Equal(pageNo(3), l3.pageNo)

		r := NewPage(128, 32)
		r.pageType = branch
		r.left = l1.pageNo
		assert.Equal(3, cap(r.cells))
		r.cells = r.cells[:2]
		r.cells[0] = cell{Payload: Payload{Key: values{9}, Right: l2.pageNo}}
		r.cells[1] = cell{Payload: Payload{Key: values{16}, Right: l3.pageNo}}
		assert.NoError(b.create(r))
		assert.Equal(pageNo(4), r.pageNo)

		n, err := b.Insert(int(r.pageNo), values{10}, values{"10"})
		assert.NoError(err)
		assert.Equal(r.pageNo, pageNo(n))

		r, err = b.get(pageNo(4))
		assert.NoError(err)
		assert.Equal(pageNo(1), r.left)
		assert.Len(r.cells, 3)
		assert.Equal(values{uint64(9)}, r.cells[0].Key)
		assert.Equal(pageNo(2), r.cells[0].Right)
		assert.Equal(values{uint64(13)}, r.cells[1].Key)
		assert.Equal(pageNo(5), r.cells[1].Right)
		assert.Equal(values{uint64(16)}, r.cells[2].Key)
		assert.Equal(pageNo(3), r.cells[2].Right)

		l1, err = b.get(pageNo(1))
		assert.NoError(err)
		assert.Equal(leaf, l1.pageType)
		assert.Equal(pageNo(2), l1.next)
		assert.Equal(pageNo(0), l1.prev)
		assert.Len(l1.cells, 2)
		assert.Equal(values{uint64(1)}, l1.cells[0].Key)
		assert.Equal(values{"1"}, l1.cells[0].Value)
		assert.Equal(values{uint64(4)}, l1.cells[1].Key)
		assert.Equal(values{"4"}, l1.cells[1].Value)

		l2, err = b.get(pageNo(2))
		assert.NoError(err)
		assert.Equal(leaf, l2.pageType)
		assert.Equal(pageNo(5), l2.next)
		assert.Equal(pageNo(1), l2.prev)
		assert.Len(l2.cells, 2)
		assert.Equal(values{uint64(9)}, l2.cells[0].Key)
		assert.Equal(values{"9"}, l2.cells[0].Value)
		assert.Equal(values{uint64(10)}, l2.cells[1].Key)
		assert.Equal(values{"10"}, l2.cells[1].Value)

		l3, err = b.get(pageNo(5))
		assert.NoError(err)
		assert.Equal(leaf, l3.pageType)
		assert.Equal(pageNo(3), l3.next)
		assert.Equal(pageNo(2), l3.prev)
		assert.Len(l3.cells, 2)
		assert.Equal(values{uint64(13)}, l3.cells[0].Key)
		assert.Equal(values{"13"}, l3.cells[0].Value)
		assert.Equal(values{uint64(15)}, l3.cells[1].Key)
		assert.Equal(values{"15"}, l3.cells[1].Value)

		l4, err := b.get(pageNo(3))
		assert.NoError(err)
		assert.Equal(leaf, l4.pageType)
		assert.Equal(pageNo(0), l4.next)
		assert.Equal(pageNo(5), l4.prev)
		assert.Len(l4.cells, 3)
		assert.Equal(values{uint64(16)}, l4.cells[0].Key)
		assert.Equal(values{"16"}, l4.cells[0].Value)
		assert.Equal(values{uint64(20)}, l4.cells[1].Key)
		assert.Equal(values{"20"}, l4.cells[1].Value)
		assert.Equal(values{uint64(25)}, l4.cells[2].Key)
		assert.Equal(values{"25"}, l4.cells[2].Value)
	})

	t.Run("insert 11", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.pageType = leaf
		l1.next = 2
		l1.prev = 0
		assert.Equal(3, cap(l1.cells))
		l1.cells = l1.cells[:2]
		l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
		l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(pageNo(1), l1.pageNo)

		l2 := NewPage(128, 32)
		l2.pageType = leaf
		l2.next = 3
		l2.prev = 1
		assert.Equal(3, cap(l2.cells))
		l2.cells = l2.cells[:2]
		l2.cells[0] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
		l2.cells[1] = cell{Payload: Payload{Key: values{10}, Value: values{"10"}}}
		assert.NoError(b.create(l2))
		assert.Equal(pageNo(2), l2.pageNo)

		l3 := NewPage(128, 32)
		l3.pageType = leaf
		l3.next = 4
		l3.prev = 2
		assert.Equal(3, cap(l3.cells))
		l3.cells = l3.cells[:2]
		l3.cells[0] = cell{Payload: Payload{Key: values{13}, Value: values{"13"}}}
		l3.cells[1] = cell{Payload: Payload{Key: values{15}, Value: values{"15"}}}
		assert.NoError(b.create(l3))
		assert.Equal(pageNo(3), l3.pageNo)

		l4 := NewPage(128, 32)
		l4.pageType = leaf
		l4.next = 0
		l4.prev = 3
		assert.Equal(3, cap(l4.cells))
		l4.cells = l4.cells[:3]
		l4.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
		l4.cells[1] = cell{Payload: Payload{Key: values{20}, Value: values{"20"}}}
		l4.cells[2] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
		assert.NoError(b.create(l4))
		assert.Equal(pageNo(4), l4.pageNo)

		r := NewPage(128, 32)
		r.pageType = branch
		r.left = l1.pageNo
		assert.Equal(3, cap(r.cells))
		r.cells = r.cells[:3]
		r.cells[0] = cell{Payload: Payload{Key: values{9}, Right: l2.pageNo}}
		r.cells[1] = cell{Payload: Payload{Key: values{13}, Right: l3.pageNo}}
		r.cells[2] = cell{Payload: Payload{Key: values{16}, Right: l4.pageNo}}
		assert.NoError(b.create(r))
		assert.Equal(pageNo(5), r.pageNo)

		n, err := b.Insert(int(r.pageNo), values{11}, values{"11"})
		assert.NoError(err)
		assert.Equal(r.pageNo, pageNo(n))

		r, err = b.get(pageNo(5))
		assert.NoError(err)
		assert.Equal(pageNo(1), r.left)
		assert.Len(r.cells, 3)
		assert.Equal(values{uint64(9)}, r.cells[0].Key)
		assert.Equal(pageNo(2), r.cells[0].Right)
		assert.Equal(values{uint64(13)}, r.cells[1].Key)
		assert.Equal(pageNo(3), r.cells[1].Right)
		assert.Equal(values{uint64(16)}, r.cells[2].Key)
		assert.Equal(pageNo(4), r.cells[2].Right)

		l1, err = b.get(pageNo(1))
		assert.NoError(err)
		assert.Equal(leaf, l1.pageType)
		assert.Equal(pageNo(2), l1.next)
		assert.Equal(pageNo(0), l1.prev)
		assert.Len(l1.cells, 2)
		assert.Equal(values{uint64(1)}, l1.cells[0].Key)
		assert.Equal(values{"1"}, l1.cells[0].Value)
		assert.Equal(values{uint64(4)}, l1.cells[1].Key)
		assert.Equal(values{"4"}, l1.cells[1].Value)

		l2, err = b.get(pageNo(2))
		assert.NoError(err)
		assert.Equal(leaf, l2.pageType)
		assert.Equal(pageNo(3), l2.next)
		assert.Equal(pageNo(1), l2.prev)
		assert.Len(l2.cells, 3)
		assert.Equal(values{uint64(9)}, l2.cells[0].Key)
		assert.Equal(values{"9"}, l2.cells[0].Value)
		assert.Equal(values{uint64(10)}, l2.cells[1].Key)
		assert.Equal(values{"10"}, l2.cells[1].Value)
		assert.Equal(values{uint64(11)}, l2.cells[2].Key)
		assert.Equal(values{"11"}, l2.cells[2].Value)

		l3, err = b.get(pageNo(3))
		assert.NoError(err)
		assert.Equal(leaf, l3.pageType)
		assert.Equal(pageNo(4), l3.next)
		assert.Equal(pageNo(2), l3.prev)
		assert.Len(l3.cells, 2)
		assert.Equal(values{uint64(13)}, l3.cells[0].Key)
		assert.Equal(values{"13"}, l3.cells[0].Value)
		assert.Equal(values{uint64(15)}, l3.cells[1].Key)
		assert.Equal(values{"15"}, l3.cells[1].Value)

		l4, err = b.get(pageNo(4))
		assert.NoError(err)
		assert.Equal(leaf, l4.pageType)
		assert.Equal(pageNo(0), l4.next)
		assert.Equal(pageNo(3), l4.prev)
		assert.Len(l4.cells, 3)
		assert.Equal(values{uint64(16)}, l4.cells[0].Key)
		assert.Equal(values{"16"}, l4.cells[0].Value)
		assert.Equal(values{uint64(20)}, l4.cells[1].Key)
		assert.Equal(values{"20"}, l4.cells[1].Value)
		assert.Equal(values{uint64(25)}, l4.cells[2].Key)
		assert.Equal(values{"25"}, l4.cells[2].Value)
	})

	t.Run("insert 12", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.pageType = leaf
		l1.next = 2
		l1.prev = 0
		assert.Equal(3, cap(l1.cells))
		l1.cells = l1.cells[:2]
		l1.cells[0] = cell{Payload: Payload{Key: values{1}, Value: values{"1"}}}
		l1.cells[1] = cell{Payload: Payload{Key: values{4}, Value: values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(pageNo(1), l1.pageNo)

		l2 := NewPage(128, 32)
		l2.pageType = leaf
		l2.next = 3
		l2.prev = 1
		assert.Equal(3, cap(l2.cells))
		l2.cells = l2.cells[:3]
		l2.cells[0] = cell{Payload: Payload{Key: values{9}, Value: values{"9"}}}
		l2.cells[1] = cell{Payload: Payload{Key: values{10}, Value: values{"10"}}}
		l2.cells[2] = cell{Payload: Payload{Key: values{11}, Value: values{"11"}}}
		assert.NoError(b.create(l2))
		assert.Equal(pageNo(2), l2.pageNo)

		l3 := NewPage(128, 32)
		l3.pageType = leaf
		l3.next = 4
		l3.prev = 2
		assert.Equal(3, cap(l3.cells))
		l3.cells = l3.cells[:2]
		l3.cells[0] = cell{Payload: Payload{Key: values{13}, Value: values{"13"}}}
		l3.cells[1] = cell{Payload: Payload{Key: values{15}, Value: values{"15"}}}
		assert.NoError(b.create(l3))
		assert.Equal(pageNo(3), l3.pageNo)

		l4 := NewPage(128, 32)
		l4.pageType = leaf
		l4.next = 0
		l4.prev = 3
		assert.Equal(3, cap(l4.cells))
		l4.cells = l4.cells[:3]
		l4.cells[0] = cell{Payload: Payload{Key: values{16}, Value: values{"16"}}}
		l4.cells[1] = cell{Payload: Payload{Key: values{20}, Value: values{"20"}}}
		l4.cells[2] = cell{Payload: Payload{Key: values{25}, Value: values{"25"}}}
		assert.NoError(b.create(l4))
		assert.Equal(pageNo(4), l4.pageNo)

		r := NewPage(128, 32)
		r.pageType = branch
		r.left = l1.pageNo
		assert.Equal(3, cap(r.cells))
		r.cells = r.cells[:3]
		r.cells[0] = cell{Payload: Payload{Key: values{9}, Right: l2.pageNo}}
		r.cells[1] = cell{Payload: Payload{Key: values{13}, Right: l3.pageNo}}
		r.cells[2] = cell{Payload: Payload{Key: values{16}, Right: l4.pageNo}}
		assert.NoError(b.create(r))
		assert.Equal(pageNo(5), r.pageNo)

		n, err := b.Insert(int(r.pageNo), values{12}, values{"12"})
		assert.NoError(err)
		assert.Equal(pageNo(8), pageNo(n)) // new root

		r, err = b.get(pageNo(8))
		assert.NoError(err)
		assert.Equal(pageNo(5), r.left)
		assert.Len(r.cells, 1)
		assert.Equal(values{uint64(13)}, r.cells[0].Key)
		assert.Equal(pageNo(7), r.cells[0].Right)

		i1, err := b.get(pageNo(5))
		assert.NoError(err)
		assert.Equal(branch, i1.pageType)
		assert.Equal(pageNo(1), i1.left)
		assert.Len(i1.cells, 2)
		assert.Equal(values{uint64(9)}, i1.cells[0].Key)
		assert.Equal(pageNo(2), i1.cells[0].Right)
		assert.Equal(values{uint64(11)}, i1.cells[1].Key)
		assert.Equal(pageNo(6), i1.cells[1].Right)

		i2, err := b.get(pageNo(7))
		assert.NoError(err)
		assert.Equal(branch, i2.pageType)
		assert.Equal(pageNo(3), i2.left)
		assert.Len(i2.cells, 1)
		assert.Equal(values{uint64(16)}, i2.cells[0].Key)
		assert.Equal(pageNo(4), i2.cells[0].Right)

		l1, err = b.get(pageNo(1))
		assert.NoError(err)
		assert.Equal(leaf, l1.pageType)
		assert.Equal(pageNo(2), l1.next)
		assert.Equal(pageNo(0), l1.prev)
		assert.Len(l1.cells, 2)
		assert.Equal(values{uint64(1)}, l1.cells[0].Key)
		assert.Equal(values{"1"}, l1.cells[0].Value)
		assert.Equal(values{uint64(4)}, l1.cells[1].Key)
		assert.Equal(values{"4"}, l1.cells[1].Value)

		l2, err = b.get(pageNo(2))
		assert.NoError(err)
		assert.Equal(leaf, l2.pageType)
		assert.Equal(pageNo(6), l2.next)
		assert.Equal(pageNo(1), l2.prev)
		assert.Len(l2.cells, 2)
		assert.Equal(values{uint64(9)}, l2.cells[0].Key)
		assert.Equal(values{"9"}, l2.cells[0].Value)
		assert.Equal(values{uint64(10)}, l2.cells[1].Key)
		assert.Equal(values{"10"}, l2.cells[1].Value)

		l3, err = b.get(pageNo(6))
		assert.NoError(err)
		assert.Equal(leaf, l3.pageType)
		assert.Equal(pageNo(3), l3.next)
		assert.Equal(pageNo(2), l3.prev)
		assert.Len(l3.cells, 2)
		assert.Equal(values{uint64(11)}, l3.cells[0].Key)
		assert.Equal(values{"11"}, l3.cells[0].Value)
		assert.Equal(values{uint64(12)}, l3.cells[1].Key)
		assert.Equal(values{"12"}, l3.cells[1].Value)

		l4, err = b.get(pageNo(3))
		assert.NoError(err)
		assert.Equal(leaf, l4.pageType)
		assert.Equal(pageNo(4), l4.next)
		assert.Equal(pageNo(6), l4.prev)
		assert.Len(l4.cells, 2)
		assert.Equal(values{uint64(13)}, l4.cells[0].Key)
		assert.Equal(values{"13"}, l4.cells[0].Value)
		assert.Equal(values{uint64(15)}, l4.cells[1].Key)
		assert.Equal(values{"15"}, l4.cells[1].Value)

		l5, err := b.get(pageNo(4))
		assert.NoError(err)
		assert.Equal(leaf, l5.pageType)
		assert.Equal(pageNo(0), l5.next)
		assert.Equal(pageNo(3), l5.prev)
		assert.Len(l5.cells, 3)
		assert.Equal(values{uint64(16)}, l5.cells[0].Key)
		assert.Equal(values{"16"}, l5.cells[0].Value)
		assert.Equal(values{uint64(20)}, l5.cells[1].Key)
		assert.Equal(values{"20"}, l5.cells[1].Value)
		assert.Equal(values{uint64(25)}, l5.cells[2].Key)
		assert.Equal(values{"25"}, l5.cells[2].Value)
	})
}
