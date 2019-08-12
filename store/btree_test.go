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
	l1.Type = Leaf
	l1.Next = 2
	l1.Prev = 0
	assert.Equal(t, 3, cap(l1.Cells))
	l1.Cells = l1.Cells[:2]
	l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
	l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
	assert.NoError(t, b.create(l1))
	assert.Equal(t, PageNo(1), l1.PageNo)

	l2 := NewPage(128, 32)
	l2.Type = Leaf
	l2.Next = 3
	l2.Prev = 1
	assert.Equal(t, 3, cap(l2.Cells))
	l2.Cells = l2.Cells[:2]
	l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
	l2.Cells[1] = Cell{Payload: Payload{Key: Values{10}, Value: Values{"10"}}}
	assert.NoError(t, b.create(l2))
	assert.Equal(t, PageNo(2), l2.PageNo)

	l3 := NewPage(128, 32)
	l3.Type = Leaf
	l3.Next = 4
	l3.Prev = 2
	assert.Equal(t, 3, cap(l3.Cells))
	l3.Cells = l3.Cells[:2]
	l3.Cells[0] = Cell{Payload: Payload{Key: Values{11}, Value: Values{"11"}}}
	l3.Cells[1] = Cell{Payload: Payload{Key: Values{12}, Value: Values{"12"}}}
	assert.NoError(t, b.create(l3))
	assert.Equal(t, PageNo(3), l3.PageNo)

	l4 := NewPage(128, 32)
	l4.Type = Leaf
	l4.Next = 5
	l4.Prev = 3
	assert.Equal(t, 3, cap(l4.Cells))
	l4.Cells = l4.Cells[:2]
	l4.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
	l4.Cells[1] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
	assert.NoError(t, b.create(l4))
	assert.Equal(t, PageNo(4), l4.PageNo)

	l5 := NewPage(128, 32)
	l5.Type = Leaf
	l5.Next = 0
	l5.Prev = 4
	assert.Equal(t, 3, cap(l5.Cells))
	l5.Cells = l5.Cells[:3]
	l5.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
	l5.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
	l5.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
	assert.NoError(t, b.create(l5))
	assert.Equal(t, PageNo(5), l5.PageNo)

	i1 := NewPage(128, 32)
	i1.Type = Branch
	i1.Left = l1.PageNo
	assert.Equal(t, 3, cap(i1.Cells))
	i1.Cells = i1.Cells[:2]
	i1.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
	i1.Cells[1] = Cell{Payload: Payload{Key: Values{11}, Right: l3.PageNo}}
	assert.NoError(t, b.create(i1))
	assert.Equal(t, PageNo(6), i1.PageNo)

	i2 := NewPage(128, 32)
	i2.Type = Branch
	i2.Left = l4.PageNo
	assert.Equal(t, 3, cap(i2.Cells))
	i2.Cells = i2.Cells[:1]
	i2.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Right: l5.PageNo}}
	assert.NoError(t, b.create(i2))
	assert.Equal(t, PageNo(7), i2.PageNo)

	r := NewPage(128, 32)
	r.Type = Branch
	r.Left = i1.PageNo
	assert.Equal(t, 3, cap(r.Cells))
	r.Cells = r.Cells[:1]
	r.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Right: i2.PageNo}}
	assert.NoError(t, b.create(r))
	assert.Equal(t, PageNo(8), r.PageNo)

	t.Run("iterate from 1", func(t *testing.T) {
		assert := assert.New(t)

		iter := b.Iterator(r.PageNo, Values{1})
		assert.NoError(iter.Err())

		assert.True(iter.Next())
		assert.Equal(Values{uint64(1)}, iter.Key)
		assert.Equal(Values{"1"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(4)}, iter.Key)
		assert.Equal(Values{"4"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(9)}, iter.Key)
		assert.Equal(Values{"9"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(10)}, iter.Key)
		assert.Equal(Values{"10"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(11)}, iter.Key)
		assert.Equal(Values{"11"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(12)}, iter.Key)
		assert.Equal(Values{"12"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(13)}, iter.Key)
		assert.Equal(Values{"13"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(15)}, iter.Key)
		assert.Equal(Values{"15"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(16)}, iter.Key)
		assert.Equal(Values{"16"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(20)}, iter.Key)
		assert.Equal(Values{"20"}, iter.Value)

		assert.True(iter.Next())
		assert.Equal(Values{uint64(25)}, iter.Key)
		assert.Equal(Values{"25"}, iter.Value)

		assert.False(iter.Next())
		assert.NoError(iter.Err())
	})
}

func TestBTree_Search(t *testing.T) {
	dir, err := ioutil.TempDir("", "test")
	assert.NoError(t, err)
	defer func() { assert.NoError(t, os.RemoveAll(dir)) }()

	b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
	assert.NoError(t, err)

	l1 := NewPage(128, 32)
	l1.Type = Leaf
	l1.Next = 2
	l1.Prev = 0
	assert.Equal(t, 3, cap(l1.Cells))
	l1.Cells = l1.Cells[:2]
	l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
	l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
	assert.NoError(t, b.create(l1))
	assert.Equal(t, PageNo(1), l1.PageNo)

	l2 := NewPage(128, 32)
	l2.Type = Leaf
	l2.Next = 3
	l2.Prev = 1
	assert.Equal(t, 3, cap(l2.Cells))
	l2.Cells = l2.Cells[:2]
	l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
	l2.Cells[1] = Cell{Payload: Payload{Key: Values{10}, Value: Values{"10"}}}
	assert.NoError(t, b.create(l2))
	assert.Equal(t, PageNo(2), l2.PageNo)

	l3 := NewPage(128, 32)
	l3.Type = Leaf
	l3.Next = 4
	l3.Prev = 2
	assert.Equal(t, 3, cap(l3.Cells))
	l3.Cells = l3.Cells[:2]
	l3.Cells[0] = Cell{Payload: Payload{Key: Values{11}, Value: Values{"11"}}}
	l3.Cells[1] = Cell{Payload: Payload{Key: Values{12}, Value: Values{"12"}}}
	assert.NoError(t, b.create(l3))
	assert.Equal(t, PageNo(3), l3.PageNo)

	l4 := NewPage(128, 32)
	l4.Type = Leaf
	l4.Next = 5
	l4.Prev = 3
	assert.Equal(t, 3, cap(l4.Cells))
	l4.Cells = l4.Cells[:2]
	l4.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
	l4.Cells[1] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
	assert.NoError(t, b.create(l4))
	assert.Equal(t, PageNo(4), l4.PageNo)

	l5 := NewPage(128, 32)
	l5.Type = Leaf
	l5.Next = 0
	l5.Prev = 4
	assert.Equal(t, 3, cap(l5.Cells))
	l5.Cells = l5.Cells[:3]
	l5.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
	l5.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
	l5.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
	assert.NoError(t, b.create(l5))
	assert.Equal(t, PageNo(5), l5.PageNo)

	i1 := NewPage(128, 32)
	i1.Type = Branch
	i1.Left = l1.PageNo
	assert.Equal(t, 3, cap(i1.Cells))
	i1.Cells = i1.Cells[:2]
	i1.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
	i1.Cells[1] = Cell{Payload: Payload{Key: Values{11}, Right: l3.PageNo}}
	assert.NoError(t, b.create(i1))
	assert.Equal(t, PageNo(6), i1.PageNo)

	i2 := NewPage(128, 32)
	i2.Type = Branch
	i2.Left = l4.PageNo
	assert.Equal(t, 3, cap(i2.Cells))
	i2.Cells = i2.Cells[:1]
	i2.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Right: l5.PageNo}}
	assert.NoError(t, b.create(i2))
	assert.Equal(t, PageNo(7), i2.PageNo)

	r := NewPage(128, 32)
	r.Type = Branch
	r.Left = i1.PageNo
	assert.Equal(t, 3, cap(r.Cells))
	r.Cells = r.Cells[:1]
	r.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Right: i2.PageNo}}
	assert.NoError(t, b.create(r))
	assert.Equal(t, PageNo(8), r.PageNo)

	t.Run("select 1", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{1})
		assert.NoError(err)
		assert.Equal([]interface{}{"1"}, v)
	})

	t.Run("select 4", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{4})
		assert.NoError(err)
		assert.Equal([]interface{}{"4"}, v)
	})

	t.Run("select 9", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{9})
		assert.NoError(err)
		assert.Equal([]interface{}{"9"}, v)
	})

	t.Run("select 10", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{10})
		assert.NoError(err)
		assert.Equal([]interface{}{"10"}, v)
	})

	t.Run("select 11", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{11})
		assert.NoError(err)
		assert.Equal([]interface{}{"11"}, v)
	})

	t.Run("select 12", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{12})
		assert.NoError(err)
		assert.Equal([]interface{}{"12"}, v)
	})

	t.Run("select 13", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{13})
		assert.NoError(err)
		assert.Equal([]interface{}{"13"}, v)
	})

	t.Run("select 15", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{15})
		assert.NoError(err)
		assert.Equal([]interface{}{"15"}, v)
	})

	t.Run("select 16", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{16})
		assert.NoError(err)
		assert.Equal([]interface{}{"16"}, v)
	})

	t.Run("select 20", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{20})
		assert.NoError(err)
		assert.Equal([]interface{}{"20"}, v)
	})

	t.Run("select 25", func(t *testing.T) {
		assert := assert.New(t)

		v, err := b.Search(int(r.PageNo), []interface{}{25})
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
		l1.Type = Leaf
		l1.Next = 2
		l1.Prev = 0
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:3]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		l1.Cells[2] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		assert.NoError(b.create(l1))
		assert.Equal(PageNo(1), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		l2.Next = 0
		l2.Prev = 1
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:2]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.create(l2))
		assert.Equal(PageNo(2), l2.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:1]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Right: l2.PageNo}}
		assert.NoError(b.create(r))
		assert.Equal(PageNo(3), r.PageNo)

		n, err := b.Insert(int(r.PageNo), Values{20}, Values{"20"})
		assert.NoError(err)
		assert.Equal(r.PageNo, PageNo(n))

		r, err = b.get(PageNo(3))
		assert.NoError(err)
		assert.Equal(PageNo(1), r.Left)
		assert.Len(r.Cells, 1)
		assert.Equal(Values{uint64(16)}, r.Cells[0].Key)
		assert.Equal(PageNo(2), r.Cells[0].Right)

		l1, err = b.get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Equal(PageNo(2), l1.Next)
		assert.Equal(PageNo(0), l1.Prev)
		assert.Len(l1.Cells, 3)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)
		assert.Equal(Values{uint64(9)}, l1.Cells[2].Key)
		assert.Equal(Values{"9"}, l1.Cells[2].Value)

		l2, err = b.get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Equal(PageNo(0), l2.Next)
		assert.Equal(PageNo(1), l2.Prev)
		assert.Len(l2.Cells, 3)
		assert.Equal(Values{uint64(16)}, l2.Cells[0].Key)
		assert.Equal(Values{"16"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(20)}, l2.Cells[1].Key)
		assert.Equal(Values{"20"}, l2.Cells[1].Value)
		assert.Equal(Values{uint64(25)}, l2.Cells[2].Key)
		assert.Equal(Values{"25"}, l2.Cells[2].Value)
	})

	t.Run("insert 13", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		l1.Next = 2
		l1.Prev = 0
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:3]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		l1.Cells[2] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		assert.NoError(b.create(l1))
		assert.Equal(PageNo(1), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		l2.Next = 0
		l2.Prev = 1
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:3]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l2.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.create(l2))
		assert.Equal(PageNo(2), l2.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:1]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Right: l2.PageNo}}
		assert.NoError(b.create(r))
		assert.Equal(PageNo(3), r.PageNo)

		n, err := b.Insert(int(r.PageNo), Values{13}, Values{"13"})
		assert.NoError(err)
		assert.Equal(r.PageNo, PageNo(n))

		r, err = b.get(PageNo(3))
		assert.NoError(err)
		assert.Equal(PageNo(1), r.Left)
		assert.Len(r.Cells, 2)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(4), r.Cells[0].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[1].Key)
		assert.Equal(PageNo(2), r.Cells[1].Right)

		l1, err = b.get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Equal(PageNo(4), l1.Next)
		assert.Equal(PageNo(0), l1.Prev)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.get(PageNo(4))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Equal(PageNo(2), l2.Next)
		assert.Equal(PageNo(1), l2.Prev)
		assert.Len(l2.Cells, 2)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(13)}, l2.Cells[1].Key)
		assert.Equal(Values{"13"}, l2.Cells[1].Value)

		l3, err := b.get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Equal(PageNo(0), l3.Next)
		assert.Equal(PageNo(4), l3.Prev)
		assert.Len(l3.Cells, 3)
		assert.Equal(Values{uint64(16)}, l3.Cells[0].Key)
		assert.Equal(Values{"16"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(20)}, l3.Cells[1].Key)
		assert.Equal(Values{"20"}, l3.Cells[1].Value)
		assert.Equal(Values{uint64(25)}, l3.Cells[2].Key)
		assert.Equal(Values{"25"}, l3.Cells[2].Value)
	})

	t.Run("insert 15", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		l1.Next = 2
		l1.Prev = 0
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(PageNo(1), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		l2.Next = 3
		l2.Prev = 1
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:2]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		assert.NoError(b.create(l2))
		assert.Equal(PageNo(2), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		l3.Next = 0
		l3.Prev = 2
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:3]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l3.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.create(l3))
		assert.Equal(PageNo(3), l3.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:2]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{16}, Right: l3.PageNo}}
		assert.NoError(b.create(r))
		assert.Equal(PageNo(4), r.PageNo)

		n, err := b.Insert(int(r.PageNo), Values{15}, Values{"15"})
		assert.NoError(err)
		assert.Equal(r.PageNo, PageNo(n))

		r, err = b.get(PageNo(4))
		assert.NoError(err)
		assert.Equal(PageNo(1), r.Left)
		assert.Len(r.Cells, 2)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(2), r.Cells[0].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[1].Key)
		assert.Equal(PageNo(3), r.Cells[1].Right)

		l1, err = b.get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Equal(PageNo(2), l1.Next)
		assert.Equal(PageNo(0), l1.Prev)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Equal(PageNo(3), l2.Next)
		assert.Equal(PageNo(1), l2.Prev)
		assert.Len(l2.Cells, 3)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(13)}, l2.Cells[1].Key)
		assert.Equal(Values{"13"}, l2.Cells[1].Value)
		assert.Equal(Values{uint64(15)}, l2.Cells[2].Key)
		assert.Equal(Values{"15"}, l2.Cells[2].Value)

		l3, err = b.get(PageNo(3))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Equal(PageNo(0), l3.Next)
		assert.Equal(PageNo(2), l3.Prev)
		assert.Len(l3.Cells, 3)
		assert.Equal(Values{uint64(16)}, l3.Cells[0].Key)
		assert.Equal(Values{"16"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(20)}, l3.Cells[1].Key)
		assert.Equal(Values{"20"}, l3.Cells[1].Value)
		assert.Equal(Values{uint64(25)}, l3.Cells[2].Key)
		assert.Equal(Values{"25"}, l3.Cells[2].Value)
	})

	t.Run("insert 10", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		l1.Next = 2
		l1.Prev = 0
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(PageNo(1), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		l2.Next = 3
		l2.Prev = 1
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:3]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		l2.Cells[2] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
		assert.NoError(b.create(l2))
		assert.Equal(PageNo(2), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		l3.Next = 0
		l3.Prev = 2
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:3]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l3.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.create(l3))
		assert.Equal(PageNo(3), l3.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:2]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{16}, Right: l3.PageNo}}
		assert.NoError(b.create(r))
		assert.Equal(PageNo(4), r.PageNo)

		n, err := b.Insert(int(r.PageNo), Values{10}, Values{"10"})
		assert.NoError(err)
		assert.Equal(r.PageNo, PageNo(n))

		r, err = b.get(PageNo(4))
		assert.NoError(err)
		assert.Equal(PageNo(1), r.Left)
		assert.Len(r.Cells, 3)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(2), r.Cells[0].Right)
		assert.Equal(Values{uint64(13)}, r.Cells[1].Key)
		assert.Equal(PageNo(5), r.Cells[1].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[2].Key)
		assert.Equal(PageNo(3), r.Cells[2].Right)

		l1, err = b.get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Equal(PageNo(2), l1.Next)
		assert.Equal(PageNo(0), l1.Prev)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Equal(PageNo(5), l2.Next)
		assert.Equal(PageNo(1), l2.Prev)
		assert.Len(l2.Cells, 2)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(10)}, l2.Cells[1].Key)
		assert.Equal(Values{"10"}, l2.Cells[1].Value)

		l3, err = b.get(PageNo(5))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Equal(PageNo(3), l3.Next)
		assert.Equal(PageNo(2), l3.Prev)
		assert.Len(l3.Cells, 2)
		assert.Equal(Values{uint64(13)}, l3.Cells[0].Key)
		assert.Equal(Values{"13"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(15)}, l3.Cells[1].Key)
		assert.Equal(Values{"15"}, l3.Cells[1].Value)

		l4, err := b.get(PageNo(3))
		assert.NoError(err)
		assert.Equal(Leaf, l4.Type)
		assert.Equal(PageNo(0), l4.Next)
		assert.Equal(PageNo(5), l4.Prev)
		assert.Len(l4.Cells, 3)
		assert.Equal(Values{uint64(16)}, l4.Cells[0].Key)
		assert.Equal(Values{"16"}, l4.Cells[0].Value)
		assert.Equal(Values{uint64(20)}, l4.Cells[1].Key)
		assert.Equal(Values{"20"}, l4.Cells[1].Value)
		assert.Equal(Values{uint64(25)}, l4.Cells[2].Key)
		assert.Equal(Values{"25"}, l4.Cells[2].Value)
	})

	t.Run("insert 11", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		l1.Next = 2
		l1.Prev = 0
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(PageNo(1), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		l2.Next = 3
		l2.Prev = 1
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:2]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{10}, Value: Values{"10"}}}
		assert.NoError(b.create(l2))
		assert.Equal(PageNo(2), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		l3.Next = 4
		l3.Prev = 2
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:2]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
		assert.NoError(b.create(l3))
		assert.Equal(PageNo(3), l3.PageNo)

		l4 := NewPage(128, 32)
		l4.Type = Leaf
		l4.Next = 0
		l4.Prev = 3
		assert.Equal(3, cap(l4.Cells))
		l4.Cells = l4.Cells[:3]
		l4.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l4.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l4.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.create(l4))
		assert.Equal(PageNo(4), l4.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:3]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Right: l3.PageNo}}
		r.Cells[2] = Cell{Payload: Payload{Key: Values{16}, Right: l4.PageNo}}
		assert.NoError(b.create(r))
		assert.Equal(PageNo(5), r.PageNo)

		n, err := b.Insert(int(r.PageNo), Values{11}, Values{"11"})
		assert.NoError(err)
		assert.Equal(r.PageNo, PageNo(n))

		r, err = b.get(PageNo(5))
		assert.NoError(err)
		assert.Equal(PageNo(1), r.Left)
		assert.Len(r.Cells, 3)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(2), r.Cells[0].Right)
		assert.Equal(Values{uint64(13)}, r.Cells[1].Key)
		assert.Equal(PageNo(3), r.Cells[1].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[2].Key)
		assert.Equal(PageNo(4), r.Cells[2].Right)

		l1, err = b.get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Equal(PageNo(2), l1.Next)
		assert.Equal(PageNo(0), l1.Prev)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Equal(PageNo(3), l2.Next)
		assert.Equal(PageNo(1), l2.Prev)
		assert.Len(l2.Cells, 3)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(10)}, l2.Cells[1].Key)
		assert.Equal(Values{"10"}, l2.Cells[1].Value)
		assert.Equal(Values{uint64(11)}, l2.Cells[2].Key)
		assert.Equal(Values{"11"}, l2.Cells[2].Value)

		l3, err = b.get(PageNo(3))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Equal(PageNo(4), l3.Next)
		assert.Equal(PageNo(2), l3.Prev)
		assert.Len(l3.Cells, 2)
		assert.Equal(Values{uint64(13)}, l3.Cells[0].Key)
		assert.Equal(Values{"13"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(15)}, l3.Cells[1].Key)
		assert.Equal(Values{"15"}, l3.Cells[1].Value)

		l4, err = b.get(PageNo(4))
		assert.NoError(err)
		assert.Equal(Leaf, l4.Type)
		assert.Equal(PageNo(0), l4.Next)
		assert.Equal(PageNo(3), l4.Prev)
		assert.Len(l4.Cells, 3)
		assert.Equal(Values{uint64(16)}, l4.Cells[0].Key)
		assert.Equal(Values{"16"}, l4.Cells[0].Value)
		assert.Equal(Values{uint64(20)}, l4.Cells[1].Key)
		assert.Equal(Values{"20"}, l4.Cells[1].Value)
		assert.Equal(Values{uint64(25)}, l4.Cells[2].Key)
		assert.Equal(Values{"25"}, l4.Cells[2].Value)
	})

	t.Run("insert 12", func(t *testing.T) {
		assert := assert.New(t)

		dir, err := ioutil.TempDir("", "test")
		assert.NoError(err)
		defer func() { assert.NoError(os.RemoveAll(dir)) }()

		b, err := Create(filepath.Join(dir, "test.db"), PageSize(128), CellSize(32))
		assert.NoError(err)

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		l1.Next = 2
		l1.Prev = 0
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.create(l1))
		assert.Equal(PageNo(1), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		l2.Next = 3
		l2.Prev = 1
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:3]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{10}, Value: Values{"10"}}}
		l2.Cells[2] = Cell{Payload: Payload{Key: Values{11}, Value: Values{"11"}}}
		assert.NoError(b.create(l2))
		assert.Equal(PageNo(2), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		l3.Next = 4
		l3.Prev = 2
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:2]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
		assert.NoError(b.create(l3))
		assert.Equal(PageNo(3), l3.PageNo)

		l4 := NewPage(128, 32)
		l4.Type = Leaf
		l4.Next = 0
		l4.Prev = 3
		assert.Equal(3, cap(l4.Cells))
		l4.Cells = l4.Cells[:3]
		l4.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l4.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l4.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.create(l4))
		assert.Equal(PageNo(4), l4.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:3]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Right: l3.PageNo}}
		r.Cells[2] = Cell{Payload: Payload{Key: Values{16}, Right: l4.PageNo}}
		assert.NoError(b.create(r))
		assert.Equal(PageNo(5), r.PageNo)

		n, err := b.Insert(int(r.PageNo), Values{12}, Values{"12"})
		assert.NoError(err)
		assert.Equal(PageNo(8), PageNo(n)) // new root

		r, err = b.get(PageNo(8))
		assert.NoError(err)
		assert.Equal(PageNo(5), r.Left)
		assert.Len(r.Cells, 1)
		assert.Equal(Values{uint64(13)}, r.Cells[0].Key)
		assert.Equal(PageNo(7), r.Cells[0].Right)

		i1, err := b.get(PageNo(5))
		assert.NoError(err)
		assert.Equal(Branch, i1.Type)
		assert.Equal(PageNo(1), i1.Left)
		assert.Len(i1.Cells, 2)
		assert.Equal(Values{uint64(9)}, i1.Cells[0].Key)
		assert.Equal(PageNo(2), i1.Cells[0].Right)
		assert.Equal(Values{uint64(11)}, i1.Cells[1].Key)
		assert.Equal(PageNo(6), i1.Cells[1].Right)

		i2, err := b.get(PageNo(7))
		assert.NoError(err)
		assert.Equal(Branch, i2.Type)
		assert.Equal(PageNo(3), i2.Left)
		assert.Len(i2.Cells, 1)
		assert.Equal(Values{uint64(16)}, i2.Cells[0].Key)
		assert.Equal(PageNo(4), i2.Cells[0].Right)

		l1, err = b.get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Equal(PageNo(2), l1.Next)
		assert.Equal(PageNo(0), l1.Prev)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Equal(PageNo(6), l2.Next)
		assert.Equal(PageNo(1), l2.Prev)
		assert.Len(l2.Cells, 2)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(10)}, l2.Cells[1].Key)
		assert.Equal(Values{"10"}, l2.Cells[1].Value)

		l3, err = b.get(PageNo(6))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Equal(PageNo(3), l3.Next)
		assert.Equal(PageNo(2), l3.Prev)
		assert.Len(l3.Cells, 2)
		assert.Equal(Values{uint64(11)}, l3.Cells[0].Key)
		assert.Equal(Values{"11"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(12)}, l3.Cells[1].Key)
		assert.Equal(Values{"12"}, l3.Cells[1].Value)

		l4, err = b.get(PageNo(3))
		assert.NoError(err)
		assert.Equal(Leaf, l4.Type)
		assert.Equal(PageNo(4), l4.Next)
		assert.Equal(PageNo(6), l4.Prev)
		assert.Len(l4.Cells, 2)
		assert.Equal(Values{uint64(13)}, l4.Cells[0].Key)
		assert.Equal(Values{"13"}, l4.Cells[0].Value)
		assert.Equal(Values{uint64(15)}, l4.Cells[1].Key)
		assert.Equal(Values{"15"}, l4.Cells[1].Value)

		l5, err := b.get(PageNo(4))
		assert.NoError(err)
		assert.Equal(Leaf, l5.Type)
		assert.Equal(PageNo(0), l5.Next)
		assert.Equal(PageNo(3), l5.Prev)
		assert.Len(l5.Cells, 3)
		assert.Equal(Values{uint64(16)}, l5.Cells[0].Key)
		assert.Equal(Values{"16"}, l5.Cells[0].Value)
		assert.Equal(Values{uint64(20)}, l5.Cells[1].Key)
		assert.Equal(Values{"20"}, l5.Cells[1].Value)
		assert.Equal(Values{uint64(25)}, l5.Cells[2].Key)
		assert.Equal(Values{"25"}, l5.Cells[2].Value)
	})
}
