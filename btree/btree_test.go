package btree

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Insert(t *testing.T) {
	// example from http://www.cburch.com/cs/340/reading/btree/index.html

	t.Run("insert 20", func(t *testing.T) {
		assert := assert.New(t)

		f, err := ioutil.TempFile("", "test")
		assert.NoError(err)
		defer assert.NoError(os.Remove(f.Name()))

		b := BTree{
			File:     f,
			PageSize: 128,
			CellSize: 32,
		}

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:3]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		l1.Cells[2] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		assert.NoError(b.Create(l1))

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:2]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.Create(l2))

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:1]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Right: l2.PageNo}}
		assert.NoError(b.Create(r))

		n, err := b.Insert(r.PageNo, Values{20}, Values{"20"})
		assert.NoError(err)
		assert.Equal(r.PageNo, n)

		r, err = b.Get(PageNo(2))
		assert.NoError(err)
		assert.Equal(PageNo(0), r.Left)
		assert.Len(r.Cells, 1)
		assert.Equal(Values{uint64(16)}, r.Cells[0].Key)
		assert.Equal(PageNo(1), r.Cells[0].Right)

		l1, err = b.Get(PageNo(0))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Len(l1.Cells, 3)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)
		assert.Equal(Values{uint64(9)}, l1.Cells[2].Key)
		assert.Equal(Values{"9"}, l1.Cells[2].Value)

		l2, err = b.Get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
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

		f, err := ioutil.TempFile("", "test")
		assert.NoError(err)
		defer assert.NoError(os.Remove(f.Name()))

		b := BTree{
			File:     f,
			PageSize: 128,
			CellSize: 32,
		}

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:3]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		l1.Cells[2] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		assert.NoError(b.Create(l1))

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:3]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l2.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.Create(l2))

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:1]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Right: l2.PageNo}}
		assert.NoError(b.Create(r))

		n, err := b.Insert(r.PageNo, Values{13}, Values{"13"})
		assert.NoError(err)
		assert.Equal(r.PageNo, n)

		r, err = b.Get(PageNo(2))
		assert.NoError(err)
		assert.Equal(PageNo(0), r.Left)
		assert.Len(r.Cells, 2)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(3), r.Cells[0].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[1].Key)
		assert.Equal(PageNo(1), r.Cells[1].Right)

		l1, err = b.Get(PageNo(0))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.Get(PageNo(3))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Len(l2.Cells, 2)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(13)}, l2.Cells[1].Key)
		assert.Equal(Values{"13"}, l2.Cells[1].Value)

		l3, err := b.Get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
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

		f, err := ioutil.TempFile("", "test")
		assert.NoError(err)
		defer assert.NoError(os.Remove(f.Name()))

		b := BTree{
			File:     f,
			PageSize: 128,
			CellSize: 32,
		}

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.Create(l1))
		assert.Equal(PageNo(0), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:2]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		assert.NoError(b.Create(l2))
		assert.Equal(PageNo(1), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:3]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l3.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.Create(l3))
		assert.Equal(PageNo(2), l3.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:2]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{16}, Right: l3.PageNo}}
		assert.NoError(b.Create(r))
		assert.Equal(PageNo(3), r.PageNo)

		n, err := b.Insert(r.PageNo, Values{15}, Values{"15"})
		assert.NoError(err)
		assert.Equal(r.PageNo, n)

		r, err = b.Get(PageNo(3))
		assert.NoError(err)
		assert.Equal(PageNo(0), r.Left)
		assert.Len(r.Cells, 2)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(1), r.Cells[0].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[1].Key)
		assert.Equal(PageNo(2), r.Cells[1].Right)

		l1, err = b.Get(PageNo(0))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.Get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Len(l2.Cells, 3)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(13)}, l2.Cells[1].Key)
		assert.Equal(Values{"13"}, l2.Cells[1].Value)
		assert.Equal(Values{uint64(15)}, l2.Cells[2].Key)
		assert.Equal(Values{"15"}, l2.Cells[2].Value)

		l3, err = b.Get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
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

		f, err := ioutil.TempFile("", "test")
		assert.NoError(err)
		defer assert.NoError(os.Remove(f.Name()))

		b := BTree{
			File:     f,
			PageSize: 128,
			CellSize: 32,
		}

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.Create(l1))
		assert.Equal(PageNo(0), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:3]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		l2.Cells[2] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
		assert.NoError(b.Create(l2))
		assert.Equal(PageNo(1), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:3]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l3.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.Create(l3))
		assert.Equal(PageNo(2), l3.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:2]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{16}, Right: l3.PageNo}}
		assert.NoError(b.Create(r))
		assert.Equal(PageNo(3), r.PageNo)

		n, err := b.Insert(r.PageNo, Values{10}, Values{"10"})
		assert.NoError(err)
		assert.Equal(r.PageNo, n)

		r, err = b.Get(PageNo(3))
		assert.NoError(err)
		assert.Equal(PageNo(0), r.Left)
		assert.Len(r.Cells, 3)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(1), r.Cells[0].Right)
		assert.Equal(Values{uint64(13)}, r.Cells[1].Key)
		assert.Equal(PageNo(4), r.Cells[1].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[2].Key)
		assert.Equal(PageNo(2), r.Cells[2].Right)

		l1, err = b.Get(PageNo(0))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.Get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Len(l2.Cells, 2)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(10)}, l2.Cells[1].Key)
		assert.Equal(Values{"10"}, l2.Cells[1].Value)

		l3, err = b.Get(PageNo(4))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Len(l3.Cells, 2)
		assert.Equal(Values{uint64(13)}, l3.Cells[0].Key)
		assert.Equal(Values{"13"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(15)}, l3.Cells[1].Key)
		assert.Equal(Values{"15"}, l3.Cells[1].Value)

		l4, err := b.Get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l4.Type)
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

		f, err := ioutil.TempFile("", "test")
		assert.NoError(err)
		defer assert.NoError(os.Remove(f.Name()))

		b := BTree{
			File:     f,
			PageSize: 128,
			CellSize: 32,
		}

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.Create(l1))
		assert.Equal(PageNo(0), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:2]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{10}, Value: Values{"10"}}}
		assert.NoError(b.Create(l2))
		assert.Equal(PageNo(1), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:2]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
		assert.NoError(b.Create(l3))
		assert.Equal(PageNo(2), l3.PageNo)

		l4 := NewPage(128, 32)
		l4.Type = Leaf
		assert.Equal(3, cap(l4.Cells))
		l4.Cells = l4.Cells[:3]
		l4.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l4.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l4.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.Create(l4))
		assert.Equal(PageNo(3), l4.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:3]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Right: l3.PageNo}}
		r.Cells[2] = Cell{Payload: Payload{Key: Values{16}, Right: l4.PageNo}}
		assert.NoError(b.Create(r))
		assert.Equal(PageNo(4), r.PageNo)

		n, err := b.Insert(r.PageNo, Values{11}, Values{"11"})
		assert.NoError(err)
		assert.Equal(r.PageNo, n)

		r, err = b.Get(PageNo(4))
		assert.NoError(err)
		assert.Equal(PageNo(0), r.Left)
		assert.Len(r.Cells, 3)
		assert.Equal(Values{uint64(9)}, r.Cells[0].Key)
		assert.Equal(PageNo(1), r.Cells[0].Right)
		assert.Equal(Values{uint64(13)}, r.Cells[1].Key)
		assert.Equal(PageNo(2), r.Cells[1].Right)
		assert.Equal(Values{uint64(16)}, r.Cells[2].Key)
		assert.Equal(PageNo(3), r.Cells[2].Right)

		l1, err = b.Get(PageNo(0))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.Get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Len(l2.Cells, 3)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(10)}, l2.Cells[1].Key)
		assert.Equal(Values{"10"}, l2.Cells[1].Value)
		assert.Equal(Values{uint64(11)}, l2.Cells[2].Key)
		assert.Equal(Values{"11"}, l2.Cells[2].Value)

		l3, err = b.Get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Len(l3.Cells, 2)
		assert.Equal(Values{uint64(13)}, l3.Cells[0].Key)
		assert.Equal(Values{"13"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(15)}, l3.Cells[1].Key)
		assert.Equal(Values{"15"}, l3.Cells[1].Value)

		l4, err = b.Get(PageNo(3))
		assert.NoError(err)
		assert.Equal(Leaf, l4.Type)
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

		f, err := ioutil.TempFile("", "test")
		assert.NoError(err)
		defer assert.NoError(os.Remove(f.Name()))

		b := BTree{
			File:     f,
			PageSize: 128,
			CellSize: 32,
		}

		l1 := NewPage(128, 32)
		l1.Type = Leaf
		assert.Equal(3, cap(l1.Cells))
		l1.Cells = l1.Cells[:2]
		l1.Cells[0] = Cell{Payload: Payload{Key: Values{1}, Value: Values{"1"}}}
		l1.Cells[1] = Cell{Payload: Payload{Key: Values{4}, Value: Values{"4"}}}
		assert.NoError(b.Create(l1))
		assert.Equal(PageNo(0), l1.PageNo)

		l2 := NewPage(128, 32)
		l2.Type = Leaf
		assert.Equal(3, cap(l2.Cells))
		l2.Cells = l2.Cells[:3]
		l2.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Value: Values{"9"}}}
		l2.Cells[1] = Cell{Payload: Payload{Key: Values{10}, Value: Values{"10"}}}
		l2.Cells[2] = Cell{Payload: Payload{Key: Values{11}, Value: Values{"11"}}}
		assert.NoError(b.Create(l2))
		assert.Equal(PageNo(1), l2.PageNo)

		l3 := NewPage(128, 32)
		l3.Type = Leaf
		assert.Equal(3, cap(l3.Cells))
		l3.Cells = l3.Cells[:2]
		l3.Cells[0] = Cell{Payload: Payload{Key: Values{13}, Value: Values{"13"}}}
		l3.Cells[1] = Cell{Payload: Payload{Key: Values{15}, Value: Values{"15"}}}
		assert.NoError(b.Create(l3))
		assert.Equal(PageNo(2), l3.PageNo)

		l4 := NewPage(128, 32)
		l4.Type = Leaf
		assert.Equal(3, cap(l4.Cells))
		l4.Cells = l4.Cells[:3]
		l4.Cells[0] = Cell{Payload: Payload{Key: Values{16}, Value: Values{"16"}}}
		l4.Cells[1] = Cell{Payload: Payload{Key: Values{20}, Value: Values{"20"}}}
		l4.Cells[2] = Cell{Payload: Payload{Key: Values{25}, Value: Values{"25"}}}
		assert.NoError(b.Create(l4))
		assert.Equal(PageNo(3), l4.PageNo)

		r := NewPage(128, 32)
		r.Type = Branch
		r.Left = l1.PageNo
		assert.Equal(3, cap(r.Cells))
		r.Cells = r.Cells[:3]
		r.Cells[0] = Cell{Payload: Payload{Key: Values{9}, Right: l2.PageNo}}
		r.Cells[1] = Cell{Payload: Payload{Key: Values{13}, Right: l3.PageNo}}
		r.Cells[2] = Cell{Payload: Payload{Key: Values{16}, Right: l4.PageNo}}
		assert.NoError(b.Create(r))
		assert.Equal(PageNo(4), r.PageNo)

		n, err := b.Insert(r.PageNo, Values{12}, Values{"12"})
		assert.NoError(err)
		assert.Equal(PageNo(7), n) // new root

		r, err = b.Get(PageNo(7))
		assert.NoError(err)
		assert.Equal(PageNo(4), r.Left)
		assert.Len(r.Cells, 1)
		assert.Equal(Values{uint64(13)}, r.Cells[0].Key)
		assert.Equal(PageNo(6), r.Cells[0].Right)

		i1, err := b.Get(PageNo(4))
		assert.NoError(err)
		assert.Equal(Branch, i1.Type)
		assert.Equal(PageNo(0), i1.Left)
		assert.Len(i1.Cells, 2)
		assert.Equal(Values{uint64(9)}, i1.Cells[0].Key)
		assert.Equal(PageNo(1), i1.Cells[0].Right)
		assert.Equal(Values{uint64(11)}, i1.Cells[1].Key)
		assert.Equal(PageNo(5), i1.Cells[1].Right)

		i2, err := b.Get(PageNo(6))
		assert.NoError(err)
		assert.Equal(Branch, i2.Type)
		assert.Equal(PageNo(2), i2.Left)
		assert.Len(i2.Cells, 1)
		assert.Equal(Values{uint64(16)}, i2.Cells[0].Key)
		assert.Equal(PageNo(3), i2.Cells[0].Right)

		l1, err = b.Get(PageNo(0))
		assert.NoError(err)
		assert.Equal(Leaf, l1.Type)
		assert.Len(l1.Cells, 2)
		assert.Equal(Values{uint64(1)}, l1.Cells[0].Key)
		assert.Equal(Values{"1"}, l1.Cells[0].Value)
		assert.Equal(Values{uint64(4)}, l1.Cells[1].Key)
		assert.Equal(Values{"4"}, l1.Cells[1].Value)

		l2, err = b.Get(PageNo(1))
		assert.NoError(err)
		assert.Equal(Leaf, l2.Type)
		assert.Len(l2.Cells, 2)
		assert.Equal(Values{uint64(9)}, l2.Cells[0].Key)
		assert.Equal(Values{"9"}, l2.Cells[0].Value)
		assert.Equal(Values{uint64(10)}, l2.Cells[1].Key)
		assert.Equal(Values{"10"}, l2.Cells[1].Value)

		l3, err = b.Get(PageNo(5))
		assert.NoError(err)
		assert.Equal(Leaf, l3.Type)
		assert.Len(l3.Cells, 2)
		assert.Equal(Values{uint64(11)}, l3.Cells[0].Key)
		assert.Equal(Values{"11"}, l3.Cells[0].Value)
		assert.Equal(Values{uint64(12)}, l3.Cells[1].Key)
		assert.Equal(Values{"12"}, l3.Cells[1].Value)

		l4, err = b.Get(PageNo(2))
		assert.NoError(err)
		assert.Equal(Leaf, l4.Type)
		assert.Len(l4.Cells, 2)
		assert.Equal(Values{uint64(13)}, l4.Cells[0].Key)
		assert.Equal(Values{"13"}, l4.Cells[0].Value)
		assert.Equal(Values{uint64(15)}, l4.Cells[1].Key)
		assert.Equal(Values{"15"}, l4.Cells[1].Value)

		l5, err := b.Get(PageNo(3))
		assert.NoError(err)
		assert.Equal(Leaf, l5.Type)
		assert.Len(l5.Cells, 3)
		assert.Equal(Values{uint64(16)}, l5.Cells[0].Key)
		assert.Equal(Values{"16"}, l5.Cells[0].Value)
		assert.Equal(Values{uint64(20)}, l5.Cells[1].Key)
		assert.Equal(Values{"20"}, l5.Cells[1].Value)
		assert.Equal(Values{uint64(25)}, l5.Cells[2].Key)
		assert.Equal(Values{"25"}, l5.Cells[2].Value)
	})
}
