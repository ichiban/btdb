package btree

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Insert(t *testing.T) {
	assert := assert.New(t)

	f, err := ioutil.TempFile("", "test")
	assert.NoError(err)
	defer assert.NoError(os.Remove(f.Name()))

	b := BTree{
		File:     f,
		PageSize: 32,
		CellSize: 16,
	}

	p := NewPage(32, 16)
	p.Type = Leaf
	_, err = p.WriteTo(f)
	assert.NoError(err)

	assert.NoError(b.Insert([]byte("x"), []byte("1")))

	assert.NoError(b.Get(&p, 0))
	assert.Equal(Leaf, p.Type)
	assert.Equal(PageNo(0), p.Next)
	assert.Len(p.Cells, 1)
	assert.Equal(PageNo(0), p.Cells[0].Overflow)
	assert.Equal(PageNo(0), p.Cells[0].Left)
	assert.Equal([]byte("x1"), p.Cells[0].Payload)

	v, err := b.Search([]byte("x"))
	assert.NoError(err)
	assert.Equal([]byte("1"), v)
}
