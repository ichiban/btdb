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
		PageSize: 128,
		CellSize: 32,
	}

	// TODO: the root page has to be created somewhere else.
	p := NewPage(128, 32)
	p.Type = Leaf
	assert.NoError(b.Create(p))

	assert.NoError(b.Insert(Values{"x"}, Values{1}))
	v, err := b.Search(Values{"x"})
	assert.NoError(err)
	assert.Equal(Values{uint64(1)}, v)

	assert.NoError(b.Insert(Values{"y"}, Values{2}))
	v, err = b.Search(Values{"y"})
	assert.NoError(err)
	assert.Equal(Values{uint64(2)}, v)

	// duplicate key
	assert.Error(b.Insert(Values{"x"}, Values{2}))
}

func TestBTree_Delete(t *testing.T) {
	assert := assert.New(t)

	f, err := ioutil.TempFile("", "test")
	assert.NoError(err)
	defer assert.NoError(os.Remove(f.Name()))

	b := BTree{
		File:     f,
		PageSize: 128,
		CellSize: 32,
	}

	// TODO: the root page has to be created somewhere else.
	p := NewPage(128, 32)
	p.Type = Leaf
	assert.NoError(b.Create(p))

	assert.NoError(b.Insert(Values{"x"}, Values{1}))
	v, err := b.Search(Values{"x"})
	assert.NoError(err)
	assert.Equal(Values{uint64(1)}, v)

	assert.NoError(b.Delete(Values{"x"}))
	_, err = b.Search(Values{"x"})
	assert.Error(err)
}
