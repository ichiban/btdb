package btree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPage_ReadFrom(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{})

		p := NewPage(128, 16)
		_, err := p.ReadFrom(r)
		assert.Error(err)
	})

	t.Run("blank", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x01, 0x00, 0x00, 0x00, // page type: branch
			0x00, 0x00, 0x00, 0x00, // page prev: 0
			0x00, 0x00, 0x00, 0x00, // page next: 0
			0x00, 0x00, 0x00, 0x00, // page cell count: 0

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
			0x00, 0x00, 0x00, 0x00,
		})

		p := NewPage(128, 16)

		n, err := p.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(128), n)

		assert.Equal(Branch, p.Type)
		assert.Equal(PageNo(0), p.Next)
		assert.Len(p.Cells, 0)
	})

	t.Run("single cell", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x01, 0x00, 0x00, 0x00, // page type: branch
			0x00, 0x00, 0x00, 0x00, // page prev: 0
			0x00, 0x00, 0x00, 0x00, // page next: 0
			0x00, 0x00, 0x00, 0x01, // page cell count: 1

			0x00, 0x00, 0x00, 0x00, // cell overflow: 0
			0x00, 0x00, 0x00, 0x00, // cell left: 0
			0x00, 0x00, 0x00, 0x00, // cell key size: 0
			0x00, 0x00, 0x00, 0x00, // cell value size: 0

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
		})

		p := NewPage(128, 16)

		n, err := p.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(128), n)

		assert.Equal(Branch, p.Type)
		assert.Equal(PageNo(0), p.Next)
		assert.Len(p.Cells, 1)
		//		assert.Equal(PageNo(0), p.Cells[0].Overflow)
	})

	t.Run("multiple cells", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x01, 0x00, 0x00, 0x00, // page type: branch
			0x00, 0x00, 0x00, 0x00, // page prev: 0
			0x00, 0x00, 0x00, 0x00, // page next: 0
			0x00, 0x00, 0x00, 0x03, // page cell count: 3

			0x00, 0x00, 0x00, 0x00, // cell overflow: 0
			0x00, 0x00, 0x00, 0x00, // cell left: 0
			0x00, 0x00, 0x00, 0x00, // cell key size: 0
			0x00, 0x00, 0x00, 0x00, // cell value size: 0

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,

			0x00, 0x00, 0x00, 0x00, // cell overflow: 0
			0x00, 0x00, 0x00, 0x00, // cell left: 0
			0x00, 0x00, 0x00, 0x00, // cell key size: 0
			0x00, 0x00, 0x00, 0x00, // cell value size: 0

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,

			0x00, 0x00, 0x00, 0x00, // cell overflow: 0
			0x00, 0x00, 0x00, 0x00, // cell left: 0
			0x00, 0x00, 0x00, 0x00, // cell key size: 0
			0x00, 0x00, 0x00, 0x00, // cell value size: 0

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		})

		p := NewPage(128, 16)

		n, err := p.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(128), n)

		assert.Equal(Branch, p.Type)
		assert.Equal(PageNo(0), p.Next)
		assert.Len(p.Cells, 3)
	})
}

func TestPage_WriteTo(t *testing.T) {
	t.Run("blank", func(t *testing.T) {
		assert := assert.New(t)

		p := NewPage(32, 16)
		p.Type = Branch

		var w bytes.Buffer
		n, err := p.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x01, 0x00, 0x00, 0x00, // page type: branch
			0x00, 0x00, 0x00, 0x00, // page prev: 0
			0x00, 0x00, 0x00, 0x00, // page next: 0
			0x00, 0x00, 0x00, 0x00, // page cell count: 0

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})

	t.Run("single cell", func(t *testing.T) {
		assert := assert.New(t)

		p := NewPage(32, 16)
		p.Type = Branch
		p.Cells.Set([]*Cell{
			NewCell(16),
		})

		var w bytes.Buffer
		n, err := p.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x01, 0x00, 0x00, 0x00, // page type: branch
			0x00, 0x00, 0x00, 0x00, // page prev: 0
			0x00, 0x00, 0x00, 0x00, // page next: 0
			0x00, 0x00, 0x00, 0x01, // page cell count: 1

			0x00, 0x00, 0x00, 0x00, // cell overflow: 0
			0x00, 0x00, 0x00, 0x00, // cell left: 0
			0x00, 0x00, 0x00, 0x00, // cell size: 0
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})
}
