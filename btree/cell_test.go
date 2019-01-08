package btree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCell_ReadFrom(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{})

		c := NewCell(32)
		_, err := c.ReadFrom(r)
		assert.Error(err)
	})

	t.Run("zero length", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x00, // key size: 0
			0x00, 0x00, 0x00, 0x00, // value size: 0

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		})

		c := NewCell(32)
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal(PageNo(0), c.Overflow)
		assert.Equal(PageNo(0), c.Left)
		assert.Equal([]byte{}, c.Key)
		assert.Equal([]byte{}, c.Value)
	})

	t.Run("some", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x01, // key size: 1
			0x00, 0x00, 0x00, 0x00, // value size: 0

			0x01, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		})

		c := NewCell(32)
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal(PageNo(0), c.Overflow)
		assert.Equal(PageNo(0), c.Left)
		assert.Equal([]byte{0x01}, c.Key)
		assert.Equal([]byte{}, c.Value)
	})

	t.Run("overflow", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x01, // overflow: 1
			0x00, 0x00, 0x00, 0x01, // left: 1
			0x00, 0x00, 0x00, 0x02, // key size: 2
			0x00, 0x00, 0x00, 0x02, // value size: 2

			0x01, 0x02, 0x03, 0x04,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		})

		c := NewCell(32)
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal(PageNo(1), c.Overflow)
		assert.Equal(PageNo(1), c.Left)
		assert.Equal([]byte{0x01, 0x02}, c.Key)
		assert.Equal([]byte{0x03, 0x04}, c.Value)
	})
}

func TestCell_WriteTo(t *testing.T) {
	t.Run("zero length", func(t *testing.T) {
		assert := assert.New(t)

		c := NewCell(32)

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x00, // key size: 0
			0x00, 0x00, 0x00, 0x00, // value size: 0

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})

	t.Run("some", func(t *testing.T) {
		assert := assert.New(t)

		c := NewCell(32)
		c.Key = []byte{0x01}

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x01, // key size: 1
			0x00, 0x00, 0x00, 0x00, // value size: 0

			0x01, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})

	t.Run("overflow", func(t *testing.T) {
		assert := assert.New(t)

		c := NewCell(32)
		c.Overflow = 1
		c.Left = 1
		c.Key = []byte{0x01, 0x02}
		c.Value = []byte{0x03, 0x04}

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x01, // overflow: 1
			0x00, 0x00, 0x00, 0x01, // left: 1
			0x00, 0x00, 0x00, 0x02, // key size: 2
			0x00, 0x00, 0x00, 0x02, // value size: 2

			0x01, 0x02, 0x03, 0x04,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})
}
