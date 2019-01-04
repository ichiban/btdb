package btree

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCell_ReadFrom(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{})

		c := NewCell(16)
		_, err := c.ReadFrom(r)
		assert.Equal(io.EOF, err)
	})

	t.Run("zero length", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x00, // size: 0
			0x00, 0x00, 0x00, 0x00, // padding
		})

		c := NewCell(16)
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(16), n)

		assert.Equal(PageNo(0), c.Overflow)
		assert.Equal(PageNo(0), c.Left)
		assert.Equal([]byte{}, c.Payload)
	})

	t.Run("some", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x01, // size: 1
			0x01, 0x00, 0x00, 0x00,
		})

		c := NewCell(16)
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(16), n)

		assert.Equal(PageNo(0), c.Overflow)
		assert.Equal(PageNo(0), c.Left)
		assert.Equal([]byte{0x01}, c.Payload)
	})

	t.Run("overflow", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x01, // overflow: 1
			0x00, 0x00, 0x00, 0x01, // left: 1
			0x00, 0x00, 0x00, 0x04, // size: 4
			0x01, 0x02, 0x03, 0x04,
		})

		c := NewCell(16)
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(16), n)

		assert.Equal(PageNo(1), c.Overflow)
		assert.Equal(PageNo(1), c.Left)
		assert.Equal([]byte{0x01, 0x02, 0x03, 0x04}, c.Payload)
	})
}

func TestCell_WriteTo(t *testing.T) {
	t.Run("zero length", func(t *testing.T) {
		assert := assert.New(t)

		c := NewCell(16)

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(16), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x00, // size: 0
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})

	t.Run("some", func(t *testing.T) {
		assert := assert.New(t)

		c := NewCell(16)
		copy(c.Payload, []byte{0x01})

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(16), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x00, // left: 0
			0x00, 0x00, 0x00, 0x01, // size: 1
			0x01, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})

	t.Run("overflow", func(t *testing.T) {
		assert := assert.New(t)

		c := NewCell(16)
		c.Overflow = 1
		c.Left = 1
		copy(c.Payload, []byte{0x01, 0x02, 0x03, 0x04})

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(16), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x01, // overflow: 1
			0x00, 0x00, 0x00, 0x01, // left: 1
			0x00, 0x00, 0x00, 0x04, // size: 4
			0x01, 0x02, 0x03, 0x04,
		}, w.Bytes())
	})
}
