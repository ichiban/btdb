package store

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCell_ReadFrom(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{})

		c := cell{size: 32}
		_, err := c.ReadFrom(r)
		assert.Error(err)
	})

	t.Run("zero length", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x01, // payload size: 1
			0xa0, 0x00, 0x00, 0x00, // payload: {}
			0x00, 0x00, 0x00, 0x00,

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		})

		c := cell{size: 32}
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal(pageNo(0), c.overflow)
		assert.Equal(pageNo(0), c.Right)
		assert.Nil(c.Key)
		assert.Nil(c.Value)
	})

	t.Run("some", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x04, // payload size: 4
			0xa1, 0x01, 0x81, 0x01, // payload: {1:[1]}
			0x00, 0x00, 0x00, 0x00,

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		})

		c := cell{size: 32}
		c.Key = values{int(0)} // type hint
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal(pageNo(0), c.overflow)
		assert.Equal(pageNo(0), c.Right)
		assert.Equal(values{1}, c.Key)
		assert.Nil(c.Value)
	})

	t.Run("overflow", func(t *testing.T) {
		assert := assert.New(t)

		r := bytes.NewReader([]byte{
			0x00, 0x00, 0x00, 0x01, // overflow: 1
			0x00, 0x00, 0x00, 0x11, // payload size: 11
			0xa3, 0x01, 0x82, 0x01, // payload: {1:[1,2],2:[3,4],3:1}
			0x02, 0x02, 0x82, 0x03,

			0x04, 0x03, 0x01, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		})

		c := cell{size: 32}
		c.Key = values{int(0), int(0)}   // type hint
		c.Value = values{int(0), int(0)} // type hint
		n, err := c.ReadFrom(r)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal(pageNo(1), c.overflow)
		assert.Equal(pageNo(1), c.Right)
		assert.Equal(values{1, 2}, c.Key)
		assert.Equal(values{3, 4}, c.Value)
	})
}

func TestCell_WriteTo(t *testing.T) {
	t.Run("zero length", func(t *testing.T) {
		assert := assert.New(t)

		c := cell{size: 32}

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x01, // payload size: 1
			0xa0, 0x00, 0x00, 0x00, // payload: {}
			0x00, 0x00, 0x00, 0x00,

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})

	t.Run("some", func(t *testing.T) {
		assert := assert.New(t)

		c := cell{size: 32}
		c.Key = values{1}

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x00, // overflow: 0
			0x00, 0x00, 0x00, 0x04, // payload size: 4
			0xa1, 0x01, 0x81, 0x01, // payload: {1:[1]}
			0x00, 0x00, 0x00, 0x00,

			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})

	t.Run("overflow", func(t *testing.T) {
		assert := assert.New(t)

		c := cell{size: 32}
		c.overflow = 1
		c.Right = 1
		c.Key = values{1, 2}
		c.Value = values{3, 4}

		var w bytes.Buffer
		n, err := c.WriteTo(&w)
		assert.NoError(err)
		assert.Equal(int64(32), n)

		assert.Equal([]byte{
			0x00, 0x00, 0x00, 0x01, // overflow: 1
			0x00, 0x00, 0x00, 0x0b, // payload size: 11
			0xa3, 0x01, 0x82, 0x01, // payload: {1:[1, 2], 2:[3, 4], 3:1}
			0x02, 0x02, 0x82, 0x03,

			0x04, 0x03, 0x01, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}, w.Bytes())
	})
}
