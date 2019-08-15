package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValues_Compare(t *testing.T) {
	t.Run("less", func(t *testing.T) {
		assert := assert.New(t)
		assert.Equal(-1, values{1}.compare(values{2}))
		assert.Equal(-1, values{1, 2}.compare(values{1, 3}))
	})

	t.Run("equal", func(t *testing.T) {
		assert := assert.New(t)
		assert.Equal(0, values{1}.compare(values{1}))
		assert.Equal(0, values{1, 2}.compare(values{1, 2}))
		assert.Equal(0, values{"x"}.compare(values{"x"}))
	})

	t.Run("greater", func(t *testing.T) {
		assert := assert.New(t)
		assert.Equal(1, values{2}.compare(values{1}))
		assert.Equal(1, values{1, 3}.compare(values{1, 2}))
	})
}
