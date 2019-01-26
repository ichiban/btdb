package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValues_Compare(t *testing.T) {
	t.Run("less", func(t *testing.T) {
		assert := assert.New(t)
		assert.Equal(-1, Values{1}.Compare(Values{2}))
		assert.Equal(-1, Values{1, 2}.Compare(Values{1, 3}))
	})

	t.Run("equal", func(t *testing.T) {
		assert := assert.New(t)
		assert.Equal(0, Values{1}.Compare(Values{1}))
		assert.Equal(0, Values{1, 2}.Compare(Values{1, 2}))
		assert.Equal(0, Values{"x"}.Compare(Values{"x"}))
	})

	t.Run("greater", func(t *testing.T) {
		assert := assert.New(t)
		assert.Equal(1, Values{2}.Compare(Values{1}))
		assert.Equal(1, Values{1, 3}.Compare(Values{1, 2}))
	})
}
