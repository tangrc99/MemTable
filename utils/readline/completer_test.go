package readline

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCompleter(t *testing.T) {

	c := NewCompleter()

	c.Register(&Hint{"123", ""})
	c.Register(&Hint{"122", ""})
	c.Register(&Hint{"1234", ""})
	c.Register(&Hint{"12356", "helper"})

	words := c.Query("12")

	assert.Subset(t, []string{"122", "123", "1234", "12356"}, words)
	assert.Equal(t, 4, len(words))

	assert.True(t, c.Exist("12356"))
	helper, ok := c.GetHelper("12356")
	assert.True(t, ok)
	assert.Equal(t, "helper", helper)

	helper, ok = c.GetHelper("1")

	assert.False(t, ok)
	assert.Equal(t, "", helper)
}
