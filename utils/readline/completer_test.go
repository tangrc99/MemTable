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
	c.Register(&Hint{"12356", ""})

	words := c.Query("12")

	assert.Subset(t, []string{"122", "123", "1234", "12356"}, words)
	assert.Equal(t, 4, len(words))
}
