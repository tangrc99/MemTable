package client

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClientFlag(t *testing.T) {

	cli := NewClient()
	cli.toInTx()
	assert.Equal(t, 0x101, cli.flag)

	cli.toNotInTx()
	assert.Equal(t, 0x1, cli.flag)

	cli.toDisconnected()
	assert.Equal(t, 0x0, cli.flag)

}
