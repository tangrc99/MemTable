package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestChannel(t *testing.T) {

	ch := newChannel()
	receiver := make(chan []byte, 1)
	ch.subscribe("u1", &receiver)
	assert.Equal(t, 1, ch.publish([]byte("msg")))
	assert.Equal(t, []byte("msg"), <-receiver)

	ch.unSubscribe("u1")
	assert.Equal(t, 0, ch.publish([]byte("msg")))

}

func TestChannelsSingle(t *testing.T) {

	ch := NewChannels()
	receiver := make(chan []byte, 1)

	ch.Subscribe("ch1", "u1", &receiver)
	assert.Equal(t, 1, ch.Publish("ch1", []byte("msg")))
	assert.Equal(t, []byte("msg"), <-receiver)

	ch.UnSubscribe("ch1", "u1")
	assert.Equal(t, 0, ch.Publish("ch1", []byte("msg")))

}

func TestChannelsPath(t *testing.T) {

	ch := NewChannels()
	receiver := make(chan []byte, 1)

	ch.Subscribe("/a/b", "u1", &receiver)

	assert.Equal(t, 1, ch.Publish("/a/b", []byte("msg")))
	assert.Equal(t, []byte("msg"), <-receiver)

	assert.Equal(t, 1, ch.Publish("/a", []byte("msg")))
	assert.Equal(t, []byte("msg"), <-receiver)

	ch.UnSubscribe("/a/b", "u1")
	assert.Equal(t, 0, ch.Publish("/a", []byte("msg")))

}
