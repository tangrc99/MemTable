package db

import (
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/tangrc99/MemTable/server/global"
	"testing"
)

func TestConsumerMap(t *testing.T) {

	c := newBlockMap()

	// basic
	notifier1 := make(chan []byte, 1)
	notifier2 := make(chan []byte, 1)

	c.register("123", uuid.Must(uuid.NewV1()), notifier1, -1)
	c.tryConsume("123", []byte("1"))
	assert.Equal(t, []byte("1"), <-notifier1)

	assert.False(t, c.tryConsume("123", []byte("2")))

	// seq
	c.register("123", uuid.Must(uuid.NewV1()), notifier1, -1)
	c.register("123", uuid.Must(uuid.NewV1()), notifier2, -1)
	c.tryConsume("123", []byte("3"))
	c.tryConsume("123", []byte("4"))
	assert.Equal(t, []byte("3"), <-notifier1)
	assert.Equal(t, []byte("4"), <-notifier2)

	// unregister
	id := uuid.Must(uuid.NewV1())
	c.register("123", uuid.Must(uuid.NewV1()), notifier1, -1)
	c.unregister("123", id)

}

// TestConsumerMapRepeatable 多次注册在同一个键上，应该覆盖之前的值
func TestConsumerMapRepeatable(t *testing.T) {
	c := newBlockMap()

	id := uuid.Must(uuid.NewV1())
	notifier1 := make(chan []byte, 1)
	notifier2 := make(chan []byte, 1)
	c.register("123", id, notifier1, -1)
	c.register("123", id, notifier2, -1)
	assert.Equal(t, 1, c.consumers["123"].Size())

	c.tryConsume("123", []byte("1"))

	assert.Nil(t, c.consumers["123"])
	assert.Equal(t, []byte("1"), <-notifier2)
}

func TestConsumerMapDeadLine(t *testing.T) {
	c := newBlockMap()

	id := uuid.Must(uuid.NewV1())
	notifier1 := make(chan []byte, 1)
	c.register("123", id, notifier1, 1)

	assert.False(t, c.tryConsume("123", []byte("1")))

	c.register("123", id, notifier1, global.Now.Unix()+1)
	assert.True(t, c.tryConsume("123", []byte("2")))

}

func TestConsumerMapDeadLine2(t *testing.T) {
	c := newBlockMap()

	notifier1 := make(chan []byte, 1)
	notifier2 := make(chan []byte, 1)
	notifier3 := make(chan []byte, 1)
	notifier4 := make(chan []byte, 1)

	c.register("123", uuid.Must(uuid.NewV1()), notifier1, 1)
	c.register("123", uuid.Must(uuid.NewV1()), notifier2, 1)
	c.register("123", uuid.Must(uuid.NewV1()), notifier3, -1)
	c.register("123", uuid.Must(uuid.NewV1()), notifier4, global.Now.Unix()+1)

	assert.True(t, c.tryConsume("123", []byte("1")))
	assert.True(t, c.tryConsume("123", []byte("2")))
	assert.False(t, c.tryConsume("123", []byte("3")))

	assert.Equal(t, []byte("1"), <-notifier3)
	assert.Equal(t, []byte("2"), <-notifier4)

}
