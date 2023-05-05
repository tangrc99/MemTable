package ring_buffer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingBuffer(t *testing.T) {
	r := RingBuffer{}
	r.Init(5)

	if r.capacity != 8 {
		t.Error("Init Failed")
	}

	assert.Equal(t, uint64(0), r.LowWaterLevel())

	c1 := []byte{'1', '2', '3'}
	r.Append(c1)
	r.Append([]byte{'4', '5', '6'})
	r.Append([]byte{'7', '8', '9'})

	if r.LowWaterLevel() != 1 {
		t.Error("Mod Failed")
	}

	if string(r.Read(r.LowWaterLevel(), 100)) != "23456789" {
		t.Error("Mod Failed")
	}

	r.offset = 1<<64 - 1
	r.Append(c1)

	assert.Equal(t, uint64(0), r.LowWaterLevel())
	assert.Equal(t, uint64(2), r.HighWaterLevel())
}

func TestRingBufferLargerThanCap(t *testing.T) {
	r := RingBuffer{}
	r.Init(5)

	assert.Equal(t, uint64(8), r.capacity)

	r.Append([]byte("1111"))
	r.Append([]byte("222222222"))

	assert.Equal(t, uint64(16), r.capacity)

	bytes := r.ReadSince(r.LowWaterLevel())

	assert.Equal(t, []byte("1111222222222"), bytes)

	r.Append([]byte("333"))

	bytes = r.ReadSince(r.LowWaterLevel())
	assert.Equal(t, []byte("1111222222222333"), bytes)

}
