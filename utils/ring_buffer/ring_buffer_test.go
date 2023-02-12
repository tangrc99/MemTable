package ring_buffer

import "testing"

func TestRingBuffer(t *testing.T) {
	r := RingBuffer{}
	r.Init(5)

	if r.capacity != 8 {
		t.Error("Init Failed")
	}

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

	if !r.ringed || r.LowWaterLevel()+r.capacity != uint64(2) || r.HighWaterLevel() != 2 {
		t.Error("OverFlow Failed")
	}
}
