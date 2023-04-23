package db

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestWatcherNotify(t *testing.T) {

	w := newWatcher()

	notifier1, notifier2 := false, false
	w.watch("123", &notifier1)
	w.watch("234", &notifier2)
	w.reviseNotify("123")
	assert.True(t, notifier1)
	assert.False(t, notifier2)
}

func TestWatcherNotify2(t *testing.T) {

	w := newWatcher()

	notifier1, notifier2 := false, false
	w.watch("123", &notifier1)
	w.watch("123", &notifier2)
	w.reviseNotify("123")
	assert.True(t, notifier1)
	assert.True(t, notifier2)
}

func TestWatcherNotifyAll(t *testing.T) {

	w := newWatcher()

	notifier1, notifier2 := false, false
	w.watch("123", &notifier1)
	w.watch("234", &notifier2)
	w.reviseNotifyAll()
	assert.True(t, notifier1)
	assert.True(t, notifier2)

}

func TestWatcherUnwatch(t *testing.T) {

	w := newWatcher()

	notifier := false

	w.watch("123", &notifier)
	w.unwatch("123", &notifier)
	w.reviseNotifyAll()
	assert.False(t, notifier)
}

func TestWatcherWatchMany(t *testing.T) {

	w := newWatcher()

	notifier := false

	w.watch("123", &notifier)
	w.watch("234", &notifier)

	w.reviseNotify("234")
	assert.True(t, notifier)
}

func TestWatcherCost(t *testing.T) {

	w := newWatcher()

	assert.Equal(t, watcherBasicCost, w.Cost())

	notifier1, notifier2 := false, false

	w.watch("123", &notifier1)
	assert.Equal(t, int64(35), w.Cost())

	w.watch("234", &notifier1)
	assert.Equal(t, int64(54), w.Cost())

	w.watch("234", &notifier2)
	assert.Equal(t, int64(62), w.Cost())

}
