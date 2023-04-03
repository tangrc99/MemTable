package db

import "unsafe"

const watcherBasicCost = int64(unsafe.Sizeof(watcher{}))

type watcher struct {
	watches map[string]map[*bool]struct{}
	cost    int64
}

func newWatcher() *watcher {
	return &watcher{
		watches: make(map[string]map[*bool]struct{}),
		cost:    watcherBasicCost,
	}
}

func (w *watcher) Cost() int64 {
	return w.cost
}

func (w *watcher) Size() int {
	return len(w.watches)
}

func (w *watcher) watch(key string, flag *bool) {
	v, ok := w.watches[key]

	if !ok {
		flags := make(map[*bool]struct{})
		flags[flag] = struct{}{}
		w.cost += int64(len(key)) + 16
		w.watches[key] = flags
		return
	}

	v[flag] = struct{}{}
	w.cost += 8
}

func (w *watcher) unwatch(key string, flag *bool) {
	v, ok := w.watches[key]
	if !ok {
		return
	}
	flags := v
	delete(flags, flag)
	w.cost -= 8
	if len(flags) == 0 {
		delete(w.watches, key)
		w.cost -= 8 + int64(len(key))
	}
}

// reviseNotify 通知键修改
func (w *watcher) reviseNotify(key string) {
	v, ok := w.watches[key]
	if !ok {
		return
	}
	flags := v
	for flag := range flags {
		*flag = true
	}
}

// reviseNotifyAll 通知所有被 watch 的键修改
func (w *watcher) reviseNotifyAll() {
	for _, v := range w.watches {
		flags := v
		for flag := range flags {
			*flag = true
		}
	}
}
