package db

type Watcher struct {
	watches map[string]map[*bool]struct{}
}

func (w *Watcher) Cost() int64 {

	//TODO:

	return -1
}

func (w *Watcher) watch(key string, flag *bool) {
	v, ok := w.watches[key]

	if !ok {
		flags := make(map[*bool]struct{})
		flags[flag] = struct{}{}
		w.watches[key] = flags
		return
	}

	v[flag] = struct{}{}
}

func (w *Watcher) unwatch(key string, flag *bool) {

}

// reviseNotify 通知键修改
func (w *Watcher) reviseNotify(key string) {
	//v, ok := db_.watches.Get(key)
	//if !ok {
	//	return
	//}
	//flags := v.(*map[*bool]struct{})
	//
	//for flag := range *flags {
	//	*flag = true
	//}
}

// reviseNotifyAll 通知所有被 watch 的键修改
func (w *Watcher) reviseNotifyAll() {

	//dicts, _ := db_.watches.GetAll()
	//
	//for _, dict := range *dicts {
	//	for _, v := range dict {
	//		flags := v.(*map[*bool]struct{})
	//		for flag := range *flags {
	//			*flag = true
	//		}
	//	}
	//}
}
