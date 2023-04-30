package config

import (
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"reflect"
)

// ReviseEvent 是配置文件的更新事件
type ReviseEvent struct {
	Fields []string // 完成更新的字段
	Err    error    // 发生的错误
}

// Watcher 是对 fsnotify.Watcher 的包装，它将收到的通知二次处理，并重新发送出去
type Watcher struct {
	w      *fsnotify.Watcher
	events chan ReviseEvent
}

func newWatcher(file string) *Watcher {

	if file == "" {
		return &Watcher{
			events: make(chan ReviseEvent),
		}
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	err = watcher.Add(Conf.ConfFile)
	if err != nil {
		panic(err)
	}
	return &Watcher{
		w:      watcher,
		events: make(chan ReviseEvent, 10),
	}
}

// Notification 阻塞地获取配置文件的修改通知。
func (w *Watcher) Notification() <-chan ReviseEvent {
	return w.events
}

var (
	ConfWatcher        *Watcher
	confWatcherEnabled bool
)

func initWatcher() {

	ConfWatcher = newWatcher(Conf.ConfFile)

	if Conf.ConfFile != "" && confWatcherEnabled {

		// 监听 fsnotify 库的通知，并且利用反射来判断配置文件的修改内容
		go func() {
			for {
				select {
				case event, ok := <-ConfWatcher.w.Events:
					if !ok {
						ConfWatcher.events <- ReviseEvent{
							Err: errors.New("config watcher is closed"),
						}
						return
					}
					if event.Has(fsnotify.Write) {

						// 复制旧的配置文件
						cfg := Conf
						// 解析新配置文件
						if err := cfg.parseFile(); err != nil {
							ConfWatcher.events <- ReviseEvent{
								Err: err,
							}
							continue
						}

						newVal := reflect.ValueOf(cfg)
						oldVal := reflect.ValueOf(Conf)
						t := reflect.TypeOf(cfg)
						updatedFields := make([]string, 0)

						// 根据反射计算哪些字段进行了更改
						for i := 0; i < newVal.NumField(); i++ {
							equal := true
							switch newVal.Field(i).Kind() {
							case reflect.String:
								if newVal.Field(i).String() != oldVal.Field(i).String() {
									equal = false
								}
							case reflect.Int, reflect.Int64:
								if newVal.Field(i).Int() != oldVal.Field(i).Int() {
									equal = false
								}
							case reflect.Bool:
								if newVal.Field(i).Bool() != oldVal.Field(i).Bool() {
									equal = false
								}
							case reflect.Uint, reflect.Uint64:
								if newVal.Field(i).Uint() != oldVal.Field(i).Uint() {
									equal = false
								}
							default:
								panic(fmt.Sprintf("unexpected type %s: %d", t.Field(i).Name, newVal.Field(i).Kind()))
							}
							if !equal {
								updatedFields = append(updatedFields, t.Field(i).Name)
							}
						}

						// 拷贝解析后的配置
						Conf = cfg

						// 如果有更新，则发出通知
						if len(updatedFields) > 0 {
							ConfWatcher.events <- ReviseEvent{
								Fields: updatedFields,
							}
						}
					}

				case err, ok := <-ConfWatcher.w.Errors:
					if !ok {
						ConfWatcher.events <- ReviseEvent{
							Err: errors.New("config watcher is closed"),
						}
						return
					}
					ConfWatcher.events <- ReviseEvent{
						Err: err,
					}
				}
			}
		}()
	}
}
