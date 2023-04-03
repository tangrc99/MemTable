package eviction

import (
	"github.com/tangrc99/MemTable/server/global"
	"time"
)

// veteranThreshold 是键值对被移出 RookieList 的访问阈值。
const veteranThreshold = 2

// rookiePeriod 是键值对的保护时间
const rookiePeriod = time.Second * 3

type rookie struct {
	Tp     time.Time // 加入的时间
	Access int       // 访问的次数
}

func newRookie() *rookie {
	return &rookie{
		Tp:     global.Now,
		Access: 1,
	}
}

// RookieList 实现了 inactive 链表，所有新加入的键值对会被放入该结构体中，只有当访问次数超过 veteranThreshold 后才会被移出。
// 当需要进行键值对淘汰时，会优先考虑从 RookieList 中选择加入时间超过 rookiePeriod 的键值对移出，这是为了防止淘汰最新加入的热点数据。
// 使用 LRU 算法时，可能会有一些垃圾缓存加入后不会被访问，我们可以尝试优先删除掉这些缓存。
type RookieList struct {
	rookies map[string]*rookie
}

func NewRookieList() *RookieList {
	return &RookieList{
		rookies: make(map[string]*rookie),
	}
}

// NewOne 将一个新键加入到链表中
func (l *RookieList) NewOne(key string) {
	l.rookies[key] = newRookie()
}

// Hit 记录该键的访问次数，如果被访问次数达到 veteranThreshold 会把键移出。
func (l *RookieList) Hit(key string) {
	if rookie, exist := l.rookies[key]; exist {
		rookie.Access++
		if rookie.Access >= veteranThreshold {
			delete(l.rookies, key)
		}
	}
}

// Candidates 随机返回最有可能会被淘汰的 key，淘汰规则是该键长时间未被访问。该函数并不会直接删除选择的节点，
// 因为选择的节点并不一定全部被淘汰掉。
func (l *RookieList) Candidates(num int) []string {
	victims := make([]string, 0, num)
	max := 20
	for key, rookie := range l.rookies {
		if global.Now.Sub(rookie.Tp) > rookiePeriod {
			victims = append(victims, key)
		}
		if len(victims) == num || max < 0 {
			break
		}
		max--
	}
	return victims
}

// InProtection 用于确定一个键是否正处于保护期内
func (l *RookieList) InProtection(key string) bool {
	if rookie, exist := l.rookies[key]; exist {
		if global.Now.Sub(rookie.Tp) <= rookiePeriod {
			return true
		}
	}
	return false
}

func (l *RookieList) RemoveMany(victims []string) {
	for _, key := range victims {
		delete(l.rookies, key)
	}
}

func (l *RookieList) RemoveOne(key string) {
	delete(l.rookies, key)
}
