package eviction

import (
	"github.com/tangrc99/MemTable/db/structure"
	"github.com/tangrc99/MemTable/utils"
)

// tinyLFU is an admission helper that keeps track of access frequency using
// tiny (4-bit) counters in the form of a count-min sketch.
// tinyLFU is NOT thread safe.
type tinyLFU struct {
	freq    *cmSketch
	door    *structure.Bloom // 布隆过滤器
	incrs   int64
	resetAt int64

	Eviction
}

func newTinyLFU(numCounters int64) *tinyLFU {
	return &tinyLFU{
		freq:    newCmSketch(numCounters),
		door:    structure.NewBloomFilter(float64(numCounters), 0.01),
		resetAt: numCounters,
	}
}

func (p *tinyLFU) Permitted(key string) bool {
	hashVal := utils.MemHashString(key)
	return p.door.Has(hashVal)
}

func (p *tinyLFU) KeyUsed(key string, item *Item) {

	hashVal := utils.MemHashString(key)

	// Flip doorkeeper bit if not already done.
	if added := p.door.AddIfNotHas(hashVal); !added {
		// Increment count-min counter if doorkeeper bit is already set.
		p.freq.Increment(hashVal)
	}
	p.incrs++ // 放入的键值对数量
	if p.incrs >= p.resetAt {
		// Zero out incrs.
		p.incrs = 0
		// clears doorkeeper bits
		p.door.Clear()
		// halves count-min counters
		p.freq.Reset()
	}
}

// Estimate 估算 key 的命中值
func (p *tinyLFU) Estimate(key string) int64 {
	hashVal := utils.MemHashString(key)

	hits := p.freq.Estimate(hashVal)
	if p.door.Has(hashVal) {
		hits++
	}
	return hits
}

func (p *tinyLFU) Clear() {
	p.incrs = 0
	p.door.Clear()
	p.freq.Clear()
}
