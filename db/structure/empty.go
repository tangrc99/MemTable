package structure

type Empty struct{}

func (e Empty) Cost() int64 {
	return 0
}
