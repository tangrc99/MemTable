package structure

type String []byte

func (s String) Cost() int64 {
	return int64(len(s))
}
