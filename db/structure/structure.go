package structure

// Object 是数据库中存储的基本类型
type Object interface {
	// Cost 会返回当前元素所占用的内存大小
	Cost() int64
}

type Int64 int64

func (Int64) Cost() int64 {
	return 8
}

func (i Int64) Value() int64 {
	return int64(i)
}

type Nil struct{}

func (e Nil) Cost() int64 {
	return 0
}

type Float32 float32

func (Float32) Cost() int64 {
	return 4
}
func (i Float32) Value() float32 {
	return float32(i)
}

type Slice []byte

func (s Slice) Cost() int64 {
	return int64(len(s))
}

type String string

func (s String) Cost() int64 {
	return int64(len(s))
}
