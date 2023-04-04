package structure

// Object 是数据库中存储的基本类型
type Object interface {
	// Cost 会返回当前元素所占用的内存大小
	Cost() int64
}

// Int64 是对 int64 的封装
type Int64 int64

func (Int64) Cost() int64 {
	return 8
}

func (i Int64) Value() int64 {
	return int64(i)
}

// Nil 是对 struct{} 的封装
type Nil struct{}

func (e Nil) Cost() int64 {
	return 0
}

// Float32 是对 float32 的封装
type Float32 float32

func (Float32) Cost() int64 {
	return 4
}

func (i Float32) Value() float32 {
	return float32(i)
}

// Slice 是对 []byte 的封装
type Slice []byte

func (s Slice) Cost() int64 {
	return int64(len(s))
}

// String 是对 string 的封装
type String string

func (s String) Cost() int64 {
	return int64(len(s))
}
