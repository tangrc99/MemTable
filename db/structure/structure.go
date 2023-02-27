package structure

type CostCounter interface {
	Cost() int64
}

type Int64 int64

func (Int64) Cost() int64 {
	return 8
}

func (i Int64) Value() int64 {
	return int64(i)
}
