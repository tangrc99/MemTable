package resp

// PlainDataToResp 将 redis-pipeline 类型数据转化为 RESP 类型数据
func PlainDataToResp(data [][]byte) RedisData {
	lines := make([]RedisData, len(data))

	for i := range data {
		lines[i] = MakeBulkData(data[i])
	}

	return MakeArrayData(lines)
}
