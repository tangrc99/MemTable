package resp

func PlainDataToResp(data [][]byte) RedisData {
	lines := make([]RedisData, len(data))

	for i := range data {
		lines[i] = MakeBulkData(data[i])
	}

	return MakeArrayData(lines)
}
