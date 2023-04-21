package resp

import (
	"fmt"
	"reflect"
	"strings"
)

// PlainDataToResp 将 redis-pipeline 类型数据转化为 RESP 类型数据
func PlainDataToResp(data [][]byte) RedisData {
	lines := make([]RedisData, len(data))

	for i := range data {
		lines[i] = MakeBulkData(data[i])
	}

	return MakeArrayData(lines)
}

func ToReadableString(data RedisData, prefix string) string {
	t := reflect.TypeOf(data)
	switch t.String() {
	case "*resp.StringData":
		return data.(*StringData).Data()
	case "*resp.ErrorData":
		return "(error) " + data.(*ErrorData).Error()
	case "*resp.IntData":
		return fmt.Sprintf("(integer) %d", data.(*IntData).Data())
	case "*resp.BulkData":
		d := data.(*BulkData).Data()
		if len(d) == 0 {
			return "(nil)"
		}
		return fmt.Sprintf("\"%s\"", d)
	case "*resp.ArrayData":
		d := data.(*ArrayData)
		p := 0
		for i := len(d.data); i != 0; i /= 10 {
			p++
		}
		ret := ""
		for i := range d.data {
			if i == 0 {
				ret += fmt.Sprintf("%d) %s\n", i, ToReadableString(d.data[i], prefix+strings.Repeat(" ", p+2)))
			} else {
				ret += fmt.Sprintf("%s%d) %s\n", prefix, i, ToReadableString(d.data[i], prefix+strings.Repeat(" ", p+2)))
			}
		}
		return ret[:len(ret)-1]
	case "*resp.PlainData":
		return data.(*PlainData).Data()
	}
	return ""
}
