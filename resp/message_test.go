package resp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringData(t *testing.T) {

	data := MakeStringData("123")

	assert.Equal(t, []byte("123"), data.ByteData())
	assert.Equal(t, []byte("+123\r\n"), data.ToBytes())
	assert.Equal(t, "123", data.Data())
}

func TestErrorData(t *testing.T) {

	data := MakeErrorData("123")

	assert.Equal(t, []byte("123"), data.ByteData())
	assert.Equal(t, []byte("-123\r\n"), data.ToBytes())
	assert.Equal(t, "123", data.Error())
}

func TestBulkData(t *testing.T) {

	data := MakeBulkData([]byte("123"))

	assert.Equal(t, []byte("123"), data.ByteData())
	assert.Equal(t, []byte("$3\r\n123\r\n"), data.ToBytes())
	assert.Equal(t, []byte("123"), data.Data())
}

func TestIntData(t *testing.T) {

	data := MakeIntData(123)

	assert.Equal(t, []byte("123"), data.ByteData())
	assert.Equal(t, []byte(":123\r\n"), data.ToBytes())
	assert.Equal(t, int64(123), data.Data())
}

func TestArrayData(t *testing.T) {

	data1 := MakeStringData("123")
	data2 := MakeErrorData("123")
	data3 := MakeBulkData([]byte("123"))
	data4 := MakeIntData(123)

	data := MakeArrayData([]RedisData{data1, data2, data3, data4})

	assert.Equal(t, []byte("123123123123"), data.ByteData())
	assert.Equal(t, []byte("*4\r\n+123\r\n-123\r\n$3\r\n123\r\n:123\r\n"), data.ToBytes())
	assert.Equal(t, []RedisData{data1, data2, data3, data4}, data.Data())
	assert.Equal(t, [][]byte{[]byte("123"), []byte("123"), []byte("123"), []byte("123")}, data.ToCommand())

	edata := MakeEmptyArrayData()
	assert.Equal(t, []byte{}, edata.ByteData())
	assert.Equal(t, []byte{'*', '0', '\r', '\n'}, edata.ToBytes())
	assert.Equal(t, []RedisData{}, edata.Data())
	assert.Equal(t, [][]byte{}, edata.ToCommand())
}

func TestPlainData(t *testing.T) {

	data := MakePlainData("set key value")

	assert.Equal(t, []byte("set key value"), data.ByteData())
	assert.Equal(t, []byte("set key value\r\n"), data.ToBytes())
	assert.Equal(t, "set key value", data.Data())

	data1 := data.ToArray()
	assert.IsType(t, &ArrayData{}, data1)
	assert.Equal(t, []byte("setkeyvalue"), data1.ByteData())
	assert.Equal(t, []byte("*3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"), data1.ToBytes())
}

func TestPlainData2(t *testing.T) {

	data := MakePlainData("set key value")

	rdata := PlainDataToResp(data.ToCommand())
	assert.IsType(t, &ArrayData{}, rdata)

	assert.Equal(t, data.ToCommand(), rdata.(*ArrayData).ToCommand())
}

func TestToReadableString(t *testing.T) {
	sdata := MakeStringData("123")
	edata := MakeErrorData("123")
	bdata := MakeBulkData([]byte("123"))
	idata := MakeIntData(123)
	pdata := MakePlainData("set key value")

	adata := MakeArrayData([]RedisData{sdata, edata, bdata, idata})
	adata2 := MakeArrayData([]RedisData{adata})
	fmt.Printf("%s\n", ToReadableString(sdata, ""))
	fmt.Printf("%s\n", ToReadableString(edata, ""))
	fmt.Printf("%s\n", ToReadableString(bdata, ""))
	fmt.Printf("%s\n", ToReadableString(idata, ""))
	fmt.Printf("%s\n", ToReadableString(pdata, ""))
	fmt.Printf("%s\n", ToReadableString(adata, ""))
	fmt.Printf("%s\n", ToReadableString(adata2, ""))

}
