package resp

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/tangrc99/MemTable/logger"
	"io"
	"reflect"
	"strconv"
)

// resp package for parsing redis serialization protocol.
// Check https://redis.io/docs/reference/protocol-spec/ for the protocol details.

type ParsedRes struct {
	Data  RedisData
	Err   error
	Abort bool // 解析中发生无法恢复的错误
}

type readState struct {
	bulkLen   int64
	arrayLen  int
	multiLine bool
	arrayData *ArrayData
	inArray   bool
}

type Parser struct {
	bufReader *bufio.Reader
	state     *readState
	exit      bool
}

func NewParser(reader io.Reader) *Parser {
	return &Parser{
		bufReader: bufio.NewReader(reader),
		state:     new(readState),
	}
}

// Stop 并不会直接终止解析，而是需要手动关闭连接
func (parser *Parser) Stop() {
	parser.exit = true
}

// Parse 将会阻塞地读取数据流，并且尝试解析出 RESP 包
func (parser *Parser) Parse() *ParsedRes {

	for {
		var res RedisData
		var err error
		var msg []byte
		msg, err = readLine(parser.bufReader, parser.state)

		if parser.exit {
			// 返回空消息
			return &ParsedRes{
				Data:  nil,
				Err:   nil,
				Abort: true,
			}
		}

		if err != nil {

			// read ended, stop reading.
			if err == io.EOF {
				return &ParsedRes{
					Err: err,
				}

			} else if reflect.TypeOf(err).PkgPath() == "crypto/tls" {
				// 如果是 tls 报错，不应该继续读取
				logger.Error(err)
				*parser.state = readState{}

				return &ParsedRes{
					Err:   err,
					Abort: true,
				}

			} else {

				// Protocol error
				logger.Error(err)
				*parser.state = readState{}

				return &ParsedRes{
					Err: err,
				}
			}
		}
		// parse the read messages
		// if msg is an array or a bulk string, then parse their header first.
		// if msg is a normal line, parse it directly.
		if !parser.state.multiLine {
			// parse single line: no bulk string

			if msg[0] == '*' {
				err := parseArrayHeader(msg, parser.state)
				if err != nil {
					logger.Error(err)
					*parser.state = readState{}

					return &ParsedRes{
						Err: err,
					}
				} else {
					if parser.state.arrayLen == -1 {
						// null array
						*parser.state = readState{}
						return &ParsedRes{
							Data: MakeArrayData(nil),
						}
					} else if parser.state.arrayLen == 0 {
						// empty array
						*parser.state = readState{}
						return &ParsedRes{
							Data: MakeArrayData([]RedisData{}),
						}
					}
				}
				continue
			}

			if msg[0] == '$' {
				err := parseBulkHeader(msg, parser.state)
				if err != nil {
					logger.Error(err)
					*parser.state = readState{}

					return &ParsedRes{
						Err: err,
					}
				} else {
					if parser.state.bulkLen == -1 {
						// null bulk string
						parser.state.multiLine = false
						parser.state.bulkLen = 0
						res = MakeBulkData(nil)
						if parser.state.inArray {
							parser.state.arrayData.data = append(parser.state.arrayData.data, res)
							if len(parser.state.arrayData.data) == parser.state.arrayLen {

								return &ParsedRes{
									Data: parser.state.arrayData,
									Err:  nil,
								}
							}
						} else {
							return &ParsedRes{
								Data: res,
							}
						}
					}
				}
				continue
			}

			res, err = parseSingleLine(msg)
		} else {
			// parse multiple lines: bulk string (binary safe)
			parser.state.multiLine = false
			parser.state.bulkLen = 0
			res, err = parseMultiLine(msg)
		}

		if err != nil {
			logger.Error(err)
			*parser.state = readState{}

			return &ParsedRes{
				Err: err,
			}

		}

		// Struct parsed data as an array or a single data, and put it into channel.
		if parser.state.inArray {
			parser.state.arrayData.data = append(parser.state.arrayData.data, res)
			if len(parser.state.arrayData.data) == parser.state.arrayLen {
				return &ParsedRes{
					Data: parser.state.arrayData,
					Err:  nil,
				}
			}
		} else {
			return &ParsedRes{
				Data: res,
				Err:  err,
			}
		}
	}

	// 空消息
	//return &ParsedRes{}
}

// Read a line or bulk line end of "\r\n" from a reader.
// Return:
//
//	[]byte: read bytes.
//	error: io.EOF or Protocol error
func readLine(reader *bufio.Reader, state *readState) ([]byte, error) {
	var msg []byte
	var err error
	if state.multiLine && state.bulkLen >= 0 {
		// read bulk line, binary safety.
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(reader, msg)
		if err != nil {
			return nil, err
		}
		state.bulkLen = 0
		if msg[len(msg)-1] != '\n' || msg[len(msg)-2] != '\r' {
			return nil, errors.New(fmt.Sprintf("Protocol error. Stream message %s is invalid.", string(msg)))
		}
	} else {
		// read normal line
		msg, err = reader.ReadBytes('\n')
		if err != nil {
			return msg, err
		}

		if len(msg) < 2 || msg[len(msg)-2] != '\r' {
			return nil, errors.New(fmt.Sprintf("Protocol error. Stream message %s is invalid.", string(msg)))
		}
	}
	return msg, nil
}

func parseSingleLine(msg []byte) (RedisData, error) {
	// discard "\r\n"
	msgType := msg[0]
	msgData := string(msg[1 : len(msg)-2])
	var res RedisData

	switch msgType {
	case '+':
		// simple string
		res = MakeStringData(msgData)
	case '-':
		// error
		res = MakeErrorData(msgData)
	case ':':
		//    integer
		data, err := strconv.ParseInt(msgData, 10, 64)
		if err != nil {
			logger.Error("Protocol error: " + string(msg))
			return nil, err
		}
		res = MakeIntData(data)
	default:
		// plain string
		res = MakePlainData(string(msg[0 : len(msg)-2]))
	}
	if res == nil {
		logger.Error("Protocol error: parseSingleLine get nil data")
		return nil, errors.New("Protocol error: " + string(msg))
	}
	return res, nil
}

func parseMultiLine(msg []byte) (RedisData, error) {
	// discard "\r\n"
	if len(msg) < 2 {
		return nil, errors.New("protocol error: invalid bulk string")
	}
	msgData := msg[:len(msg)-2]
	res := MakeBulkData(msgData)
	return res, nil
}

func parseArrayHeader(msg []byte, state *readState) error {
	arrayLen, err := strconv.Atoi(string(msg[1 : len(msg)-2]))
	if err != nil || arrayLen < -1 {
		return errors.New("Protocol error: " + string(msg))
	}
	state.arrayLen = arrayLen
	state.inArray = true
	state.arrayData = MakeArrayData([]RedisData{})
	return nil
}

func parseBulkHeader(msg []byte, state *readState) error {
	bulkLen, err := strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil || bulkLen < -1 {
		return errors.New("Protocol error: " + string(msg))
	}
	state.bulkLen = bulkLen
	state.multiLine = true
	return nil
}
