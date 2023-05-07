package client

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const pingMsg = "*1\r\n$4\r\nping\r\n"
const pongMsg = "pong"

type DelayResult struct {
	minDelay int     // 测试中的最小延迟
	maxDelay int     // 测试中的最大延迟
	avgDelay float32 // 测试中的平均延迟
	count    int     // 完成的测试次数
	err      error   // 发生错误的原因
}

func (r *DelayResult) Print() {

	if r.count > 0 {
		fmt.Printf("min: %d, max: %d, avg %.2f, samples: %d\n", r.minDelay, r.maxDelay, r.avgDelay, r.count)
	}
	if r.err != nil {
		fmt.Printf("(error) %s\n", r.err.Error())
	}
}

// testDelaySingle 会测试客户端到服务器之间的延迟，延迟会以 ping 命令来测算
func testDelaySingle(c *Client) (int, error) {

	// 确保客户端正在连接中
	if !c.isConnected() {
		_ = c.Dial()
	}

	start := time.Now()

	echo, err := c.Call([]byte(pingMsg))
	if err != nil {
		return -1, errors.New("server disconnected")
	}

	duration := time.Since(start).Milliseconds()

	if strings.ToLower(echo) != pongMsg {
		return -1, errors.New(fmt.Sprintf("server returns wrong msg '%s'", echo))
	}

	return int(duration), nil
}

func calMinMaxAvg(array []int) (min int, max int, avg float32) {

	max = math.MinInt
	min = math.MaxInt
	sum := 0

	for _, e := range array {
		if e < min {
			min = e
		}
		if e > max {
			max = e
		}
		sum += e
	}
	return min, max, float32(sum) / float32(len(array))
}

// TestDelayByCount 最多进行 count 次测试，如果测试中遇到异常会中断测试，返回测试结果
func TestDelayByCount(c *Client, count int) DelayResult {

	tests := make([]int, 0, count)

	for i := 0; i < count; i++ {
		delay, err := testDelaySingle(c)
		if err != nil {
			min, max, avg := calMinMaxAvg(tests)
			return DelayResult{
				minDelay: min,
				maxDelay: max,
				avgDelay: avg,
				count:    i,
				err:      err,
			}
		}
		tests = append(tests, delay)
	}
	min, max, avg := calMinMaxAvg(tests)
	return DelayResult{
		minDelay: min,
		maxDelay: max,
		avgDelay: avg,
		count:    count,
		err:      nil,
	}
}

// TestDelayByInterval 最多进行 count 次测试，如果测试中遇到异常会中断测试，返回测试结果
func TestDelayByInterval(c *Client, ms int64) DelayResult {

	tests := make([]int, 0)
	for start := time.Now(); time.Since(start).Milliseconds() < ms*time.Millisecond.Milliseconds(); {
		delay, err := testDelaySingle(c)
		if err != nil {
			min, max, avg := calMinMaxAvg(tests)
			return DelayResult{
				minDelay: min,
				maxDelay: max,
				avgDelay: avg,
				count:    len(tests),
				err:      err,
			}
		}
		tests = append(tests, delay)
	}
	min, max, avg := calMinMaxAvg(tests)
	return DelayResult{
		minDelay: min,
		maxDelay: max,
		avgDelay: avg,
		count:    len(tests),
		err:      nil,
	}
}
