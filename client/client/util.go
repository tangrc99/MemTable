package client

import "fmt"

func (c *Client) sendAuthMessage() (string, error) {

	if c.user == "" && c.password == "" {
		return "", nil
	}

	var cmd string
	if c.user != "" {
		cmd = fmt.Sprintf("*3\r\n"+
			"$4\r\n"+
			"auth\r\n"+
			"$%d\r\n"+
			"%s\r\n"+
			"$%d\r\n"+
			"%s\r\n", len(c.user), c.user, len(c.password), c.password)
	} else {
		cmd = fmt.Sprintf("*2\r\n"+
			"$4\r\n"+
			"auth\r\n"+
			"$%d\r\n"+
			"%s\r\n", len(c.password), c.password)
	}

	return c.Call([]byte(cmd))
}
