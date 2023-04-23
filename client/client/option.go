package client

type Option func(*Client)

func WithHost(host string) Option {
	return func(c *Client) {
		c.host = host
	}
}

func WithPort(port int) Option {
	return func(c *Client) {
		c.port = port
	}
}
