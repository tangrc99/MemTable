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

func WithUser(user string) Option {
	return func(c *Client) {
		c.user = user
	}
}

func WithPassword(password string) Option {
	return func(c *Client) {
		c.password = password
	}
}

func WithDatabase(number int) Option {
	return func(c *Client) {
		c.db = number
	}
}
