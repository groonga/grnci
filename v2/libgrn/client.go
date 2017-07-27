package libgrn

import (
	"io"

	"github.com/groonga/grnci/v2"
)

const (
	defaultMaxIdleConns = 2 // Maximum number of idle connections
)

// ClientOptions is options of Client.
type ClientOptions struct {
	MaxIdleConns int // Maximum number of idle connections
}

// NewClientOptions returns the default ClientOptions.
func NewClientOptions() *ClientOptions {
	return &ClientOptions{
		MaxIdleConns: defaultMaxIdleConns,
	}
}

// Client is a thread-safe GQTP client or DB handle.
type Client struct {
	addr      string
	baseConn  *conn
	idleConns chan *conn
}

// Dial returns a new Client connected to a GQTP server.
// The expected address format is [scheme://][host][:port].
func Dial(addr string, options *ClientOptions) (*Client, error) {
	if options == nil {
		options = NewClientOptions()
	}
	cn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr:      addr,
		idleConns: make(chan *conn, options.MaxIdleConns),
	}
	c.idleConns <- cn
	cn.client = c
	return c, nil
}

// Open opens an existing DB and returns a new Client.
func Open(path string, options *ClientOptions) (*Client, error) {
	if options == nil {
		options = NewClientOptions()
	}
	cn, err := open(path)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseConn:  cn,
		idleConns: make(chan *conn, options.MaxIdleConns),
	}, nil
}

// Create creates a new DB and returns a new Client.
func Create(path string, options *ClientOptions) (*Client, error) {
	if options == nil {
		options = NewClientOptions()
	}
	cn, err := create(path)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseConn:  cn,
		idleConns: make(chan *conn, options.MaxIdleConns),
	}, nil
}

// Close closes the idle connections.
// Close should be called after all responses are closed.
// Otherwise, connections will be leaked.
func (c *Client) Close() error {
	var err error
Loop:
	for {
		select {
		case conn := <-c.idleConns:
			if e := conn.Close(); e != nil && err == nil {
				err = e
			}
		default:
			break Loop
		}
	}
	if c.baseConn != nil {
		if e := c.baseConn.Close(); e != nil {
			err = e
		}
	}
	return err
}

// exec sends a command and receives a response.
func (c *Client) exec(cmd string, body io.Reader) (grnci.Response, error) {
	var conn *conn
	var err error
	select {
	case conn = <-c.idleConns:
	default:
		if c.baseConn == nil {
			conn, err = dial(c.addr)
			if err != nil {
				return nil, err
			}
			conn.client = c
		} else {
			conn, err = c.baseConn.Dup()
			if err != nil {
				return nil, err
			}
			conn.client = c
		}
	}
	resp, err := conn.Exec(cmd, body)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return resp, nil
}

// Exec parses cmd, reassembles it and calls Query.
func (c *Client) Exec(cmd string, body io.Reader) (grnci.Response, error) {
	command, err := grnci.ParseCommand(cmd)
	if err != nil {
		return nil, err
	}
	command.SetBody(body)
	return c.Query(command)
}

// Invoke assembles name, params and body into a command and calls Query.
func (c *Client) Invoke(name string, params map[string]interface{}, body io.Reader) (grnci.Response, error) {
	cmd, err := grnci.NewCommand(name, params)
	if err != nil {
		return nil, err
	}
	cmd.SetBody(body)
	return c.Query(cmd)
}

// Query sends a command and receives a response.
// It is the caller's responsibility to close the response.
func (c *Client) Query(cmd *grnci.Command) (grnci.Response, error) {
	if err := cmd.Check(); err != nil {
		return nil, err
	}
	return c.exec(cmd.String(), cmd.Body())
}
