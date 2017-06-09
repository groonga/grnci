package libgrn

import (
	"io"

	"github.com/groonga/grnci/v2"
)

const (
	maxIdleConns = 2 // Maximum number of idle connections
)

// Client is a thread-safe GQTP client or DB handle.
type Client struct {
	addr      *grnci.Address
	baseConn  *Conn
	idleConns chan *Conn
}

// DialClient returns a new Client connected to a GQTP server.
// The expected address format is [scheme://][host][:port].
func DialClient(addr string) (*Client, error) {
	a, err := grnci.ParseGQTPAddress(addr)
	if err != nil {
		return nil, err
	}
	conn, err := Dial(addr)
	if err != nil {
		return nil, err
	}
	conns := make(chan *Conn, maxIdleConns)
	conns <- conn
	return &Client{
		addr:      a,
		idleConns: conns,
	}, nil
}

// OpenClient opens an existing DB and returns a new Client.
func OpenClient(path string) (*Client, error) {
	conn, err := Open(path)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseConn: conn,
	}, nil
}

// CreateClient creates a new DB and returns a new Client.
func CreateClient(path string) (*Client, error) {
	conn, err := Create(path)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseConn: conn,
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

// Exec sends a request and receives a response.
// It is the caller's responsibility to close the response.
func (c *Client) Exec(cmd string, body io.Reader) (grnci.Response, error) {
	var conn *Conn
	var err error
	select {
	case conn = <-c.idleConns:
	default:
		if c.addr != nil {
			conn, err = Dial(c.addr.String())
			if err != nil {
				return nil, err
			}
		} else {
			conn, err = c.baseConn.Dup()
			if err != nil {
				return nil, err
			}
		}
	}
	resp, err := conn.Exec(cmd, body)
	if err != nil {
		conn.Close()
		return nil, err
	}
	resp.(*response).client = c
	return resp, nil
}

// Query calls Exec with req.GQTPRequest and returns the result.
func (c *Client) Query(req *grnci.Request) (grnci.Response, error) {
	cmd, body, err := req.GQTPRequest()
	if err != nil {
		return nil, err
	}
	return c.Exec(cmd, body)
}
