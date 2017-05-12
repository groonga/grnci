package grnci

import (
	"fmt"
	"net"
)

// gqtpClient is a GQTP client.
type gqtpClient struct {
	conn net.Conn
}

// dialGQTP returns a new gqtpClient connected to a GQTP server.
func dialGQTP(a *Address) (*gqtpClient, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", a.Host, a.Port))
	if err != nil {
		return nil, err
	}
	return newGQTPClient(conn)
}

// newGQTPClient returns a new gqtpClient using an existing connection.
func newGQTPClient(conn net.Conn) (*gqtpClient, error) {
	return &gqtpClient{conn: conn}, nil
}

// Close closes the connection.
func (c *gqtpClient) Close() error {
	if err := c.conn.Close(); err != nil {
		return err
	}
	return nil
}

// Query sends a request and receives a response.
func (c *gqtpClient) Query(req *Request) (*Response, error) {
	if err := req.Check(); err != nil {
		return nil, err
	}

	// TODO
	return nil, nil
}
