package libgrn

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"
import "github.com/groonga/grnci/v2"

// Client is a GQTP client.
type Client struct {
	ctx *grnCtx
}

// Connect establishes a connection with a GQTP server.
func Connect(addr string) (*grnci.Client, error) {
	return nil, nil
}

// Close closes a client.
func (c *Client) Close() error {
	return nil
}

// Query sends a request and receives a response.
func (c *Client) Query(req *grnci.Request) (*grnci.Response, error) {
	return nil, nil
}
