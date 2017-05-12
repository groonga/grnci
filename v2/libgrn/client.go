package libgrn

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"
import "github.com/groonga/grnci/v2"

// Client is associated with a C.grn_ctx.
type Client struct {
	ctx *grnCtx
}

// Open opens a local Groonga DB.
func Open(path string) (*grnci.Client, error) {
	return nil, nil
}

// Create creates a local Groonga DB.
func Create(path string) (*grnci.Client, error) {
	return nil, nil
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
