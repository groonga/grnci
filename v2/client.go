package grnci

import (
	"fmt"
	"net"
	"net/http"
)

// iClient is a Groonga client interface.
type iClient interface {
	Close() error
	Query(*Request) (*Response, error)
}

// Client is a Groonga client.
type Client struct {
	iClient
}

// NewClient returns a new Client using an existing client.
func NewClient(c iClient) *Client {
	return &Client{iClient: c}
}

// NewClientByAddress returns a new Client to access a Groonga server.
func NewClientByAddress(addr string) (*Client, error) {
	a, err := ParseAddress(addr)
	if err != nil {
		return nil, err
	}
	switch a.Scheme {
	case gqtpScheme:
		c, err := dialGQTP(a)
		if err != nil {
			return nil, err
		}
		return NewClient(c), nil
	case httpScheme, httpsScheme:
		c, err := newHTTPClient(a, nil)
		if err != nil {
			return nil, err
		}
		return NewClient(c), nil
	default:
		return nil, fmt.Errorf("invalid scheme: raw = %s", a.Raw)
	}
}

// NewGQTPClient returns a new Client using an existing connection.
func NewGQTPClient(conn net.Conn) (*Client, error) {
	c, err := newGQTPClient(conn)
	if err != nil {
		return nil, err
	}
	return NewClient(c), nil
}

// NewHTTPClient returns a new Client using an existing HTTP client.
// If client is nil, NewHTTPClient uses http.DefaultClient.
func NewHTTPClient(addr string, client *http.Client) (*Client, error) {
	a, err := ParseAddress(addr)
	if err != nil {
		return nil, err
	}
	if client == nil {
		client = http.DefaultClient
	}
	switch a.Scheme {
	case httpScheme, httpsScheme:
	default:
		return nil, fmt.Errorf("invalid scheme: raw = %s", a.Raw)
	}
	c, err := newHTTPClient(a, client)
	if err != nil {
		return nil, err
	}
	return NewClient(c), nil
}
