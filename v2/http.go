package grnci

import (
	"fmt"
	"net/http"
	"net/url"
)

// httpClient is an HTTP client.
type httpClient struct {
	url    *url.URL
	client *http.Client
}

// newHTTPClient returns a new httpClient.
func newHTTPClient(a *Address, client *http.Client) (*httpClient, error) {
	url, err := url.Parse(a.String())
	if err != nil {
		return nil, fmt.Errorf("url.Parse failed: %v", err)
	}
	if client == nil {
		client = http.DefaultClient
	}
	return &httpClient{
		url:    url,
		client: client,
	}, nil
}

// Close closes a client.
func (c *httpClient) Close() error {
	return nil
}

// Query sends a request and receives a response.
func (c *httpClient) Query(req *Request) (*Response, error) {
	// TODO
	return nil, nil
}
