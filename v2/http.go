package grnci

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
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
	if err := req.Check(); err != nil {
		return nil, err
	}

	u := *c.url
	u.Path = path.Join(u.Path, req.Command)
	if len(req.Arguments) != 0 {
		q := u.Query()
		for _, arg := range req.Arguments {
			q.Set(arg.Key, arg.Value)
		}
		u.RawQuery = q.Encode()
	}
	addr := u.String()

	var resp *http.Response
	var err error
	if req.Body == nil {
		if resp, err = c.client.Get(addr); err != nil {
			return nil, fmt.Errorf("c.client.Get failed: %v", err)
		}
	} else {
		if resp, err = c.client.Post(addr, "application/json", req.Body); err != nil {
			return nil, fmt.Errorf("c.client.Post failed: %v", err)
		}
	}
	defer resp.Body.Close()

	// TODO: parse the response.
	respBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("ioutil.ReadAll failed: %v", err)
	}
	return &Response{Bytes: respBytes}, nil
}
