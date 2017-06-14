package grnci

import "io"

// Handler defines the required methods of DB clients and handles.
type Handler interface {
	Exec(cmd string, body io.Reader) (Response, error)
	Invoke(cmd string, params map[string]interface{}, body io.Reader) (Response, error)
	Query(req *Request) (Response, error)
	Close() error
}
