package grnci

import "io"

// Handler defines the required methods of DB clients and handles.
type Handler interface {
	Exec(cmd string, body io.Reader) (Response, error)
	Invoke(name string, params map[string]interface{}, body io.Reader) (Response, error)
	Query(cmd *Command) (Response, error)
	Close() error
}
