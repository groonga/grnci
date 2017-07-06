package grnci

import "io"

// Handler defines the required methods of DB clients and handles.
type Handler interface {
	// Exec parses cmd, sends the parsed command and returns the response.
	Exec(cmd string, body io.Reader) (Response, error)

	// Invoke assembles name and params into a command,
	// sends the command and returns the response.
	Invoke(name string, params map[string]interface{}, body io.Reader) (Response, error)

	// Query sends cmd and returns the response.
	Query(cmd *Command) (Response, error)

	// Close closes the underlying connections or handles.
	Close() error
}
