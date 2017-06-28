package grnci

import (
	"time"
)

// Response is the interface of responses.
type Response interface {
	// Start returns the start time of the command.
	// The definition of the start time varies according to the protocol.
	//
	// HTTPClient returns a response with the server-side start time,
	// because an HTTP server returns a result with the start time.
	//
	// GQTPConn and GQTPClient return a response with the client-side start time,
	// because a GQTP server does not return the start time.
	// libgrn.Conn and libgrn.Client also return the client-side start time.
	Start() time.Time

	// Elapsed returns the elapsed time of the command.
	// The definition of the elapsed time varies likewise the start time.
	// See above for the details.
	Elapsed() time.Duration

	// Read reads the response body at most len(p) bytes into p.
	// The return value n is the number of bytes read.
	Read(p []byte) (n int, err error)

	// Close closes the response body.
	Close() error

	// Err returns the details of an error response.
	// If the command was successfully completed, Err returns nil.
	Err() error
}
