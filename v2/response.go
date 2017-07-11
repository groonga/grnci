package grnci

import (
	"time"
)

// Response is the interface of responses.
type Response interface {
	// Start returns the server-side start time of the command if available.
	// Responses of HTTPClient may return a valid (non-zero) time.
	Start() time.Time

	// Elapsed returns the server-side elapsed time of the command if available.
	// Responses of HTTPClient may return a valid (non-zero) duration.
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
