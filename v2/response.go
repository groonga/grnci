package grnci

import (
	"time"
)

// Response is an interface for responses.
type Response interface {
	// Status returns the status code.
	Status() int

	// Start returns the start time.
	Start() time.Time

	// Elapsed returns the elapsed time.
	Elapsed() time.Duration

	// Read reads the response body at most len(p) bytes into p.
	// The return value n is the number of bytes read.
	Read(p []byte) (n int, err error)

	// Close closes the response body.
	Close() error

	// Err returns an error.
	Err() error
}
