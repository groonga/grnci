package grnci

import (
	"time"
)

// Response stores a response of Groonga.
type Response struct {
	Bytes   []byte
	Error   error
	Time    time.Time
	Elapsed time.Duration
}
