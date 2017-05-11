package grnci

import (
	"time"
)

type Response struct {
	Bytes   []byte
	Error   error
	Time    time.Time
	Elapsed time.Duration
}
