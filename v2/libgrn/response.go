package libgrn

import (
	"io"
	"io/ioutil"
	"time"

	"github.com/groonga/grnci/v2"
)

// response is a response.
type response struct {
	conn   *Conn
	left   []byte
	flags  byte
	err    error
	closed bool
}

// newGQTPResponse returns a new GQTP response.
func newGQTPResponse(conn *Conn, name string, data []byte, flags byte, err error) *response {
	return &response{
		conn:  conn,
		left:  data,
		flags: flags,
		err:   err,
	}
}

// newDBResponse returns a new DB response.
func newDBResponse(conn *Conn, data []byte, flags byte, err error) *response {
	return &response{
		conn:  conn,
		left:  data,
		flags: flags,
		err:   err,
	}
}

// Start returns the start time.
func (r *response) Start() time.Time {
	return time.Time{}
}

// Elapsed returns the elapsed time.
func (r *response) Elapsed() time.Duration {
	return 0
}

// Read reads the response body at most len(p) bytes into p.
// The return value n is the number of bytes read.
func (r *response) Read(p []byte) (n int, err error) {
	if r.closed {
		return 0, io.EOF
	}
	for len(r.left) == 0 {
		if r.flags&flagMore == 0 {
			return 0, io.EOF
		}
		data, flags, err := r.conn.ctx.Recv()
		if err != nil {
			r.conn.broken = true
			return 0, err
		}
		r.left = data
		r.flags = flags
	}
	n = copy(p, r.left)
	r.left = r.left[n:]
	return
}

// Close closes the response body.
func (r *response) Close() error {
	if r.closed {
		return nil
	}
	var err error
	if !r.conn.broken {
		if _, err = io.CopyBuffer(ioutil.Discard, r, r.conn.getBuffer()); err != nil {
			r.conn.broken = true
			err = grnci.NewError(grnci.NetworkError, map[string]interface{}{
				"method": "io.CopyBuffer",
				"error":  err.Error(),
			})
		}
	}
	r.closed = true
	if !r.conn.broken {
		r.conn.ready = true
	}
	if r.conn.client != nil {
		// Broken connections are closed.
		if r.conn.broken {
			if e := r.conn.Close(); e != nil && err != nil {
				err = e
			}
		}
		select {
		case r.conn.client.idleConns <- r.conn:
		default:
			if e := r.conn.Close(); e != nil && err != nil {
				err = e
			}
		}
	}
	return err
}

// Err returns the stored error.
func (r *response) Err() error {
	return r.err
}
