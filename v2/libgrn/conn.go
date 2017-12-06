package libgrn

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"
import (
	"io"
	"unicode/utf8"
	"unsafe"

	"github.com/groonga/grnci/v2"
)

const (
	maxChunkSize      = 1 << 30 // Maximum chunk size
	defaultBufferSize = 1 << 16 // Default buffer size
)

// connOptions is options of conn.
type connOptions struct {
	BufferSize int
}

// newConnOptions returns the default connOptions.
func newConnOptions() *connOptions {
	return &connOptions{
		BufferSize: defaultBufferSize,
	}
}

// conn is a thread-unsafe GQTP client or DB handle.
type conn struct {
	client  *Client      // Owner client if available
	ctx     *grnCtx      // C.grn_ctx
	db      *grnDB       // C.grn_obj
	options *connOptions // Options
	buf     []byte       // Copy buffer
	ready   bool         // Whether or not the connection is ready to send a command
	broken  bool         // Whether or not the connection is broken
}

// newConn returns a new conn.
func newConn(ctx *grnCtx, db *grnDB, options *connOptions) *conn {
	if options == nil {
		options = newConnOptions()
	}
	optionsClone := *options
	return &conn{
		ctx:     ctx,
		db:      db,
		buf:     make([]byte, options.BufferSize),
		options: &optionsClone,
		ready:   true,
	}
}

// dial returns a new conn connected to a GQTP server.
func dial(addr string, options *connOptions) (*conn, error) {
	a, err := grnci.ParseGQTPAddress(addr)
	if err != nil {
		return nil, err
	}
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	cHost := C.CString(a.Host)
	defer C.free(unsafe.Pointer(cHost))
	// C.grn_ctx_connect always returns ctx.ctx.rc.
	C.grn_ctx_connect(ctx.ctx, cHost, C.int(a.Port), 0)
	if err := ctx.Err("C.grn_ctx_connect"); err != nil {
		ctx.Close()
		return nil, err
	}
	return newConn(ctx, nil, options), nil
}

// open opens an existing DB and returns a new conn as its handle.
func open(path string, options *connOptions) (*conn, error) {
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	db, err := openGrnDB(ctx, path)
	if err != nil {
		ctx.Close()
		return nil, err
	}
	return newConn(ctx, db, options), nil
}

// create creates a new DB and returns a new conn as its handle.
func create(path string, options *connOptions) (*conn, error) {
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	db, err := createGrnDB(ctx, path)
	if err != nil {
		ctx.Close()
		return nil, err
	}
	return newConn(ctx, db, options), nil
}

// Dup duplicates the conn if it is a DB handle.
func (c *conn) Dup() (*conn, error) {
	if c.db == nil {
		return nil, grnci.NewError(grnci.OperationError, "GQTP clients do not support Dup.", nil)
	}
	ctx, err := c.db.Dup()
	if err != nil {
		return nil, err
	}
	return newConn(ctx, c.db, c.options), nil
}

// Close closes the conn.
func (c *conn) Close() error {
	var err error
	if c.db != nil {
		if e := c.db.Close(c.ctx); e != nil {
			err = e
		}
	}
	if e := c.ctx.Close(); e != nil {
		if err == nil {
			err = e
		}
	}
	return err
}

// execGQTPBody sends a command and receives a response.
func (c *conn) execGQTPBody(cmd string, body io.Reader) (grnci.Response, error) {
	if err := c.ctx.Send([]byte(cmd), 0); err != nil {
		return nil, err
	}
	data, flags, err := c.ctx.Recv()
	if len(data) != 0 {
		return newResponse(c, data, flags, err), nil
	}
	if err != nil {
		return nil, err
	}
	n := 0
	for {
		m, err := body.Read(c.buf[n:])
		n += m
		if err != nil {
			if err := c.ctx.Send(c.buf[:n], flagTail); err != nil {
				return nil, err
			}
			data, flags, err := c.ctx.Recv()
			if len(data) != 0 || err == nil {
				return newResponse(c, data, flags, err), nil
			}
			return nil, err
		}
		if n == len(c.buf) {
			if err := c.ctx.Send(c.buf, 0); err != nil {
				return nil, err
			}
			n = 0
			data, flags, err = c.ctx.Recv()
			if len(data) != 0 {
				return newResponse(c, data, flags, err), nil
			}
			if err != nil {
				return nil, err
			}
		}
	}
}

// execGQTP sends a command and receives a response.
func (c *conn) execGQTP(cmd string, body io.Reader) (grnci.Response, error) {
	if body != nil {
		return c.execGQTPBody(cmd, body)
	}
	if err := c.ctx.Send([]byte(cmd), flagTail); err != nil {
		return nil, err
	}
	data, flags, err := c.ctx.Recv()
	if err != nil && len(data) == 0 {
		return nil, err
	}
	return newResponse(c, data, flags, err), nil
}

// execDBBody sends a command and receives a response.
func (c *conn) execDBBody(cmd string, body io.Reader) (grnci.Response, error) {
	if err := c.ctx.Send([]byte(cmd), 0); err != nil {
		data, flags, _ := c.ctx.Recv()
		return newResponse(c, data, flags, err), nil
	}
	data, flags, err := c.ctx.Recv()
	if len(data) != 0 || err != nil {
		return newResponse(c, data, flags, err), nil
	}
	n := 0
	for {
		m, err := body.Read(c.buf[n:])
		n += m
		if err != nil {
			if err := c.ctx.Send(c.buf[:n], flagTail); err != nil {
				data, flags, _ := c.ctx.Recv()
				return newResponse(c, data, flags, err), nil
			}
			data, flags, err := c.ctx.Recv()
			return newResponse(c, data, flags, err), nil
		}
		if n == len(c.buf) {
			const maxOdd = 6
			odd := 0
			for ; odd <= maxOdd; odd++ {
				r, size := utf8.DecodeLastRune(c.buf[:n-odd])
				if r != utf8.RuneError || size != 1 {
					break
				}
			}
			if odd > maxOdd {
				// FIXME: failed to find a good break.
				odd = 0
			}
			if err := c.ctx.Send(c.buf[:n-odd], 0); err != nil {
				data, flags, _ := c.ctx.Recv()
				return newResponse(c, data, flags, err), nil
			}
			copy(c.buf, c.buf[n-odd:])
			n = odd
			data, flags, err = c.ctx.Recv()
			if len(data) != 0 || err != nil {
				return newResponse(c, data, flags, err), nil
			}
		}
	}
}

// execDB sends a command and receives a response.
func (c *conn) execDB(cmd string, body io.Reader) (grnci.Response, error) {
	if body != nil {
		return c.execDBBody(cmd, body)
	}
	if err := c.ctx.Send([]byte(cmd), flagTail); err != nil {
		data, flags, _ := c.ctx.Recv()
		return newResponse(c, data, flags, err), nil
	}
	data, flags, err := c.ctx.Recv()
	return newResponse(c, data, flags, err), nil
}

// Exec sends a command and receives a response.
func (c *conn) Exec(cmd string, body io.Reader) (grnci.Response, error) {
	if c.broken {
		return nil, grnci.NewError(grnci.OperationError, "The connection is broken.", nil)
	}
	if !c.ready {
		return nil, grnci.NewError(grnci.OperationError, "The connection is not ready to send a command.", nil)
	}
	if len(cmd) > maxChunkSize {
		return nil, grnci.NewError(grnci.CommandError, "The command is too long.", map[string]interface{}{
			"length": len(cmd),
		})
	}
	c.ready = false
	if c.db == nil {
		return c.execGQTP(cmd, body)
	}
	return c.execDB(cmd, body)
}
