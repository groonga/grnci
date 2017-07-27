package libgrn

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"
import (
	"io"
	"strings"
	"unsafe"

	"github.com/groonga/grnci/v2"
)

const (
	maxChunkSize      = 1 << 30 // Maximum chunk size
	defaultBufferSize = 1 << 16 // Default buffer size
)

// conn is a thread-unsafe GQTP client or DB handle.
type conn struct {
	client *Client // Owner client if available
	ctx    *grnCtx // C.grn_ctx
	db     *grnDB  // C.grn_obj
	buf    []byte  // Copy buffer
	ready  bool    // Whether or not the connection is ready to send a command
	broken bool    // Whether or not the connection is broken
}

// newConn returns a new conn.
func newConn(ctx *grnCtx, db *grnDB) *conn {
	return &conn{
		ctx:   ctx,
		db:    db,
		buf:   make([]byte, defaultBufferSize),
		ready: true,
	}
}

// dial returns a new conn connected to a GQTP server.
func dial(addr string) (*conn, error) {
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
	return newConn(ctx, nil), nil
}

// open opens an existing DB and returns a new conn as its handle.
func open(path string) (*conn, error) {
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	db, err := openGrnDB(ctx, path)
	if err != nil {
		ctx.Close()
		return nil, err
	}
	return newConn(ctx, db), nil
}

// create creates a new DB and returns a new conn as its handle.
func create(path string) (*conn, error) {
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	db, err := createGrnDB(ctx, path)
	if err != nil {
		ctx.Close()
		return nil, err
	}
	return newConn(ctx, db), nil
}

// Dup duplicates the conn if it is a DB handle.
func (c *conn) Dup() (*conn, error) {
	if c.db == nil {
		return nil, grnci.NewError(grnci.OperationError, map[string]interface{}{
			"error": "GQTP clients do not support Dup.",
		})
	}
	ctx, err := c.db.Dup()
	if err != nil {
		return nil, err
	}
	return newConn(ctx, c.db), nil
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

// execNoBodyGQTP sends a command and receives a response.
func (c *conn) execNoBodyGQTP(cmd string) (grnci.Response, error) {
	name := strings.TrimLeft(cmd, " \t\r\n")
	if idx := strings.IndexAny(name, " \t\r\n"); idx != -1 {
		name = name[:idx]
	}
	if err := c.ctx.Send([]byte(cmd), flagTail); err != nil {
		return nil, err
	}
	data, flags, err := c.ctx.Recv()
	if err != nil && len(data) == 0 {
		return nil, err
	}
	return newGQTPResponse(c, name, data, flags, err), nil
}

// execNoBodyDB executes a command and receives a response.
func (c *conn) execNoBodyDB(cmd string) (grnci.Response, error) {
	if err := c.ctx.Send([]byte(cmd), flagTail); err != nil {
		data, flags, _ := c.ctx.Recv()
		return newDBResponse(c, data, flags, err), nil
	}
	data, flags, err := c.ctx.Recv()
	return newDBResponse(c, data, flags, err), nil
}

// execNoBody sends a command without body and receives a response.
func (c *conn) execNoBody(cmd string) (grnci.Response, error) {
	if c.db == nil {
		return c.execNoBodyGQTP(cmd)
	}
	return c.execNoBodyDB(cmd)
}

// execBodyGQTP sends a command and receives a response.
func (c *conn) execBodyGQTP(cmd string, body io.Reader) (grnci.Response, error) {
	name := strings.TrimLeft(cmd, " \t\r\n")
	if idx := strings.IndexAny(name, " \t\r\n"); idx != -1 {
		name = name[:idx]
	}
	if err := c.ctx.Send([]byte(cmd), 0); err != nil {
		return nil, err
	}
	data, flags, err := c.ctx.Recv()
	if len(data) != 0 {
		return newGQTPResponse(c, name, data, flags, err), nil
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
				return newGQTPResponse(c, name, data, flags, err), nil
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
				return newGQTPResponse(c, name, data, flags, err), nil
			}
			if err != nil {
				return nil, err
			}
		}
	}
}

// execBodyDB sends a command and receives a response.
func (c *conn) execBodyDB(cmd string, body io.Reader) (grnci.Response, error) {
	if err := c.ctx.Send([]byte(cmd), 0); err != nil {
		data, flags, _ := c.ctx.Recv()
		return newDBResponse(c, data, flags, err), nil
	}
	data, flags, err := c.ctx.Recv()
	if len(data) != 0 || err != nil {
		return newDBResponse(c, data, flags, err), nil
	}
	n := 0
	for {
		m, err := body.Read(c.buf[n:])
		n += m
		if err != nil {
			if err := c.ctx.Send(c.buf[:n], flagTail); err != nil {
				data, flags, _ := c.ctx.Recv()
				return newDBResponse(c, data, flags, err), nil
			}
			data, flags, err := c.ctx.Recv()
			return newDBResponse(c, data, flags, err), nil
		}
		if n == len(c.buf) {
			if err := c.ctx.Send(c.buf, 0); err != nil {
				data, flags, _ := c.ctx.Recv()
				return newDBResponse(c, data, flags, err), nil
			}
			n = 0
			data, flags, err = c.ctx.Recv()
			if len(data) != 0 || err != nil {
				return newDBResponse(c, data, flags, err), nil
			}
		}
	}
}

// execBody sends a command with body and receives a response.
func (c *conn) execBody(cmd string, body io.Reader) (grnci.Response, error) {
	if c.db == nil {
		return c.execBodyGQTP(cmd, body)
	}
	return c.execBodyDB(cmd, body)
}

// Exec sends a command and receives a response.
func (c *conn) Exec(cmd string, body io.Reader) (grnci.Response, error) {
	if c.broken {
		return nil, grnci.NewError(grnci.OperationError, map[string]interface{}{
			"error": "The connection is broken.",
		})
	}
	if !c.ready {
		return nil, grnci.NewError(grnci.OperationError, map[string]interface{}{
			"error": "The connection is not ready to send a command.",
		})
	}
	if len(cmd) > maxChunkSize {
		return nil, grnci.NewError(grnci.CommandError, map[string]interface{}{
			"length": len(cmd),
			"error":  "The command is too long.",
		})
	}
	c.ready = false
	if body == nil {
		return c.execNoBody(cmd)
	}
	return c.execBody(cmd, body)
}
