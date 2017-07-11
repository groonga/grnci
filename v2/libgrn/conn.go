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

// Conn is a thread-unsafe GQTP client or DB handle.
type Conn struct {
	ctx     *grnCtx // C.grn_ctx
	db      *grnDB  // C.grn_obj
	buf     []byte  // Copy buffer
	bufSize int     // Copy buffer size
	ready   bool    // Whether or not Exec is ready
}

// newConn returns a new Conn.
func newConn(ctx *grnCtx, db *grnDB) *Conn {
	return &Conn{
		ctx:     ctx,
		db:      db,
		bufSize: defaultBufferSize,
		ready:   true,
	}
}

// Dial returns a new Conn connected to a GQTP server.
func Dial(addr string) (*Conn, error) {
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

// Open opens an existing DB and returns a new Conn as its handle.
func Open(path string) (*Conn, error) {
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

// Create creates a new DB and returns a new Conn as its handle.
func Create(path string) (*Conn, error) {
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

// Dup duplicates the Conn if it is a DB handle.
func (c *Conn) Dup() (*Conn, error) {
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

// Close closes the Conn.
func (c *Conn) Close() error {
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

// SetBufferSize updates the size of the copy buffer.
func (c *Conn) SetBufferSize(n int) {
	if n <= 0 || n > maxChunkSize {
		n = defaultBufferSize
	}
	c.bufSize = n
}

// getBuffer returns the copy buffer.
func (c *Conn) getBuffer() []byte {
	if len(c.buf) != c.bufSize {
		c.buf = make([]byte, c.bufSize)
	}
	return c.buf
}

// execNoBodyGQTP sends a command and receives a response.
func (c *Conn) execNoBodyGQTP(cmd string) (grnci.Response, error) {
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
func (c *Conn) execNoBodyDB(cmd string) (grnci.Response, error) {
	if err := c.ctx.Send([]byte(cmd), flagTail); err != nil {
		data, flags, _ := c.ctx.Recv()
		return newDBResponse(c, data, flags, err), nil
	}
	data, flags, err := c.ctx.Recv()
	return newDBResponse(c, data, flags, err), nil
}

// execNoBody sends a command without body and receives a response.
func (c *Conn) execNoBody(cmd string) (grnci.Response, error) {
	if c.db == nil {
		return c.execNoBodyGQTP(cmd)
	}
	return c.execNoBodyDB(cmd)
}

// execBodyGQTP sends a command and receives a response.
func (c *Conn) execBodyGQTP(cmd string, body io.Reader) (grnci.Response, error) {
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
	buf := c.getBuffer()
	for {
		m, err := body.Read(buf[n:])
		n += m
		if err != nil {
			if err := c.ctx.Send(buf[:n], flagTail); err != nil {
				return nil, err
			}
			data, flags, err := c.ctx.Recv()
			if len(data) != 0 || err == nil {
				return newGQTPResponse(c, name, data, flags, err), nil
			}
			return nil, err
		}
		if n == len(buf) {
			if err := c.ctx.Send(buf, 0); err != nil {
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
func (c *Conn) execBodyDB(cmd string, body io.Reader) (grnci.Response, error) {
	if err := c.ctx.Send([]byte(cmd), 0); err != nil {
		data, flags, _ := c.ctx.Recv()
		return newDBResponse(c, data, flags, err), nil
	}
	data, flags, err := c.ctx.Recv()
	if len(data) != 0 || err != nil {
		return newDBResponse(c, data, flags, err), nil
	}
	n := 0
	buf := c.getBuffer()
	for {
		m, err := body.Read(buf[n:])
		n += m
		if err != nil {
			if err := c.ctx.Send(buf[:n], flagTail); err != nil {
				data, flags, _ := c.ctx.Recv()
				return newDBResponse(c, data, flags, err), nil
			}
			data, flags, err := c.ctx.Recv()
			return newDBResponse(c, data, flags, err), nil
		}
		if n == len(buf) {
			if err := c.ctx.Send(buf, 0); err != nil {
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
func (c *Conn) execBody(cmd string, body io.Reader) (grnci.Response, error) {
	if c.db == nil {
		return c.execBodyGQTP(cmd, body)
	}
	return c.execBodyDB(cmd, body)
}

// exec sends a command and receives a response.
func (c *Conn) exec(cmd string, body io.Reader) (grnci.Response, error) {
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

// Exec parses cmd, reassembles it and calls Query.
// The Conn must not be used until the response is closed.
func (c *Conn) Exec(cmd string, body io.Reader) (grnci.Response, error) {
	command, err := grnci.ParseCommand(cmd)
	if err != nil {
		return nil, err
	}
	command.SetBody(body)
	return c.Query(command)
}

// Invoke assembles name, params and body into a command and calls Query.
func (c *Conn) Invoke(name string, params map[string]interface{}, body io.Reader) (grnci.Response, error) {
	cmd, err := grnci.NewCommand(name, params)
	if err != nil {
		return nil, err
	}
	cmd.SetBody(body)
	return c.Query(cmd)
}

// Query sends a command and receives a response.
// It is the caller's responsibility to close the response.
func (c *Conn) Query(cmd *grnci.Command) (grnci.Response, error) {
	if err := cmd.Check(); err != nil {
		return nil, err
	}
	return c.exec(cmd.String(), cmd.Body())
}
