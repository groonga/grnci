package grnci

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"time"
)

// Constants for gqtpHeader.
const (
	gqtpProtocol = byte(0xc7)

	// gqtpQueryTypeNone    = byte(0)
	// gqtpQueryTypeTSV     = byte(1)
	// gqtpQueryTypeJSON    = byte(2)
	// gqtpQueryTypeXML     = byte(3)
	// gqtpQueryTypeMsgPack = byte(4)

	// gqtpFlagMore  = byte(0x01)
	gqtpFlagTail = byte(0x02)
	// gqtpFlagHead  = byte(0x04)
	// gqtpFlagQuiet = byte(0x08)
	// gqtpFlagQuit  = byte(0x10)
)

const (
	gqtpMaxChunkSize        = 1 << 30 // Maximum chunk size
	gqtpDefaultBufferSize   = 1 << 16 // Default buffer size
	gqtpDefaultMaxIdleConns = 2       // Default maximum number of idle connections
)

// gqtpHeader is a GQTP header.
type gqtpHeader struct {
	Protocol  byte   // Must be 0xc7
	QueryType byte   // Body type
	KeyLength uint16 // Unused
	Level     byte   // Unused
	Flags     byte   // Flags
	Status    uint16 // Return code
	Size      uint32 // Body size
	Opaque    uint32 // Unused
	CAS       uint64 // Unused
}

// gqtpResponse is a GQTP response.
type gqtpResponse struct {
	conn   *gqtpConn  // Connection
	head   gqtpHeader // Current header
	err    error      // Error response
	left   int        // Number of bytes left in the current chunk
	closed bool       // Whether or not the response is closed
}

// newGQTPResponse returns a new GQTP response.
func newGQTPResponse(conn *gqtpConn, head gqtpHeader) *gqtpResponse {
	resp := &gqtpResponse{
		conn: conn,
		head: head,
		left: int(head.Size),
	}
	if head.Status > 32767 {
		rc := int(head.Status) - 65536
		resp.err = NewGroongaError(ResultCode(rc), nil)
	}
	return resp
}

// Start returns zero.
func (r *gqtpResponse) Start() time.Time {
	return time.Time{}
}

// Elapsed returns zero.
func (r *gqtpResponse) Elapsed() time.Duration {
	return 0
}

// Read reads up to len(p) bytes from the response body.
// The return value n is the number of bytes read.
func (r *gqtpResponse) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	for r.left == 0 {
		if r.head.Flags&gqtpFlagTail != 0 {
			return 0, io.EOF
		}
		head, err := r.conn.recvHeader()
		if err != nil {
			r.conn.broken = true
			return 0, err
		}
		r.head = head
		r.left = int(head.Size)
	}
	if r.left < len(p) {
		p = p[:r.left]
	}
	n, err := r.conn.conn.Read(p)
	r.left -= n
	if err == io.EOF {
		return n, io.EOF
	}
	if err != nil {
		r.conn.broken = true
		return n, NewError(NetworkError, map[string]interface{}{
			"method": "net.Conn.Read",
			"n":      n,
			"error":  err.Error(),
		})
	}
	return n, nil
}

// Close closes the response body.
func (r *gqtpResponse) Close() error {
	if r.closed {
		return nil
	}
	var err error
	if _, e := io.CopyBuffer(ioutil.Discard, r, r.conn.buf); e != nil {
		r.conn.broken = true
		err = NewError(NetworkError, map[string]interface{}{
			"method": "io.CopyBuffer",
			"error":  e.Error(),
		})
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

// Err returns the error details.
func (r *gqtpResponse) Err() error {
	return r.err
}

// gqtpConnOptions is options of gqtpConn.
type gqtpConnOptions struct {
	BufferSize int
}

// newGQTPConnOptions returns the default gqtpConnOptions.
func newGQTPConnOptions() *gqtpConnOptions {
	return &gqtpConnOptions{
		BufferSize: gqtpDefaultBufferSize,
	}
}

// gqtpConn is a thread-unsafe GQTP client.
type gqtpConn struct {
	client *GQTPClient // Owner client if available
	conn   net.Conn    // Connection to a GQTP server
	buf    []byte      // Copy buffer
	ready  bool        // Whether or not the connection is ready to send a command
	broken bool        // Whether or not the connection is broken
}

// dialGQTP returns a new gqtpConn connected to a GQTP server.
// The expected address format is [scheme://][host][:port].
func dialGQTP(addr string, options *gqtpConnOptions) (*gqtpConn, error) {
	a, err := ParseGQTPAddress(addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", a.Host, a.Port))
	if err != nil {
		return nil, NewError(NetworkError, map[string]interface{}{
			"host":  a.Host,
			"port":  a.Port,
			"error": err.Error(),
		})
	}
	return newGQTPConn(conn, options), nil
}

// newGQTPConn returns a new gqtpConn using an existing connection.
func newGQTPConn(conn net.Conn, options *gqtpConnOptions) *gqtpConn {
	if options == nil {
		options = newGQTPConnOptions()
	}
	return &gqtpConn{
		conn:  conn,
		buf:   make([]byte, options.BufferSize),
		ready: true,
	}
}

// Close closes the connection.
func (c *gqtpConn) Close() error {
	if err := c.conn.Close(); err != nil {
		return NewError(NetworkError, map[string]interface{}{
			"method": "net.Conn.Close",
			"error":  err.Error(),
		})
	}
	return nil
}

// sendHeader sends a GQTP header.
func (c *gqtpConn) sendHeader(flags byte, size int) error {
	head := gqtpHeader{
		Protocol: gqtpProtocol,
		Flags:    flags,
		Size:     uint32(size),
	}
	if err := binary.Write(c.conn, binary.BigEndian, head); err != nil {
		return NewError(NetworkError, map[string]interface{}{
			"method": "binary.Write",
			"error":  err.Error(),
		})
	}
	return nil
}

// sendChunkBytes sends data with flags.
func (c *gqtpConn) sendChunkBytes(data []byte, flags byte) error {
	if err := c.sendHeader(flags, len(data)); err != nil {
		return err
	}
	if _, err := c.conn.Write(data); err != nil {
		return NewError(NetworkError, map[string]interface{}{
			"method": "net.Conn.Write",
			"error":  err.Error(),
		})
	}
	return nil
}

// sendChunkString sends data with flags.
func (c *gqtpConn) sendChunkString(data string, flags byte) error {
	if err := c.sendHeader(flags, len(data)); err != nil {
		return err
	}
	if _, err := io.WriteString(c.conn, data); err != nil {
		return NewError(NetworkError, map[string]interface{}{
			"method": "io.WriteString",
			"error":  err.Error(),
		})
	}
	return nil
}

// recvHeader receives a GQTP header.
func (c *gqtpConn) recvHeader() (gqtpHeader, error) {
	var head gqtpHeader
	if err := binary.Read(c.conn, binary.BigEndian, &head); err != nil {
		return head, NewError(NetworkError, map[string]interface{}{
			"method": "binary.Read",
			"error":  err.Error(),
		})
	}
	return head, nil
}

// execNoBody sends a command without body and receives a response.
func (c *gqtpConn) execNoBody(cmd string) (Response, error) {
	if err := c.sendChunkString(cmd, gqtpFlagTail); err != nil {
		return nil, err
	}
	head, err := c.recvHeader()
	if err != nil {
		return nil, err
	}
	return newGQTPResponse(c, head), nil
}

// execBody sends a command with body and receives a response.
func (c *gqtpConn) execBody(cmd string, body io.Reader) (Response, error) {
	if err := c.sendChunkString(cmd, 0); err != nil {
		return nil, err
	}
	head, err := c.recvHeader()
	if err != nil {
		return nil, err
	}
	if head.Status != 0 || head.Size != 0 {
		return newGQTPResponse(c, head), nil
	}
	n := 0
	for {
		m, err := body.Read(c.buf[n:])
		n += m
		if err != nil {
			if err := c.sendChunkBytes(c.buf[:n], gqtpFlagTail); err != nil {
				return nil, err
			}
			head, err = c.recvHeader()
			if err != nil {
				return nil, err
			}
			return newGQTPResponse(c, head), nil
		}
		if n == len(c.buf) {
			if err := c.sendChunkBytes(c.buf, 0); err != nil {
				return nil, err
			}
			head, err = c.recvHeader()
			if err != nil {
				return nil, err
			}
			if head.Status != 0 || head.Size != 0 {
				return newGQTPResponse(c, head), nil
			}
			n = 0
		}
	}
}

// Exec sends a command and receives a response.
func (c *gqtpConn) Exec(cmd string, body io.Reader) (Response, error) {
	if c.broken {
		return nil, NewError(OperationError, map[string]interface{}{
			"error": "The connection is broken.",
		})
	}
	if !c.ready {
		return nil, NewError(OperationError, map[string]interface{}{
			"error": "The connection is not ready to send a command.",
		})
	}
	if len(cmd) > gqtpMaxChunkSize {
		return nil, NewError(CommandError, map[string]interface{}{
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

// GQTPClientOptions is options of GQTPClient.
type GQTPClientOptions struct {
	BufferSize   int // Buffer size
	MaxIdleConns int // Maximum number of idle connections
}

// NewGQTPClientOptions returns the default GQTPClientOptions.
func NewGQTPClientOptions() *GQTPClientOptions {
	return &GQTPClientOptions{
		BufferSize:   gqtpDefaultBufferSize,
		MaxIdleConns: gqtpDefaultMaxIdleConns,
	}
}

// GQTPClient is a thread-safe GQTP client.
type GQTPClient struct {
	addr        string           // Server address
	connOptions *gqtpConnOptions // Options for connections
	idleConns   chan *gqtpConn   // Idle connections
}

// NewGQTPClient returns a new GQTPClient connected to a GQTP server.
// The expected address format is [scheme://][host][:port].
func NewGQTPClient(addr string, options *GQTPClientOptions) (*GQTPClient, error) {
	if options == nil {
		options = NewGQTPClientOptions()
	}
	connOptions := newGQTPConnOptions()
	connOptions.BufferSize = options.BufferSize
	conn, err := dialGQTP(addr, connOptions)
	if err != nil {
		return nil, err
	}
	c := &GQTPClient{
		addr:        addr,
		connOptions: connOptions,
		idleConns:   make(chan *gqtpConn, options.MaxIdleConns),
	}
	c.idleConns <- conn
	conn.client = c
	return c, nil
}

// Close closes the idle connections.
// Close should be called after all responses are closed.
// Otherwise, connections will be leaked.
func (c *GQTPClient) Close() error {
	var err error
	for {
		select {
		case conn := <-c.idleConns:
			if e := conn.Close(); e != nil && err == nil {
				err = e
			}
		default:
			return err
		}
	}
}

// exec sends a request and receives a response.
func (c *GQTPClient) exec(cmd string, body io.Reader) (Response, error) {
	var conn *gqtpConn
	var err error
	select {
	case conn = <-c.idleConns:
	default:
		conn, err = dialGQTP(c.addr, c.connOptions)
		if err != nil {
			return nil, err
		}
	}
	resp, err := conn.Exec(cmd, body)
	if err != nil {
		conn.Close()
		return nil, err
	}
	return resp, nil
}

// Exec parses cmd, sends the parsed command and returns the response.
// It is the caller's responsibility to close the response.
func (c *GQTPClient) Exec(cmd string, body io.Reader) (Response, error) {
	command, err := ParseCommand(cmd)
	if err != nil {
		return nil, err
	}
	command.SetBody(body)
	return c.Query(command)
}

// Invoke assembles name and params into a command,
// sends the command and returns the response.
// It is the caller's responsibility to close the response.
func (c *GQTPClient) Invoke(name string, params map[string]interface{}, body io.Reader) (Response, error) {
	cmd, err := NewCommand(name, params)
	if err != nil {
		return nil, err
	}
	cmd.SetBody(body)
	return c.Query(cmd)
}

// Query sends cmd and returns the response.
// It is the caller's responsibility to close the response.
func (c *GQTPClient) Query(cmd *Command) (Response, error) {
	if err := cmd.Check(); err != nil {
		return nil, err
	}
	return c.exec(cmd.String(), cmd.Body())
}
