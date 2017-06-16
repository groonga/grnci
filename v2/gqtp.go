package grnci

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
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
	gqtpMaxChunkSize      = 1 << 30 // Maximum chunk size
	gqtpDefaultBufferSize = 1 << 16 // Default buffer size
	gqtpMaxIdleConns      = 2       // Maximum number of idle connections
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
	client  *GQTPClient   // Client
	conn    *GQTPConn     // Connection
	head    gqtpHeader    // Current header
	start   time.Time     // Start time
	elapsed time.Duration // Elapsed time
	err     error         // Error response
	left    int           // Number of bytes left in the current chunk
	broken  bool          // Whether or not the connection is broken
	closed  bool          // Whether or not the response is closed
}

// newGQTPResponse returns a new GQTP response.
func newGQTPResponse(conn *GQTPConn, head gqtpHeader, start time.Time, name string) *gqtpResponse {
	resp := &gqtpResponse{
		conn:    conn,
		head:    head,
		start:   start,
		elapsed: time.Now().Sub(start),
		left:    int(head.Size),
	}
	if head.Status > 32767 {
		code := int(head.Status) - 65536
		resp.err = NewError(code, nil)
	}
	return resp
}

func (r *gqtpResponse) Start() time.Time {
	return r.start
}

func (r *gqtpResponse) Elapsed() time.Duration {
	return r.elapsed
}

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
			r.broken = true
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
		r.broken = true
		return n, NewError(NetworkError, map[string]interface{}{
			"method": "net.Conn.Read",
			"n":      n,
			"error":  err.Error(),
		})
	}
	return n, nil
}

func (r *gqtpResponse) Close() error {
	if r.closed {
		return nil
	}
	var err error
	if _, e := io.CopyBuffer(ioutil.Discard, r, r.conn.getBuffer()); e != nil {
		r.broken = true
		err = NewError(NetworkError, map[string]interface{}{
			"method": "io.CopyBuffer",
			"error":  err.Error(),
		})
	}
	r.closed = true
	if err == nil {
		r.conn.ready = true
	}
	if r.client != nil {
		// Broken connections are closed.
		if r.broken {
			if e := r.conn.Close(); e != nil && err != nil {
				err = e
			}
		}
		select {
		case r.client.idleConns <- r.conn:
		default:
			if e := r.conn.Close(); e != nil && err != nil {
				err = e
			}
		}
	}
	return err
}

func (r *gqtpResponse) Err() error {
	return r.err
}

// GQTPConn is a thread-unsafe GQTP client.
type GQTPConn struct {
	conn    net.Conn // Connection to a GQTP server
	buf     []byte   // Copy buffer
	bufSize int      // Copy buffer size
	ready   bool     // Whether or not Exec and Query are ready
}

// DialGQTP returns a new GQTPConn connected to a GQTP server.
// The expected address format is [scheme://][host][:port].
func DialGQTP(addr string) (*GQTPConn, error) {
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
	return NewGQTPConn(conn), nil
}

// NewGQTPConn returns a new GQTPConn using an existing connection.
func NewGQTPConn(conn net.Conn) *GQTPConn {
	return &GQTPConn{
		conn:    conn,
		bufSize: gqtpDefaultBufferSize,
		ready:   true,
	}
}

// Close closes the connection.
func (c *GQTPConn) Close() error {
	if err := c.conn.Close(); err != nil {
		return NewError(NetworkError, map[string]interface{}{
			"method": "net.Conn.Close",
			"error":  err.Error(),
		})
	}
	return nil
}

// SetBufferSize updates the size of the copy buffer.
func (c *GQTPConn) SetBufferSize(n int) {
	if n <= 0 || n > gqtpMaxChunkSize {
		n = gqtpDefaultBufferSize
	}
	c.bufSize = n
}

// getBuffer returns the copy buffer.
func (c *GQTPConn) getBuffer() []byte {
	if len(c.buf) != c.bufSize {
		c.buf = make([]byte, c.bufSize)
	}
	return c.buf
}

// sendHeader sends a GQTP header.
func (c *GQTPConn) sendHeader(flags byte, size int) error {
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
func (c *GQTPConn) sendChunkBytes(data []byte, flags byte) error {
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
func (c *GQTPConn) sendChunkString(data string, flags byte) error {
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
func (c *GQTPConn) recvHeader() (gqtpHeader, error) {
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
func (c *GQTPConn) execNoBody(cmd string) (Response, error) {
	start := time.Now()
	name := strings.TrimLeft(cmd, " \t\r\n")
	if idx := strings.IndexAny(name, " \t\r\n"); idx != -1 {
		name = name[:idx]
	}
	if err := c.sendChunkString(cmd, gqtpFlagTail); err != nil {
		return nil, err
	}
	head, err := c.recvHeader()
	if err != nil {
		return nil, err
	}
	return newGQTPResponse(c, head, start, name), nil
}

// execBody sends a command with body and receives a response.
func (c *GQTPConn) execBody(cmd string, body io.Reader) (Response, error) {
	start := time.Now()
	name := strings.TrimLeft(cmd, " \t\r\n")
	if idx := strings.IndexAny(name, " \t\r\n"); idx != -1 {
		name = name[:idx]
	}
	if err := c.sendChunkString(cmd, 0); err != nil {
		return nil, err
	}
	head, err := c.recvHeader()
	if err != nil {
		return nil, err
	}
	if head.Status != 0 || head.Size != 0 {
		return newGQTPResponse(c, head, start, name), nil
	}
	n := 0
	buf := c.getBuffer()
	for {
		m, err := body.Read(buf[n:])
		n += m
		if err != nil {
			if err := c.sendChunkBytes(buf[:n], gqtpFlagTail); err != nil {
				return nil, err
			}
			head, err = c.recvHeader()
			if err != nil {
				return nil, err
			}
			return newGQTPResponse(c, head, start, name), nil
		}
		if n == len(buf) {
			if err := c.sendChunkBytes(buf, 0); err != nil {
				return nil, err
			}
			head, err = c.recvHeader()
			if err != nil {
				return nil, err
			}
			if head.Status != 0 || head.Size != 0 {
				return newGQTPResponse(c, head, start, name), nil
			}
			n = 0
		}
	}
}

// exec sends a command without body and receives a response.
func (c *GQTPConn) exec(cmd string, body io.Reader) (Response, error) {
	if !c.ready {
		return nil, NewError(InvalidOperation, map[string]interface{}{
			"error": "The connection is not ready to send a command.",
		})
	}
	if len(cmd) > gqtpMaxChunkSize {
		return nil, NewError(InvalidCommand, map[string]interface{}{
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
// The GQTPConn must not be used until the response is closed.
func (c *GQTPConn) Exec(cmd string, body io.Reader) (Response, error) {
	command, err := ParseCommand(cmd)
	if err != nil {
		return nil, err
	}
	command.SetBody(body)
	return c.Query(command)
}

// Invoke assembles name, params and body into a command and calls Query.
func (c *GQTPConn) Invoke(name string, params map[string]interface{}, body io.Reader) (Response, error) {
	cmd, err := NewCommand(name, params)
	if err != nil {
		return nil, err
	}
	cmd.SetBody(body)
	return c.Query(cmd)
}

// Query sends a command and receives a response.
// It is the caller's responsibility to close the response.
func (c *GQTPConn) Query(cmd *Command) (Response, error) {
	if err := cmd.Check(); err != nil {
		return nil, err
	}
	return c.exec(cmd.String(), cmd.Body())
}

// GQTPClient is a thread-safe GQTP client.
type GQTPClient struct {
	addr      *Address
	idleConns chan *GQTPConn
}

// NewGQTPClient returns a new GQTPClient connected to a GQTP server.
// The expected address format is [scheme://][host][:port].
func NewGQTPClient(addr string) (*GQTPClient, error) {
	a, err := ParseGQTPAddress(addr)
	if err != nil {
		return nil, err
	}
	conn, err := DialGQTP(addr)
	if err != nil {
		return nil, err
	}
	conns := make(chan *GQTPConn, gqtpMaxIdleConns)
	conns <- conn
	return &GQTPClient{
		addr:      a,
		idleConns: conns,
	}, nil
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
	var conn *GQTPConn
	var err error
	select {
	case conn = <-c.idleConns:
	default:
		conn, err = DialGQTP(c.addr.String())
		if err != nil {
			return nil, err
		}
	}
	resp, err := conn.Exec(cmd, body)
	if err != nil {
		conn.Close()
		return nil, err
	}
	resp.(*gqtpResponse).client = c
	return resp, nil
}

// Exec parses cmd, reassembles it and calls Query.
func (c *GQTPClient) Exec(cmd string, body io.Reader) (Response, error) {
	command, err := ParseCommand(cmd)
	if err != nil {
		return nil, err
	}
	command.SetBody(body)
	return c.Query(command)
}

// Invoke assembles name, params and body into a command and calls Query.
func (c *GQTPClient) Invoke(name string, params map[string]interface{}, body io.Reader) (Response, error) {
	cmd, err := NewCommand(name, params)
	if err != nil {
		return nil, err
	}
	cmd.SetBody(body)
	return c.Query(cmd)
}

// Query sends a command and receives a response.
// It is the caller's responsibility to close the response.
func (c *GQTPClient) Query(cmd *Command) (Response, error) {
	if err := cmd.Check(); err != nil {
		return nil, err
	}
	return c.exec(cmd.String(), cmd.Body())
}
