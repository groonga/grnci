package grnci

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
// #include "grnci.h"
import "C"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"unsafe"
)

// joinErrors joins errors.
func joinErrors(errs []error) error {
	if len(errs) == 1 {
		return errs[0]
	} else if len(errs) > 1 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// DBMode is a mode of DB instance.
type DBMode int

const (
	InvalidDB = DBMode(iota) // Invalid instance
	LocalDB                  // Handle to a local Groonga DB
	GQTPClient               // Connection to a GQTP Groonga server
	HTTPClient               // Connection to an HTTP Groonga server
)

// localDB is a handle to a local Groonga DB.
type localDB struct {
	ctx   *C.grn_ctx  // Context
	obj   *C.grn_obj  // Database object
	path  string      // Database path
	cnt   *int        // Reference count
	mutex *sync.Mutex // Mutex for reference count
}

func newLocalDB() *localDB {
	return &localDB{}
}

func (db *localDB) fin() error {
	if db == nil {
		return fmt.Errorf("db = nil")
	}
	if db.ctx == nil {
		return nil
	}
	var errs []error
	if db.obj != nil {
		db.mutex.Lock()
		*db.cnt--
		if *db.cnt == 0 {
			if rc := C.grn_obj_close(db.ctx, db.obj); rc != C.GRN_SUCCESS {
				errs = append(errs, fmt.Errorf("C.grn_obj_close failed: rc = %s", rc))
			}
		}
		db.mutex.Unlock()
	}
	if rc := C.grn_ctx_close(db.ctx); rc != C.GRN_SUCCESS {
		errs = append(errs, fmt.Errorf("C.grn_ctx_close failed: rc = %s", rc))
	}
	return joinErrors(errs)
}

func (db *localDB) errorf(format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	if db == nil {
		return fmt.Errorf("%s", msg)
	}
	if (db.ctx == nil) || (db.ctx.rc == C.GRN_SUCCESS) {
		return fmt.Errorf("%s: path = \"%s\"", msg, db.path)
	}
	ctxMsg := C.GoString(&db.ctx.errbuf[0])
	return fmt.Errorf("%s: path = \"%s\", ctx = %s \"%s\"",
		msg, db.path, db.ctx.rc, ctxMsg)
}

func createLocalDB(path string) (*localDB, error) {
	var errs []error
	db := newLocalDB()
	if db.ctx = C.grn_ctx_open(C.int(0)); db.ctx == nil {
		errs = append(errs, fmt.Errorf("C.grn_ctx_open failed"))
	} else {
		cPath := C.CString(path)
		defer C.free(unsafe.Pointer(cPath))
		if db.obj = C.grn_db_create(db.ctx, cPath, nil); db.obj == nil {
			err := fmt.Errorf("C.grn_db_create failed: path = \"%s\"", path)
			errs = append(errs, err)
		} else {
			db.cnt = new(int)
			db.mutex = new(sync.Mutex)
			*db.cnt++
			if cAbsPath := C.grn_obj_path(db.ctx, db.obj); cAbsPath == nil {
				errs = append(errs, fmt.Errorf("C.grn_obj_path failed"))
			} else {
				db.path = C.GoString(cAbsPath)
			}
		}
	}
	if errs != nil {
		if err := db.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	return db, nil
}

func openLocalDB(path string) (*localDB, error) {
	var errs []error
	db := newLocalDB()
	if db.ctx = C.grn_ctx_open(C.int(0)); db.ctx == nil {
		errs = append(errs, fmt.Errorf("C.grn_ctx_open failed"))
	} else {
		cPath := C.CString(path)
		defer C.free(unsafe.Pointer(cPath))
		if db.obj = C.grn_db_open(db.ctx, cPath); db.obj == nil {
			err := fmt.Errorf("C.grn_db_open failed: path = \"%s\"", path)
			errs = append(errs, err)
		} else {
			db.cnt = new(int)
			db.mutex = new(sync.Mutex)
			*db.cnt++
			if cAbsPath := C.grn_obj_path(db.ctx, db.obj); cAbsPath == nil {
				errs = append(errs, fmt.Errorf("C.grn_obj_path failed"))
			} else {
				db.path = C.GoString(cAbsPath)
			}
		}
	}
	if errs != nil {
		if err := db.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	return db, nil
}

func (db *localDB) dup() (*localDB, error) {
	var errs []error
	dupDB := newLocalDB()
	if dupDB.ctx = C.grn_ctx_open(C.int(0)); dupDB.ctx == nil {
		errs = append(errs, fmt.Errorf("C.grn_ctx_open failed"))
	} else {
		if rc := C.grn_ctx_use(dupDB.ctx, db.obj); rc != C.GRN_SUCCESS {
			errs = append(errs, db.errorf("C.grn_ctx_use failed: rc = %s", rc))
		}
	}
	if errs != nil {
		if err := dupDB.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	dupDB.obj = db.obj
	dupDB.path = db.path
	dupDB.cnt = db.cnt
	dupDB.mutex = db.mutex
	*dupDB.cnt++
	return dupDB, nil
}

func (db *localDB) check() error {
	if db == nil {
		return fmt.Errorf("db = nil")
	}
	if db.ctx == nil {
		return fmt.Errorf("ctx = nil")
	}
	if db.obj == nil {
		return fmt.Errorf("obj = nil")
	}
	return nil
}

// checkCmdName checks whether s is valid as a command name.
func checkCmdName(s string) error {
	if s == "" {
		return fmt.Errorf("invalid command name: s = \"\"")
	}
	if s[0] == '_' {
		return fmt.Errorf("invalid command name: s = \"%s\"", s)
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= 'a') && (s[i] <= 'z')) && (s[i] != '_') {
			return fmt.Errorf("invalid command name: s = \"%s\"", s)
		}
	}
	return nil
}

// checkCmdArgKey checks whether s is valid as a command argument key.
func checkCmdArgKey(s string) error {
	if s == "" {
		return fmt.Errorf("invalid command argument key: s = \"\"")
	}
	if s[0] == '_' {
		return fmt.Errorf("invalid command argument key: s = \"%s\"", s)
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= 'a') && (s[i] <= 'z')) && (s[i] != '_') {
			return fmt.Errorf("invalid command argument key: s = \"%s\"", s)
		}
	}
	return nil
}

type cmdArg struct {
	key string
	val string
}

// check checks whether a command argument is valid.
func (arg *cmdArg) check() error {
	return checkCmdArgKey(arg.key)
}

// composeCmd composes a command from its name and arguments.
func composeCmd(name string, args []cmdArg) (string, error) {
	if err := checkCmdName(name); err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if _, err := buf.WriteString(name); err != nil {
		return "", err
	}
	for _, arg := range args {
		if err := arg.check(); err != nil {
			return "", err
		}
		val := strings.Replace(arg.val, "\\", "\\\\", -1)
		val = strings.Replace(val, "'", "\\'", -1)
		fmt.Fprintf(buf, " --%s '%s'", arg.key, val)
	}
	return buf.String(), nil
}

// send sends data.
func (db *localDB) send(data []byte) error {
	var p *C.char
	if len(data) != 0 {
		p = (*C.char)(unsafe.Pointer(&data[0]))
	}
	rc := C.grn_rc(C.grn_ctx_send(db.ctx, p, C.uint(len(data)), C.int(0)))
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		return db.errorf("C.grn_ctx_send failed: rc = %s", rc)
	}
	return nil
}

// recv receives the response to sent data.
func (db *localDB) recv() ([]byte, error) {
	var resp *C.char
	var respLen C.uint
	var respFlags C.int
	rc := C.grn_rc(C.grn_ctx_recv(db.ctx, &resp, &respLen, &respFlags))
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		return nil, db.errorf("C.grn_ctx_recv failed: rc = %s", rc)
	}
	return C.GoBytes(unsafe.Pointer(resp), C.int(respLen)), nil
}

// query sends a command and receives the response.
func (db *localDB) query(name string, args []cmdArg, data []byte) ([]byte, error) {
	cmd, err := composeCmd(name, args)
	if err != nil {
		return nil, err
	}
	// Send a command.
	if err := db.send([]byte(cmd)); err != nil {
		resp, _ := db.recv()
		return resp, err
	}
	resp, err := db.recv()
	if (data == nil) || (err != nil) {
		return resp, err
	}
	// Send data if available.
	if len(resp) != 0 {
		return resp, db.errorf("unexpected response")
	}
	if err := db.send(data); err != nil {
		resp, _ := db.recv()
		return resp, err
	}
	return db.recv()
}

// gqtpClient is a connection to a GQTP Groonga server.
type gqtpClient struct {
	ctx  *C.grn_ctx // Context
	host string     // Server's host name or IP address
	port int        // Server's port number
}

func newGQTPClient() *gqtpClient {
	return &gqtpClient{}
}

func (db *gqtpClient) fin() error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if db.ctx == nil {
		return nil
	}
	var errs []error
	if rc := C.grn_ctx_close(db.ctx); rc != C.GRN_SUCCESS {
		errs = append(errs, fmt.Errorf("C.grn_ctx_close failed: rc = %s", rc))
	}
	return joinErrors(errs)
}

func (db *gqtpClient) errorf(format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	if db == nil {
		return fmt.Errorf("%s", msg)
	}
	if (db.ctx == nil) || (db.ctx.rc == C.GRN_SUCCESS) {
		return fmt.Errorf("%s: host = \"%s\", port = %d", msg, db.host, db.port)
	}
	ctxMsg := C.GoString(&db.ctx.errbuf[0])
	return fmt.Errorf("%s: host = \"%s\", port = %d, ctx = %s \"%s\"",
		msg, db.host, db.port, db.ctx.rc, ctxMsg)
}

func (db *gqtpClient) check() error {
	if db == nil {
		return fmt.Errorf("db = nil")
	}
	if db.ctx == nil {
		return fmt.Errorf("ctx = nil")
	}
	return nil
}

func openGQTPClient(host string, port int) (*gqtpClient, error) {
	var errs []error
	db := newGQTPClient()
	if db.ctx = C.grn_ctx_open(C.int(0)); db.ctx == nil {
		errs = append(errs, fmt.Errorf("C.grn_ctx_open failed"))
	} else {
		cHost := C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
		rc := C.grn_ctx_connect(db.ctx, cHost, C.int(port), C.int(0))
		if rc != C.GRN_SUCCESS {
			const format = "C.grn_ctx_connect failed: host = \"%s\", port = %d, rc = %s"
			err := fmt.Errorf(format, host, port, rc)
			errs = append(errs, err)
		} else {
			db.host = host
			db.port = port
		}
	}
	if errs != nil {
		if err := db.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	return db, nil
}

// send sends data.
func (db *gqtpClient) send(data []byte) error {
	var p *C.char
	if len(data) != 0 {
		p = (*C.char)(unsafe.Pointer(&data[0]))
	}
	rc := C.grn_rc(C.grn_ctx_send(db.ctx, p, C.uint(len(data)), C.int(0)))
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		return db.errorf("grn_ctx_send failed: rc = %s", rc)
	}
	return nil
}

// recv receives the response to sent data.
func (db *gqtpClient) recv() ([]byte, error) {
	var resp *C.char
	var respLen C.uint
	var respFlags C.int
	rc := C.grn_rc(C.grn_ctx_recv(db.ctx, &resp, &respLen, &respFlags))
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		return nil, db.errorf("grn_ctx_recv failed: rc = %s", rc)
	}
	if (respFlags & C.GRN_CTX_MORE) == 0 {
		return C.GoBytes(unsafe.Pointer(resp), C.int(respLen)), nil
	}
	buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(resp), C.int(respLen)))
	var bufErr error
	for {
		rc := C.grn_rc(C.grn_ctx_recv(db.ctx, &resp, &respLen, &respFlags))
		if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
			return nil, db.errorf("grn_ctx_recv failed: rc = %s", rc)
		}
		if bufErr == nil {
			_, bufErr = buf.Write(C.GoBytes(unsafe.Pointer(resp), C.int(respLen)))
		}
		if (respFlags & C.GRN_CTX_MORE) == 0 {
			break
		}
	}
	if bufErr != nil {
		return nil, bufErr
	}
	return buf.Bytes(), nil
}

// query sends a command and receives the response.
func (db *gqtpClient) query(name string, args []cmdArg, data []byte) ([]byte, error) {
	cmd, err := composeCmd(name, args)
	if err != nil {
		return nil, err
	}
	// Send a command.
	if err := db.send([]byte(cmd)); err != nil {
		resp, _ := db.recv()
		return resp, err
	}
	resp, err := db.recv()
	if (data == nil) || (err != nil) {
		return resp, err
	}
	// Send data if available.
	if len(resp) != 0 {
		return resp, db.errorf("unexpected response")
	}
	if err := db.send(data); err != nil {
		resp, _ := db.recv()
		return resp, err
	}
	return db.recv()
}

// httpClient is a connection to an HTTP Groonga server.
type httpClient struct {
	client *http.Client // HTTP client
	addr   string       // Server address, e.g. http://localhost:10041
}

func newHTTPClient() *httpClient {
	return &httpClient{}
}

func (db *httpClient) fin() error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	return nil
}

func (db *httpClient) errorf(format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	if db == nil {
		return fmt.Errorf("%s", msg)
	}
	return fmt.Errorf("%s: addr = \"%s\"", msg, db.addr)
}

func (db *httpClient) check() error {
	if db == nil {
		return fmt.Errorf("db = nil")
	}
	if db.client == nil {
		return fmt.Errorf("client = nil")
	}
	return nil
}

func openHTTPClient(addr string, client *http.Client) (*httpClient, error) {
	return &httpClient{
		client: client,
		addr:   addr,
	}, nil
}

// query sends a command and receives the response.
func (db *httpClient) query(name string, args []cmdArg, data []byte) ([]byte, error) {
	u, err := url.Parse(db.addr)
	if err != nil {
		return nil, db.errorf("url.Parse failed: %v", err)
	}
	u.Path = path.Join(u.Path, "select")
	if len(args) != 0 {
		q := u.Query()
		for _, arg := range args {
			q.Set(arg.key, arg.val)
		}
		u.RawQuery = q.Encode()
	}
	addr := u.String()

	var respBytes []byte
	if len(data) == 0 {
		resp, err := db.client.Get(addr)
		if err != nil {
			return nil, db.errorf("db.client.Get failed: %v", err)
		}
		defer resp.Body.Close()
		if respBytes, err = ioutil.ReadAll(resp.Body); err != nil {
			return nil, db.errorf("ioutil.ReadAll failed: %v", err)
		}
	} else {
		resp, err := db.client.Post(addr, "application/json",
			bytes.NewReader(data))
		if err != nil {
			return nil, db.errorf("db.client.Post failed: %v", err)
		}
		defer resp.Body.Close()
		if respBytes, err = ioutil.ReadAll(resp.Body); err != nil {
			return nil, db.errorf("ioutil.ReadAll failed: %v", err)
		}
	}
	var rawMsgs []json.RawMessage
	if err := json.Unmarshal(respBytes, &rawMsgs); err != nil {
		return nil, db.errorf("json.Unmarshal failed: %v", err)
	}
	switch len(rawMsgs) {
	case 0:
		return nil, db.errorf("failed")
	case 2:
		return rawMsgs[1], nil
	default:
		return nil, db.errorf("failed: %s", rawMsgs[0])
	}
}

// DB is a handle to a Groonga DB or a connection to a Groonga server.
//
// Note that DB is not thread-safe.
// DB.Dup is useful to create a DB instance for each thread.
type DB struct {
	mode       DBMode      // Mode
	localDB    *localDB    // Handle to a local Groonga DB
	gqtpClient *gqtpClient // Connection to a GQTP Groonga server
	httpClient *httpClient // Connection to an HTTP Groonga server
}

// Mode returns the DB mode.
func (db *DB) Mode() DBMode {
	if db == nil {
		return InvalidDB
	}
	return db.mode
}

// Path returns the DB file path if db is a handle.
// Otherwise, it returns "".
func (db *DB) Path() string {
	if db == nil {
		return ""
	}
	switch db.mode {
	case LocalDB:
		return db.localDB.path
	default:
		return ""
	}
}

// Host returns server's host name or IP address if db is a connection.
// Otherwise, it returns "".
func (db *DB) Host() string {
	if db == nil {
		return ""
	}
	switch db.mode {
	case GQTPClient:
		return db.gqtpClient.host
	default:
		return ""
	}
}

// Port returns server's port number if db is a connection.
// Otherwise, it returns 0.
func (db *DB) Port() int {
	if db == nil {
		return 0
	}
	switch db.mode {
	case GQTPClient:
		return db.gqtpClient.port
	default:
		return 0
	}
}

// check returns an error if db is invalid.
func (db *DB) check() error {
	if db == nil {
		return fmt.Errorf("db = nil")
	}
	switch db.mode {
	case InvalidDB:
		return fmt.Errorf("mode = InvalidDB")
	case LocalDB:
		return db.localDB.check()
	case GQTPClient:
		return db.gqtpClient.check()
	case HTTPClient:
		return db.httpClient.check()
	default:
		return fmt.Errorf("undefined mode: mode = %d", db.mode)
	}
}

// newDB creates a DB instance.
// The instance must be finalized by DB.fin.
func newDB() (*DB, error) {
	if err := grnInit(); err != nil {
		return nil, err
	}
	return &DB{}, nil
}

// fin finalizes a DB instance.
func (db *DB) fin() error {
	if db == nil {
		return fmt.Errorf("db = nil")
	}
	var errs []error
	switch db.mode {
	case LocalDB:
		if err := db.localDB.fin(); err != nil {
			errs = append(errs, err)
		}
	case GQTPClient:
		if err := db.gqtpClient.fin(); err != nil {
			errs = append(errs, err)
		}
	case HTTPClient:
		if err := db.httpClient.fin(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := grnFin(); err != nil {
		errs = append(errs, err)
	}
	return joinErrors(errs)
}

// Create creates a local DB and returns a handle to it.
// The handle must be closed by DB.Close.
func Create(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("path = \"\"")
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	var errs []error
	if db.localDB, err = createLocalDB(path); err != nil {
		errs = append(errs, err)
		if err := db.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	db.mode = LocalDB
	return db, nil
}

// Open opens a local DB and returns a handle to it.
// The handle must be closed by DB.Close.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("path = \"\"")
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	var errs []error
	if db.localDB, err = openLocalDB(path); err != nil {
		errs = append(errs, err)
		if err := db.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	db.mode = LocalDB
	return db, nil
}

// Connect establishes a connection to a GQTP Groonga server.
// The connection must be closed by DB.Close.
func Connect(host string, port int) (*DB, error) {
	if host == "" {
		return nil, fmt.Errorf("host = \"\"")
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	var errs []error
	if db.gqtpClient, err = openGQTPClient(host, port); err != nil {
		errs = append(errs, err)
		if err := db.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	db.mode = GQTPClient
	return db, nil
}

// Connect establishes a connection to an HTTP Groonga server.
// The connection must be closed by DB.Close.
func ConnectHTTP(addr string, client *http.Client) (*DB, error) {
	if addr == "" {
		return nil, fmt.Errorf("addr = \"\"")
	}
	if client == nil {
		client = http.DefaultClient
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	var errs []error
	if db.httpClient, err = openHTTPClient(addr, client); err != nil {
		errs = append(errs, err)
		if err := db.fin(); err != nil {
			errs = append(errs, err)
		}
		return nil, joinErrors(errs)
	}
	db.mode = HTTPClient
	return db, nil
}

// Dup duplicates a DB instance.
// The instance must be closed by DB.Close.
func (db *DB) Dup() (*DB, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	switch db.mode {
	case LocalDB:
		dupDB, err := newDB()
		if err != nil {
			return nil, err
		}
		var errs []error
		if dupDB.localDB, err = db.localDB.dup(); err != nil {
			errs = append(errs, err)
		}
		if len(errs) != 0 {
			if err := dupDB.fin(); err != nil {
				errs = append(errs, err)
			}
			return nil, joinErrors(errs)
		}
		dupDB.mode = db.mode
		return dupDB, nil
	case GQTPClient:
		return Connect(db.Host(), db.Port())
	case HTTPClient:
		return ConnectHTTP(db.httpClient.addr, db.httpClient.client)
	default:
		return nil, fmt.Errorf("undefined mode: %v", db.mode)
	}
}

// Close closes a handle or a connection.
func (db *DB) Close() error {
	if err := db.check(); err != nil {
		return err
	}
	return db.fin()
}

// errorf creates an error.
func (db *DB) errorf(format string, args ...interface{}) error {
	switch db.mode {
	case LocalDB:
		return db.localDB.errorf(format, args...)
	case GQTPClient:
		return db.gqtpClient.errorf(format, args...)
	case HTTPClient:
		return db.httpClient.errorf(format, args...)
	default:
		return fmt.Errorf(format, args...)
	}
}

// exec sends a command and receives a response.
func (db *DB) exec(data []byte) ([]byte, error) {
	switch db.mode {
	case LocalDB:
		if err := db.localDB.send(data); err != nil {
			resp, _ := db.localDB.recv()
			return resp, err
		}
		return db.localDB.recv()
	case GQTPClient:
		if err := db.gqtpClient.send(data); err != nil {
			resp, _ := db.gqtpClient.recv()
			return resp, err
		}
		return db.gqtpClient.recv()
	case HTTPClient:
		return nil, fmt.Errorf("httpClient does not support exec.")
	default:
		return nil, fmt.Errorf("invalid mode: %d", db.mode)
	}
}

// query sends a command and receives a response.
func (db *DB) query(name string, args []cmdArg, data []byte) ([]byte, error) {
	switch db.mode {
	case LocalDB:
		return db.localDB.query(name, args, data)
	case GQTPClient:
		return db.gqtpClient.query(name, args, data)
	case HTTPClient:
		return db.httpClient.query(name, args, data)
	default:
		return nil, fmt.Errorf("invalid mode: %d", db.mode)
	}
}
