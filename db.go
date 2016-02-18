package grnci

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
// #include "grnci.h"
import "C"

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"unsafe"
)

//
// DB handle
//

// refCount is a reference counter for DB.obj.
type refCount struct {
	cnt   int        // Count.
	mutex sync.Mutex // Mutex for the reference count.
}

// newRefCount() creates a reference counter.
func newRefCount() *refCount {
	return &refCount{}
}

// DB is a handle to a database or a connection to a server.
type DB struct {
	ctx  *C.grn_ctx // Context.
	obj  *C.grn_obj // Database object (handle).
	path string     // Database path (handle).
	ref  *refCount  // Reference counter for obj.
	host string     // Server host name (connection).
	port int        // Server port number (connection).
}

// newDB() creates an instance of DB.
// The instance must be finalized by DB.fin().
func newDB() (*DB, error) {
	if err := grnInit(); err != nil {
		return nil, err
	}
	var db DB
	db.ctx = C.grn_ctx_open(C.int(0))
	if db.ctx == nil {
		grnFin()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	return &db, nil
}

// fin() finalizes an instance of DB.
func (db *DB) fin() error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if db.ctx == nil {
		return nil
	}
	if db.obj != nil {
		if db.ref == nil {
			return fmt.Errorf("ref is nil")
		}
		db.ref.mutex.Lock()
		db.ref.cnt--
		if db.ref.cnt == 0 {
			C.grn_obj_close(db.ctx, db.obj)
		}
		db.ref.mutex.Unlock()
		db.obj = nil
		db.ref = nil
	} else {
		db.host = ""
		db.port = 0
	}
	rc := C.grn_ctx_close(db.ctx)
	db.ctx = nil
	grnFin()
	if rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ctx_close() failed: rc = %s", rc)
	}
	return nil
}

// Errorf() creates an error.
func (db *DB) errorf(format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	if (db == nil) || (db.ctx == nil) || (db.ctx.rc == C.GRN_SUCCESS) {
		return fmt.Errorf(format, args...)
	}
	ctxMsg := C.GoString(&db.ctx.errbuf[0])
	return fmt.Errorf("%s: ctx.rc = %s, ctx.errbuf = %s", msg, db.ctx.rc, ctxMsg)
}

// IsHandle() returns whether db is a handle.
func (db *DB) IsHandle() bool {
	return (db != nil) && (db.obj != nil)
}

// IsConnection() returns whether db is a connection.
func (db *DB) IsConnection() bool {
	return (db != nil) && (len(db.host) != 0)
}

// Path() returns the database path if db is a handle.
// Otherwise, it returns "".
func (db *DB) Path() string {
	if db == nil {
		return ""
	}
	return db.path
}

// Host() returns the server host name if db is a connection.
// Otherwise, it returns "".
func (db *DB) Host() string {
	if db == nil {
		return ""
	}
	return db.host
}

// Port() returns the server port number if db is a connection.
// Otherwise, it returns 0.
func (db *DB) Port() int {
	if db == nil {
		return 0
	}
	return db.port
}

// check() returns an error if db is invalid.
func (db *DB) check() error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if db.ctx == nil {
		return fmt.Errorf("ctx is nil")
	}
	if (db.obj == nil) && (len(db.host) == 0) {
		return fmt.Errorf("neither a handle nor a connection")
	}
	return nil
}

// Create() creates a database and returns a handle to it.
// The handle must be closed by DB.Close().
func Create(path string) (*DB, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("path is empty")
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	db.obj = C.grn_db_create(db.ctx, cPath, nil)
	if db.obj == nil {
		db.fin()
		return nil, fmt.Errorf("grn_db_create() failed")
	}
	db.ref = newRefCount()
	db.ref.cnt++
	cAbsPath := C.grn_obj_path(db.ctx, db.obj)
	if cAbsPath == nil {
		db.fin()
		return nil, fmt.Errorf("grn_obj_path() failed")
	}
	db.path = C.GoString(cAbsPath)
	return db, nil
}

// Open() opens a database and returns a handle to it.
// The handle must be closed by DB.Close().
func Open(path string) (*DB, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("path is empty")
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	db.obj = C.grn_db_open(db.ctx, cPath)
	if db.obj == nil {
		db.fin()
		return nil, fmt.Errorf("grn_db_open() failed")
	}
	db.ref = newRefCount()
	db.ref.cnt++
	cAbsPath := C.grn_obj_path(db.ctx, db.obj)
	if cAbsPath == nil {
		db.fin()
		return nil, fmt.Errorf("grn_obj_path() failed")
	}
	db.path = C.GoString(cAbsPath)
	return db, nil
}

// Connect() establishes a connection to a server.
// The connection must be closed by DB.Close().
func Connect(host string, port int) (*DB, error) {
	if len(host) == 0 {
		return nil, fmt.Errorf("host is empty")
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	rc := C.grn_ctx_connect(db.ctx, cHost, C.int(port), C.int(0))
	if rc != C.GRN_SUCCESS {
		db.fin()
		return nil, fmt.Errorf("grn_ctx_connect() failed: rc = %s", rc)
	}
	db.host = host
	db.port = port
	return db, nil
}

// Dup() duplicates a handle or a connection.
// The handle or connection must be closed by DB.Close().
func (db *DB) Dup() (*DB, error) {
	if err := db.check(); err != nil {
		return nil, err
	}
	if db.IsConnection() {
		return Connect(db.host, db.port)
	}
	dupDB, err := newDB()
	if err != nil {
		return nil, err
	}
	if rc := C.grn_ctx_use(dupDB.ctx, db.obj); rc != C.GRN_SUCCESS {
		dupDB.fin()
		return nil, db.errorf("grn_ctx_use() failed: rc = %s", rc)
	}
	dupDB.obj = db.obj
	dupDB.ref = db.ref
	dupDB.ref.mutex.Lock()
	dupDB.ref.cnt++
	dupDB.ref.mutex.Unlock()
	dupDB.path = db.path
	return dupDB, nil
}

// Close() closes a handle or a connection.
func (db *DB) Close() error {
	if err := db.check(); err != nil {
		return err
	}
	return db.fin()
}

//
// Low-level command interface
//

// checkCmdName() checks whether a string is valid as a command name.
func checkCmdName(s string) error {
	if len(s) == 0 {
		return fmt.Errorf("command name must not be empty")
	}
	if s[0] == '_' {
		return fmt.Errorf("command name must not start with '_'")
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= 'a') && (s[i] <= 'z')) && (s[i] != '_') {
			return fmt.Errorf("command name must not contain \\x%X", s[i])
		}
	}
	return nil
}

// checkCmdArgKey() checks whether a string is valid as an argument key.
func checkArgKey(s string) error {
	if len(s) == 0 {
		return fmt.Errorf("command name must not be empty")
	}
	if s[0] == '_' {
		return fmt.Errorf("command name must not start with '_'")
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= 'a') && (s[i] <= 'z')) && (s[i] != '_') {
			return fmt.Errorf("command name must not contain \\x%X", s[i])
		}
	}
	return nil
}

type cmdArg struct {
	Key   string
	Value string
}

// composeCommand() composes a command from a name and arguments.
func (db *DB) composeCommand(name string, args []cmdArg) (string, error) {
	if err := checkCmdName(name); err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	if _, err := buf.WriteString(name); err != nil {
		return "", err
	}
	for _, arg := range args {
		if err := checkArgKey(arg.Key); err != nil {
			return "", err
		}
		val := strings.Replace(arg.Value, "\\", "\\\\", -1)
		val = strings.Replace(val, "'", "\\'", -1)
		fmt.Fprintf(buf, " --%s '%s'", arg.Key, val)
	}
	return buf.String(), nil
}

// send() sends a command.
func (db *DB) send(cmd string) error {
	if len(cmd) == 0 {
		return fmt.Errorf("cmd is empty")
	}
	cCmd := C.CString(cmd)
	defer C.free(unsafe.Pointer(cCmd))
	rc := C.grn_rc(C.grn_ctx_send(db.ctx, cCmd, C.uint(len(cmd)), C.int(0)))
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		return db.errorf("grn_ctx_send() failed: rc = %s", rc)
	}
	return nil
}

// recv() receives the result of a command sent by send().
func (db *DB) recv() ([]byte, error) {
	var res *C.char
	var resLen C.uint
	var resFlags C.int
	rc := C.grn_rc(C.grn_ctx_recv(db.ctx, &res, &resLen, &resFlags))
	if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
		return nil, db.errorf("grn_ctx_recv() failed: rc = %s", rc)
	}
	if (resFlags & C.GRN_CTX_MORE) == 0 {
		return C.GoBytes(unsafe.Pointer(res), C.int(resLen)), nil
	}
	buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(res), C.int(resLen)))
	var bufErr error
	for {
		rc := C.grn_rc(C.grn_ctx_recv(db.ctx, &res, &resLen, &resFlags))
		if (rc != C.GRN_SUCCESS) || (db.ctx.rc != C.GRN_SUCCESS) {
			return nil, db.errorf("grn_ctx_recv() failed: rc = %s", rc)
		}
		if bufErr == nil {
			_, bufErr = buf.Write(C.GoBytes(unsafe.Pointer(res), C.int(resLen)))
		}
		if (resFlags & C.GRN_CTX_MORE) == 0 {
			break
		}
	}
	if bufErr != nil {
		return nil, bufErr
	}
	return buf.Bytes(), nil
}

// query() executes a command.
func (db *DB) query(cmd string) ([]byte, error) {
	if err := db.send(cmd); err != nil {
		res, _ := db.recv()
		return res, err
	}
	return db.recv()
}

// qureyEx() executes a command with separated arguments.
func (db *DB) queryEx(name string, args []cmdArg) ([]byte, error) {
	cmd, err := db.composeCommand(name, args)
	if err != nil {
		return nil, err
	}
	return db.query(cmd)
}
