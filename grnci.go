// Package grnci provides an interface to execute Groonga commands.
//
// grnci: Groonga Command Interface (test version)
//
// The grnci package is experimental and supports only a subset of Groonga
// commands.
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
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"
)

//
// Error handling
//

// String() returns an error code with its name as a string.
func (rc C.grn_rc) String() string {
	switch rc {
	case C.GRN_SUCCESS:
		return fmt.Sprintf("GRN_SUCCESS (%d)", rc)
	case C.GRN_END_OF_DATA:
		return fmt.Sprintf("GRN_END_OF_DATA (%d)", rc)
	case C.GRN_UNKNOWN_ERROR:
		return fmt.Sprintf("GRN_UNKNOWN_ERROR (%d)", rc)
	case C.GRN_OPERATION_NOT_PERMITTED:
		return fmt.Sprintf("GRN_OPERATION_NOT_PERMITTED (%d)", rc)
	case C.GRN_NO_SUCH_FILE_OR_DIRECTORY:
		return fmt.Sprintf("GRN_NO_SUCH_FILE_OR_DIRECTORY (%d)", rc)
	case C.GRN_NO_SUCH_PROCESS:
		return fmt.Sprintf("GRN_NO_SUCH_PROCESS (%d)", rc)
	case C.GRN_INTERRUPTED_FUNCTION_CALL:
		return fmt.Sprintf("GRN_INTERRUPTED_FUNCTION_CALL (%d)", rc)
	case C.GRN_INPUT_OUTPUT_ERROR:
		return fmt.Sprintf("GRN_INPUT_OUTPUT_ERROR (%d)", rc)
	case C.GRN_NO_SUCH_DEVICE_OR_ADDRESS:
		return fmt.Sprintf("GRN_NO_SUCH_DEVICE_OR_ADDRESS (%d)", rc)
	case C.GRN_ARG_LIST_TOO_LONG:
		return fmt.Sprintf("GRN_ARG_LIST_TOO_LONG (%d)", rc)
	case C.GRN_EXEC_FORMAT_ERROR:
		return fmt.Sprintf("GRN_EXEC_FORMAT_ERROR (%d)", rc)
	case C.GRN_BAD_FILE_DESCRIPTOR:
		return fmt.Sprintf("GRN_BAD_FILE_DESCRIPTOR (%d)", rc)
	case C.GRN_NO_CHILD_PROCESSES:
		return fmt.Sprintf("GRN_NO_CHILD_PROCESSES (%d)", rc)
	case C.GRN_RESOURCE_TEMPORARILY_UNAVAILABLE:
		return fmt.Sprintf("GRN_RESOURCE_TEMPORARILY_UNAVAILABLE (%d)", rc)
	case C.GRN_NOT_ENOUGH_SPACE:
		return fmt.Sprintf("GRN_NOT_ENOUGH_SPACE (%d)", rc)
	case C.GRN_PERMISSION_DENIED:
		return fmt.Sprintf("GRN_PERMISSION_DENIED (%d)", rc)
	case C.GRN_BAD_ADDRESS:
		return fmt.Sprintf("GRN_BAD_ADDRESS (%d)", rc)
	case C.GRN_RESOURCE_BUSY:
		return fmt.Sprintf("GRN_RESOURCE_BUSY (%d)", rc)
	case C.GRN_FILE_EXISTS:
		return fmt.Sprintf("GRN_FILE_EXISTS (%d)", rc)
	case C.GRN_IMPROPER_LINK:
		return fmt.Sprintf("GRN_IMPROPER_LINK (%d)", rc)
	case C.GRN_NO_SUCH_DEVICE:
		return fmt.Sprintf("GRN_NO_SUCH_DEVICE (%d)", rc)
	case C.GRN_NOT_A_DIRECTORY:
		return fmt.Sprintf("GRN_NOT_A_DIRECTORY (%d)", rc)
	case C.GRN_IS_A_DIRECTORY:
		return fmt.Sprintf("GRN_IS_A_DIRECTORY (%d)", rc)
	case C.GRN_INVALID_ARGUMENT:
		return fmt.Sprintf("GRN_INVALID_ARGUMENT (%d)", rc)
	case C.GRN_TOO_MANY_OPEN_FILES_IN_SYSTEM:
		return fmt.Sprintf("GRN_TOO_MANY_OPEN_FILES_IN_SYSTEM (%d)", rc)
	case C.GRN_TOO_MANY_OPEN_FILES:
		return fmt.Sprintf("GRN_TOO_MANY_OPEN_FILES (%d)", rc)
	case C.GRN_INAPPROPRIATE_I_O_CONTROL_OPERATION:
		return fmt.Sprintf("GRN_INAPPROPRIATE_I_O_CONTROL_OPERATION (%d)", rc)
	case C.GRN_FILE_TOO_LARGE:
		return fmt.Sprintf("GRN_FILE_TOO_LARGE (%d)", rc)
	case C.GRN_NO_SPACE_LEFT_ON_DEVICE:
		return fmt.Sprintf("GRN_NO_SPACE_LEFT_ON_DEVICE (%d)", rc)
	case C.GRN_INVALID_SEEK:
		return fmt.Sprintf("GRN_INVALID_SEEK (%d)", rc)
	case C.GRN_READ_ONLY_FILE_SYSTEM:
		return fmt.Sprintf("GRN_READ_ONLY_FILE_SYSTEM (%d)", rc)
	case C.GRN_TOO_MANY_LINKS:
		return fmt.Sprintf("GRN_TOO_MANY_LINKS (%d)", rc)
	case C.GRN_BROKEN_PIPE:
		return fmt.Sprintf("GRN_BROKEN_PIPE (%d)", rc)
	case C.GRN_DOMAIN_ERROR:
		return fmt.Sprintf("GRN_DOMAIN_ERROR (%d)", rc)
	case C.GRN_RESULT_TOO_LARGE:
		return fmt.Sprintf("GRN_RESULT_TOO_LARGE (%d)", rc)
	case C.GRN_RESOURCE_DEADLOCK_AVOIDED:
		return fmt.Sprintf("GRN_RESOURCE_DEADLOCK_AVOIDED (%d)", rc)
	case C.GRN_NO_MEMORY_AVAILABLE:
		return fmt.Sprintf("GRN_NO_MEMORY_AVAILABLE (%d)", rc)
	case C.GRN_FILENAME_TOO_LONG:
		return fmt.Sprintf("GRN_FILENAME_TOO_LONG (%d)", rc)
	case C.GRN_NO_LOCKS_AVAILABLE:
		return fmt.Sprintf("GRN_NO_LOCKS_AVAILABLE (%d)", rc)
	case C.GRN_FUNCTION_NOT_IMPLEMENTED:
		return fmt.Sprintf("GRN_FUNCTION_NOT_IMPLEMENTED (%d)", rc)
	case C.GRN_DIRECTORY_NOT_EMPTY:
		return fmt.Sprintf("GRN_DIRECTORY_NOT_EMPTY (%d)", rc)
	case C.GRN_ILLEGAL_BYTE_SEQUENCE:
		return fmt.Sprintf("GRN_ILLEGAL_BYTE_SEQUENCE (%d)", rc)
	case C.GRN_SOCKET_NOT_INITIALIZED:
		return fmt.Sprintf("GRN_SOCKET_NOT_INITIALIZED (%d)", rc)
	case C.GRN_OPERATION_WOULD_BLOCK:
		return fmt.Sprintf("GRN_OPERATION_WOULD_BLOCK (%d)", rc)
	case C.GRN_ADDRESS_IS_NOT_AVAILABLE:
		return fmt.Sprintf("GRN_ADDRESS_IS_NOT_AVAILABLE (%d)", rc)
	case C.GRN_NETWORK_IS_DOWN:
		return fmt.Sprintf("GRN_NETWORK_IS_DOWN (%d)", rc)
	case C.GRN_NO_BUFFER:
		return fmt.Sprintf("GRN_NO_BUFFER (%d)", rc)
	case C.GRN_SOCKET_IS_ALREADY_CONNECTED:
		return fmt.Sprintf("GRN_SOCKET_IS_ALREADY_CONNECTED (%d)", rc)
	case C.GRN_SOCKET_IS_NOT_CONNECTED:
		return fmt.Sprintf("GRN_SOCKET_IS_NOT_CONNECTED (%d)", rc)
	case C.GRN_SOCKET_IS_ALREADY_SHUTDOWNED:
		return fmt.Sprintf("GRN_SOCKET_IS_ALREADY_SHUTDOWNED (%d)", rc)
	case C.GRN_OPERATION_TIMEOUT:
		return fmt.Sprintf("GRN_OPERATION_TIMEOUT (%d)", rc)
	case C.GRN_CONNECTION_REFUSED:
		return fmt.Sprintf("GRN_CONNECTION_REFUSED (%d)", rc)
	case C.GRN_RANGE_ERROR:
		return fmt.Sprintf("GRN_RANGE_ERROR (%d)", rc)
	case C.GRN_TOKENIZER_ERROR:
		return fmt.Sprintf("GRN_TOKENIZER_ERROR (%d)", rc)
	case C.GRN_FILE_CORRUPT:
		return fmt.Sprintf("GRN_FILE_CORRUPT (%d)", rc)
	case C.GRN_INVALID_FORMAT:
		return fmt.Sprintf("GRN_INVALID_FORMAT (%d)", rc)
	case C.GRN_OBJECT_CORRUPT:
		return fmt.Sprintf("GRN_OBJECT_CORRUPT (%d)", rc)
	case C.GRN_TOO_MANY_SYMBOLIC_LINKS:
		return fmt.Sprintf("GRN_TOO_MANY_SYMBOLIC_LINKS (%d)", rc)
	case C.GRN_NOT_SOCKET:
		return fmt.Sprintf("GRN_NOT_SOCKET (%d)", rc)
	case C.GRN_OPERATION_NOT_SUPPORTED:
		return fmt.Sprintf("GRN_OPERATION_NOT_SUPPORTED (%d)", rc)
	case C.GRN_ADDRESS_IS_IN_USE:
		return fmt.Sprintf("GRN_ADDRESS_IS_IN_USE (%d)", rc)
	case C.GRN_ZLIB_ERROR:
		return fmt.Sprintf("GRN_ZLIB_ERROR (%d)", rc)
	case C.GRN_LZ4_ERROR:
		return fmt.Sprintf("GRN_LZ4_ERROR (%d)", rc)
	case C.GRN_STACK_OVER_FLOW:
		return fmt.Sprintf("GRN_STACK_OVER_FLOW (%d)", rc)
	case C.GRN_SYNTAX_ERROR:
		return fmt.Sprintf("GRN_SYNTAX_ERROR (%d)", rc)
	case C.GRN_RETRY_MAX:
		return fmt.Sprintf("GRN_RETRY_MAX (%d)", rc)
	case C.GRN_INCOMPATIBLE_FILE_FORMAT:
		return fmt.Sprintf("GRN_INCOMPATIBLE_FILE_FORMAT (%d)", rc)
	case C.GRN_UPDATE_NOT_ALLOWED:
		return fmt.Sprintf("GRN_UPDATE_NOT_ALLOWED (%d)", rc)
	case C.GRN_TOO_SMALL_OFFSET:
		return fmt.Sprintf("GRN_TOO_SMALL_OFFSET (%d)", rc)
	case C.GRN_TOO_LARGE_OFFSET:
		return fmt.Sprintf("GRN_TOO_LARGE_OFFSET (%d)", rc)
	case C.GRN_TOO_SMALL_LIMIT:
		return fmt.Sprintf("GRN_TOO_SMALL_LIMIT (%d)", rc)
	case C.GRN_CAS_ERROR:
		return fmt.Sprintf("GRN_CAS_ERROR (%d)", rc)
	case C.GRN_UNSUPPORTED_COMMAND_VERSION:
		return fmt.Sprintf("GRN_UNSUPPORTED_COMMAND_VERSION (%d)", rc)
	case C.GRN_NORMALIZER_ERROR:
		return fmt.Sprintf("GRN_NORMALIZER_ERROR (%d)", rc)
	case C.GRN_TOKEN_FILTER_ERROR:
		return fmt.Sprintf("GRN_TOKEN_FILTER_ERROR (%d)", rc)
	case C.GRN_COMMAND_ERROR:
		return fmt.Sprintf("GRN_COMMAND_ERROR (%d)", rc)
	case C.GRN_PLUGIN_ERROR:
		return fmt.Sprintf("GRN_PLUGIN_ERROR (%d)", rc)
	case C.GRN_SCORER_ERROR:
		return fmt.Sprintf("GRN_SCORER_ERROR (%d)", rc)
	default:
		return fmt.Sprintf("GRN_UNDEFINED_ERROR (%d)", rc)
	}
}

//
// Utility
//

// checkTableName() checks whether a string is valid as a table name.
func checkTableName(s string) error {
	if len(s) == 0 {
		return fmt.Errorf("table name must not be empty")
	}
	if s[0] == '_' {
		return fmt.Errorf("table name must not start with '_'")
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= 'a') && (s[i] <= 'z')) &&
			!((s[i] >= 'A') && (s[i] <= 'Z')) &&
			!((s[i] >= '0') && (s[i] <= '9')) &&
			(s[i] != '#') && (s[i] != '@') && (s[i] != '-') && (s[i] != '_') {
			return fmt.Errorf("table name must not contain \\x%X", s[i])
		}
	}
	return nil
}

// checkColumnName() checks whether a string is valid as a column name.
func checkColumnName(s string) error {
	if len(s) == 0 {
		return fmt.Errorf("column name must not be empty")
	}
	for i := 0; i < len(s); i++ {
		if !((s[i] >= 'a') && (s[i] <= 'z')) &&
			!((s[i] >= 'A') && (s[i] <= 'Z')) &&
			!((s[i] >= '0') && (s[i] <= '9')) &&
			(s[i] != '#') && (s[i] != '@') && (s[i] != '-') && (s[i] != '_') {
			return fmt.Errorf("column name must not contain \\x%X", s[i])
		}
	}
	return nil
}

// splitValues() splits a string separated by sep into values.
//
// If s contains only white spaces, splitValues() returns an empty slice.
func splitValues(s string, sep byte) []string {
	var vals []string
	for {
		idx := strings.IndexByte(s, sep)
		if idx == -1 {
			s = strings.TrimSpace(s)
			if (len(vals) != 0) || (len(s) != 0) {
				vals = append(vals, s)
			}
			return vals
		}
		vals = append(vals, strings.TrimSpace(s[:idx]))
		s = s[idx+1:]
	}
}

// getStructType() returns the struct type.
func getStructType(vals interface{}) (reflect.Type, error) {
	typ := reflect.TypeOf(vals)
	switch typ.Kind() {
	case reflect.Ptr:
		typ = typ.Elem()
	case reflect.Slice:
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("unsupported value type")
	}
	return typ, nil
}

//
// Library management
//

// grnCnt is a reference count of the Groonga library.
// Init() increments grnCnt and Fin() decrements grnCnt.
var grnCnt uint32

// refLib() increments grnCnt.
// The Groonga library is initialized if grnCnt changes from 0 to 1.
func refLib() error {
	if grnCnt == math.MaxUint32 {
		return fmt.Errorf("grnCnt overflow")
	}
	if grnCnt == 0 {
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_init() failed: rc = %s", rc)
		}
		C.grnci_init_thread_limit()
	}
	grnCnt++
	return nil
}

// unrefLib() decrements grnCnt.
// The Groonga library is finalized if grnCnt changes from 1 to 0.
func unrefLib() error {
	if grnCnt == 0 {
		return fmt.Errorf("grnCnt underflow")
	}
	grnCnt--
	if grnCnt == 0 {
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_fin() failed: rc = %s", rc)
		}
	}
	return nil
}

//
// DB handle
//

// DB is a handle to a database or a connection to a server.
type DB struct {
	ctx  *C.grn_ctx // Context.
	obj  *C.grn_obj // Database object (handle).
	path string     // Database path (handle).
	host string     // Server host name (connection).
	port int        // Server port number (connection).
}

// newDB() creates an instance of DB.
// The instance must be finalized by DB.fin().
func newDB() (*DB, error) {
	if err := refLib(); err != nil {
		return nil, err
	}
	var db DB
	db.ctx = C.grn_ctx_open(C.int(0))
	if db.ctx == nil {
		unrefLib()
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
		C.grn_obj_unlink(db.ctx, db.obj)
		db.obj = nil
	} else {
		db.host = ""
		db.port = 0
	}
	rc := C.grn_ctx_close(db.ctx)
	db.ctx = nil
	unrefLib()
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

// sendEx() sends a command with separated arguments.
func (db *DB) sendEx(name string, args map[string]string) error {
	if err := checkCmdName(name); err != nil {
		return err
	}
	buf := new(bytes.Buffer)
	if _, err := buf.WriteString(name); err != nil {
		return err
	}
	for key, val := range args {
		if err := checkArgKey(key); err != nil {
			return err
		}
		val = strings.Replace(val, "\\", "\\\\", -1)
		val = strings.Replace(val, "'", "\\'", -1)
		fmt.Fprintf(buf, " --%s '%s'", key, val)
	}
	return db.send(buf.String())
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
func (db *DB) queryEx(name string, args map[string]string) ([]byte, error) {
	if err := db.sendEx(name, args); err != nil {
		res, _ := db.recv()
		return res, err
	}
	return db.recv()
}

//
// Built-in data types
//

// tagKey is the key of a struct field tag that specifies details of the
// associated Groonga column.
const tagKey = "grnci"
const oldTagKey = "groonga"

// tagSep is the separator of a struct field tag value.
const tagSep = ';'

// Bool represents Bool.
type Bool bool

// Int represents Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32 and UInt64.
type Int int64

// Float represents Float.
type Float float64

// Time represents Time.
type Time int64 // The number of microseconds elapsed since the Unix epoch.

// Text represents ShortText, Text and LongText.
type Text string

// Geo represents TokyoGeoPoint and WGS84GeoPoint.
type Geo struct {
	Lat  int32 // Latitude in milliseconds.
	Long int32 // Longitude in milliseconds.
}

var boolType = reflect.TypeOf(Bool(false))
var intType = reflect.TypeOf(Int(0))
var floatType = reflect.TypeOf(Float(0.0))
var timeType = reflect.TypeOf(Time(0))
var textType = reflect.TypeOf(Text(""))
var geoType = reflect.TypeOf(Geo{0, 0})

var vBoolType = reflect.TypeOf([]Bool(nil))
var vIntType = reflect.TypeOf([]Int(nil))
var vFloatType = reflect.TypeOf([]Float(nil))
var vTimeType = reflect.TypeOf([]Time(nil))
var vTextType = reflect.TypeOf([]Text(nil))
var vGeoType = reflect.TypeOf([]Geo(nil))

// writeTo() writes val to buf.
func (val *Bool) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, bool(*val))
	return err
}

// writeTo() writes val to buf.
func (val *Int) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, int64(*val))
	return err
}

// writeTo() writes val to buf.
func (val *Float) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, float64(*val))
	return err
}

// writeTo() writes val to buf.
func (val *Time) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	str := strconv.FormatFloat(float64(*val)/1000000.0, 'f', -1, 64)
	_, err := buf.WriteString(str)
	return err
}

// writeTo() writes val to buf.
func (val *Text) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	str := strings.Replace(string(*val), "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	_, err := fmt.Fprintf(buf, "\"%s\"", str)
	return err
}

// writeTo() writes val to buf.
func (val *Geo) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprintf(buf, "\"%d,%d\"", val.Lat, val.Long)
	return err
}

// MarshalJSON() returns JSON bytes.
func (val Time) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatFloat(float64(val)/1000000.0, 'f', -1, 64)), nil
}

// MarshalJSON() returns JSON bytes.
func (val Geo) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%d,%d\"", val.Lat, val.Long)), nil
}

// UnmarshalJSON() decodes JSON bytes.
func (val *Time) UnmarshalJSON(data []byte) error {
	sec, err := strconv.ParseFloat(string(data), 64)
	if err != nil {
		return err
	}
	*val = Time(int64(sec * 1000000.0))
	return nil
}

// UnmarshalJSON() decodes JSON bytes.
func (val *Geo) UnmarshalJSON(data []byte) error {
	str := string(data)
	if (len(str) < 2) || (str[0] != '"') || (str[len(str)-1] != '"') {
		return fmt.Errorf("Geo must be a string in JSON")
	}
	str = str[1 : len(str)-1]
	idx := strings.IndexAny(str, "x,")
	if idx == -1 {
		return fmt.Errorf("Geo needs a separator 'x' or ',' in JSON")
	}
	if strings.Contains(str, ".") {
		lat, err := strconv.ParseFloat(str[:idx], 64)
		if err != nil {
			return err
		}
		long, err := strconv.ParseFloat(str[idx+1:], 64)
		if err != nil {
			return err
		}
		val.Lat = int32(lat * 60 * 60 * 1000)
		val.Long = int32(long * 60 * 60 * 1000)
	} else {
		lat, err := strconv.ParseInt(str[:idx], 10, 32)
		if err != nil {
			return err
		}
		long, err := strconv.ParseInt(str[idx+1:], 10, 32)
		if err != nil {
			return err
		}
		val.Lat = int32(lat)
		val.Long = int32(long)
	}
	return nil
}

// Now() returns the current time.
func Now() Time {
	now := time.Now()
	return Time((now.Unix() * 1000000) + (now.UnixNano() / 1000))
}

// Unix() returns sec and nsec for time.Unix().
func (val Time) Unix() (sec, nsec int64) {
	sec = int64(val) / 1000000
	nsec = (int64(val) % 1000000) * 1000
	return
}

//
// `table_create`
//

// TableCreateOptions is a set of options for `table_create`.
//
// http://groonga.org/docs/reference/commands/table_create.html
type TableCreateOptions struct {
	Flags            string // --flags
	KeyType          string // --key_type
	ValueType        string // --value_type
	DefaultTokenizer string // --default_tokenizer
	Normalizer       string // --normalizer
	TokenFilters     string // --token_filters
}

// NewTableCreateOptions() returns the default options.
func NewTableCreateOptions() *TableCreateOptions {
	options := new(TableCreateOptions)
	return options
}

// TableCreate() executes `table_create`.
//
// If options is nil, TableCreate() uses the default options.
//
// If options.Flags does not contain TABLE_NO_KEY and options.KeyType is empty,
// TABLE_NO_KEY is automatically added to options.Flags.
//
// http://groonga.org/docs/reference/commands/table_create.html
func (db *DB) TableCreate(name string, options *TableCreateOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if err := checkTableName(name); err != nil {
		return err
	}
	if options == nil {
		options = NewTableCreateOptions()
	}
	args := make(map[string]string)
	args["name"] = name
	keyFlag := ""
	if len(options.Flags) != 0 {
		flags := splitValues(options.Flags, '|')
		for _, flag := range flags {
			switch flag {
			case "TABLE_NO_KEY":
				if len(keyFlag) != 0 {
					return fmt.Errorf("TABLE_NO_KEY must not be set with %s", keyFlag)
				}
				if len(options.KeyType) != 0 {
					return fmt.Errorf("TABLE_NO_KEY disallows KeyType")
				}
				keyFlag = flag
			case "TABLE_HASH_KEY", "TABLE_PAT_KEY", "TABLE_DAT_KEY":
				if len(keyFlag) != 0 {
					return fmt.Errorf("%s must not be set with %s", flag, keyFlag)
				}
				if len(options.KeyType) == 0 {
					return fmt.Errorf("%s requires KeyType", flag)
				}
				keyFlag = flag
			}
		}
		args["flags"] = options.Flags
	}
	if len(keyFlag) == 0 {
		if len(options.KeyType) == 0 {
			keyFlag = "TABLE_NO_KEY"
		} else {
			keyFlag = "TABLE_HASH_KEY"
		}
		if len(args["flags"]) == 0 {
			args["flags"] = keyFlag
		} else {
			args["flags"] += "|" + keyFlag
		}
	}
	if len(options.KeyType) != 0 {
		args["key_type"] = options.KeyType
	}
	if len(options.ValueType) != 0 {
		args["value_type"] = options.ValueType
	}
	if len(options.DefaultTokenizer) != 0 {
		args["default_tokenizer"] = options.DefaultTokenizer
	}
	if len(options.Normalizer) != 0 {
		args["normalizer"] = options.Normalizer
	}
	if len(options.TokenFilters) != 0 {
		args["token_filters"] = options.TokenFilters
	}
	res, err := db.queryEx("table_create", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("table_create failed")
	}
	return nil
}

//
// `column_create`
//

// ColumnCreateOptions is a set of options for `column_create`.
//
// `column_create` takes --flags as a required argument but ColumnCreateOptions
// has Flags because users can specify COLUMN_* via an argument typ of
// ColumnCreate().
// This also means that COLUMN_* should not be set manually.
//
// `column_create` takes --source as an option but ColumnCreateOptions does not
// have Source because users can specify --source via an argument typ of
// ColumnCreate().
//
// http://groonga.org/docs/reference/commands/column_create.html
type ColumnCreateOptions struct {
	Flags string // --flags
}

// NewColumnCreateOptions() returns the default options.
func NewColumnCreateOptions() *ColumnCreateOptions {
	options := new(ColumnCreateOptions)
	return options
}

// ColumnCreate() executes `column_create`.
//
// If typ starts with "[]", COLUMN_VECTOR is added to --flags.
// Else if typ contains ".", COLUMN_INDEX is added to --flags.
// In this case, the former part (before '.') is used as --type and the latter
// part (after '.') is used as --source.
// Otherwise, COLUMN_SCALAR is added to --flags.
//
// If options is nil, ColumnCreate() uses the default options.
//
// http://groonga.org/docs/reference/commands/column_create.html
func (db *DB) ColumnCreate(tbl, name, typ string, options *ColumnCreateOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if err := checkTableName(tbl); err != nil {
		return err
	}
	if err := checkColumnName(name); err != nil {
		return err
	}
	typFlag := "COLUMN_SCALAR"
	src := ""
	if strings.HasPrefix(typ, "[]") {
		typFlag = "COLUMN_VECTOR"
		typ = typ[2:]
	} else if idx := strings.IndexByte(typ, '.'); idx != -1 {
		typFlag = "COLUMN_INDEX"
		src = typ[idx+1:]
		typ = typ[:idx]
	}
	if options == nil {
		options = NewColumnCreateOptions()
	}
	args := make(map[string]string)
	args["table"] = tbl
	args["name"] = name
	if len(options.Flags) != 0 {
		args["flags"] = options.Flags
	}
	if len(typFlag) != 0 {
		if len(args["flags"]) == 0 {
			args["flags"] = typFlag
		} else {
			args["flags"] += "|" + typFlag
		}
	}
	args["type"] = typ
	if len(src) != 0 {
		args["source"] = src
	}
	res, err := db.queryEx("column_create", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return db.errorf("column_create failed")
	}
	return nil
}

//
// `load`
//

// LoadOptions is a set of options for `load`.
//
// --input_type is not supported.
//
// http://groonga.org/docs/reference/commands/load.html
type LoadOptions struct {
	Columns  string // --columns
	IfExists string // --ifexists
}

// NewLoadOptions() returns default options.
func NewLoadOptions() *LoadOptions {
	options := new(LoadOptions)
	return options
}

// loadFindTargetFields() scans the struct of vals and extracts fields to be
// used.
func (db *DB) loadFindTargetFields(vals interface{}, options *LoadOptions) ([]int, []string, error) {
	structType, err := getStructType(vals)
	if err != nil {
		return nil, nil, err
	}
	var listed map[string]bool
	if len(options.Columns) != 0 {
		listed = make(map[string]bool)
		names := splitValues(options.Columns, ',')
		for _, name := range names {
			if err := checkColumnName(name); err != nil {
				return nil, nil, err
			}
			listed[name] = true
		}
	}
	ids := make([]int, 0, structType.NumField())
	names := make([]string, 0, structType.NumField())
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if len(field.PkgPath) != 0 {
			continue
		}
		fieldType := field.Type
		isVector := false
		if fieldType.Kind() == reflect.Slice {
			fieldType = fieldType.Elem()
			isVector = true
		}
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		tag := field.Tag.Get(tagKey)
		if len(tag) == 0 {
			tag = field.Tag.Get(oldTagKey)
		}
		switch fieldType {
		case boolType, intType, floatType, timeType, textType, geoType:
		default:
			if len(tag) != 0 {
				return nil, nil, fmt.Errorf("unsupported data type")
			}
			continue
		}
		idx := strings.IndexByte(tag, tagSep)
		if idx == -1 {
			idx = len(tag)
		}
		name := field.Name
		if idx != 0 {
			name = tag[:idx]
		}
		if err := checkColumnName(name); err != nil {
			return nil, nil, err
		}
		if isVector && ((name == "_key") || (name == "_value")) {
			return nil, nil, fmt.Errorf("%s must not be vector", name)
		}
		if (listed != nil) && !listed[name] {
			continue
		}
		ids = append(ids, i)
		names = append(names, name)
	}
	return ids, names, nil
}

// loadGenHead() generates the `load` header.
func (db *DB) loadGenHead(tbl string, vals interface{}, options *LoadOptions, names []string) (string, error) {
	buf := new(bytes.Buffer)
	if _, err := fmt.Fprintf(buf, "load --table '%s'", tbl); err != nil {
		return "", err
	}
	if len(options.IfExists) != 0 {
		val := strings.Replace(options.IfExists, "\\", "\\\\", -1)
		val = strings.Replace(val, "'", "\\'", -1)
		if _, err := fmt.Fprintf(buf, " --ifexists '%s'", val); err != nil {
			return "", err
		}
	}
	if len(names) != 0 {
		if _, err := buf.WriteString(" --columns '"); err != nil {
			return "", err
		}
		for i, name := range names {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return "", err
				}
			}
			if _, err := buf.WriteString(name); err != nil {
				return "", err
			}
		}
		if err := buf.WriteByte('\''); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

// loadWriteScalar() writes a scalar value.
func (db *DB) loadWriteScalar(buf *bytes.Buffer, any interface{}) error {
	switch val := any.(type) {
	case Bool:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case Int:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case Float:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case Time:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case Text:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case Geo:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case *Bool:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case *Int:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case *Float:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case *Time:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case *Text:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	case *Geo:
		if err := val.writeTo(buf); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported data type")
	}
	return nil
}

// loadWriteVector() writes a vector value.
func (db *DB) loadWriteVector(buf *bytes.Buffer, any interface{}) error {
	if err := buf.WriteByte('['); err != nil {
		return err
	}
	switch vals := any.(type) {
	case []Bool:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []Int:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []Float:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []Time:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []Text:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []Geo:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []*Bool:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []*Int:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []*Float:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []*Time:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []*Text:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	case []*Geo:
		if err := vals[0].writeTo(buf); err != nil {
			return err
		}
		for i := 1; i < len(vals); i++ {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
			if err := vals[i].writeTo(buf); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported data type")
	}
	if err := buf.WriteByte(']'); err != nil {
		return err
	}
	return nil
}

// loadWriteValue() writes a value.
func (db *DB) loadWriteValue(buf *bytes.Buffer, val *reflect.Value, ids []int) error {
	if err := buf.WriteByte('['); err != nil {
		return err
	}
	for i, fieldId := range ids {
		if i != 0 {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
		}
		field := val.Field(fieldId)
		switch field.Kind() {
		case reflect.Slice:
			if field.Len() == 0 {
				if _, err := buf.WriteString("[]"); err != nil {
					return err
				}
				break
			}
			if err := db.loadWriteVector(buf, field.Interface()); err != nil {
				return err
			}
		default:
			if err := db.loadWriteScalar(buf, field.Interface()); err != nil {
				return err
			}
		}
	}
	if err := buf.WriteByte(']'); err != nil {
		return err
	}
	return nil
}

// loadGenBody() generates the `load` body.
func (db *DB) loadGenBody(tbl string, vals interface{}, ids []int) (string, error) {
	buf := new(bytes.Buffer)
	if err := buf.WriteByte('['); err != nil {
		return "", err
	}
	val := reflect.ValueOf(vals)
	switch val.Kind() {
	case reflect.Struct:
		if err := db.loadWriteValue(buf, &val, ids); err != nil {
			return "", err
		}
	case reflect.Ptr:
		if val.IsNil() {
			return "", fmt.Errorf("vals is nil")
		}
		elem := val.Elem()
		if err := db.loadWriteValue(buf, &elem, ids); err != nil {
			return "", err
		}
	case reflect.Slice:
		for i := 0; i < val.Len(); i++ {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return "", err
				}
			}
			idxVal := val.Index(i)
			if err := db.loadWriteValue(buf, &idxVal, ids); err != nil {
				return "", err
			}
		}
	}
	if err := buf.WriteByte(']'); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// Load() executes `load`.
//
// vals accepts a struct, a pointer to struct and a slice of struct.
// A struct and a pointer to struct are available to load one record.
// A slice of struct is useful to load more than one records.
//
// Exported fields of the struct are handled as column values.
// Bool, Int, Float, Time, Text and Geo are available to represent scalar
// values.
// Note that pointers are available to represent null and slices are available
// to represent vector values.
//
// The field name is used as the column name by default, but if the field has
// a grnci tag, its value before the first ';' is used as the column name.
//
// Load() uses all the acceptable fields.
// If you want to use a subset of the struct, specify --columns with
// options.Columns.
//
// If options is nil, Load() uses the default options.
//
// http://groonga.org/docs/reference/commands/load.html
func (db *DB) Load(tbl string, vals interface{}, options *LoadOptions) (int, error) {
	if err := db.check(); err != nil {
		return 0, err
	}
	if err := checkTableName(tbl); err != nil {
		return 0, err
	}
	if vals == nil {
		return 0, fmt.Errorf("vals is nil")
	}
	if options == nil {
		options = NewLoadOptions()
	}
	ids, names, err := db.loadFindTargetFields(vals, options)
	if err != nil {
		return 0, err
	}
	headCmd, err := db.loadGenHead(tbl, vals, options, names)
	if err != nil {
		return 0, err
	}
	bodyCmd, err := db.loadGenBody(tbl, vals, ids)
	if err != nil {
		return 0, err
	}
	if err := db.send(headCmd); err != nil {
		db.recv()
		return 0, err
	}
	if str, err := db.recv(); err != nil {
		return 0, err
	} else if len(str) != 0 {
		return 0, db.errorf("load failed")
	}
	if err := db.send(bodyCmd); err != nil {
		db.recv()
		return 0, err
	}
	res, err := db.recv()
	if err != nil {
		return 0, err
	}
	cnt, err := strconv.Atoi(string(res))
	if err != nil {
		return 0, err
	}
	return cnt, nil
}

//
// `table_create`, `column_create`, `load`
//

// loadExCreateTable() creates a table.
func (db *DB) loadExCreateTable(tbl string, structType reflect.Type) error {
	options := NewTableCreateOptions()
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if len(field.PkgPath) != 0 {
			continue
		}
		tag := field.Tag.Get(tagKey)
		if len(tag) == 0 {
			tag = field.Tag.Get(oldTagKey)
			if len(tag) == 0 {
				continue
			}
		}
		name := ""
		if idx := strings.IndexByte(tag, tagSep); idx != -1 {
			name = tag[:idx]
		} else {
			name = tag
		}
		fieldType := field.Type
		if fieldType.Kind() == reflect.Ptr {
			fieldType = fieldType.Elem()
		}
		switch name {
		case "_key":
			switch fieldType {
			case boolType:
				options.KeyType = "Bool"
			case intType:
				options.KeyType = "Int64"
			case floatType:
				options.KeyType = "Float"
			case timeType:
				options.KeyType = "Time"
			case textType:
				options.KeyType = "ShortText"
			case geoType:
				options.KeyType = "WGS84GeoPoint"
			default:
				return fmt.Errorf("unsupported key type")
			}
			// grnci:"name;key_type;flags;default_tokenizer;normalizer;token_filters"
			tags := splitValues(tag, tagSep)
			if (len(tags) >= 2) && (len(tags[1]) != 0) {
				options.KeyType = tags[1]
			}
			if (len(tags) >= 3) && (len(tags[2]) != 0) {
				options.Flags = tags[2]
			}
			if (len(tags) >= 4) && (len(tags[3]) != 0) {
				options.DefaultTokenizer = tags[3]
			}
			if (len(tags) >= 5) && (len(tags[4]) != 0) {
				options.Normalizer = tags[4]
			}
			if (len(tags) >= 6) && (len(tags[5]) != 0) {
				options.TokenFilters = tags[5]
			}
		case "_value":
			switch fieldType {
			case boolType:
				options.ValueType = "Bool"
			case intType:
				options.ValueType = "Int64"
			case floatType:
				options.ValueType = "Float"
			case timeType:
				options.ValueType = "Time"
			case geoType:
				options.ValueType = "WGS84GeoPoint"
			default:
				return fmt.Errorf("unsupported value type")
			}
			// grnci:"name;value_type"
			tags := splitValues(tag, tagSep)
			if (len(tags) >= 2) && (len(tags[1]) != 0) {
				options.ValueType = tags[1]
			}
		}
	}
	return db.TableCreate(tbl, options)
}

// loadExCreateColumns() creates columns.
func (db *DB) loadExCreateColumns(tbl string, structType reflect.Type) error {
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if len(field.PkgPath) != 0 {
			continue
		}
		tag := field.Tag.Get(tagKey)
		if len(tag) == 0 {
			tag = field.Tag.Get(oldTagKey)
		}
		idx := strings.IndexByte(tag, tagSep)
		if idx == -1 {
			idx = len(tag)
		}
		name := field.Name
		if idx != 0 {
			name = tag[:idx]
		}
		if (name == "_key") || (name == "_value") {
			continue
		}
		fieldType := field.Type
		switch fieldType.Kind() {
		case reflect.Ptr:
			fieldType = fieldType.Elem()
		case reflect.Slice:
			fieldType = fieldType.Elem()
			if fieldType.Kind() == reflect.Ptr {
				fieldType = fieldType.Elem()
			}
			fieldType = reflect.SliceOf(fieldType)
		}
		typ := ""
		switch fieldType {
		case boolType:
			typ = "Bool"
		case intType:
			typ = "Int64"
		case floatType:
			typ = "Float"
		case timeType:
			typ = "Time"
		case textType:
			typ = "Text"
		case geoType:
			typ = "WGS84GeoPoint"
		case vBoolType:
			typ = "[]Bool"
		case vIntType:
			typ = "[]Int64"
		case vFloatType:
			typ = "[]Float"
		case vTimeType:
			typ = "[]Time"
		case vTextType:
			typ = "[]Text"
		case vGeoType:
			typ = "[]WGS84GeoPoint"
		default:
			if len(tag) != 0 {
				return fmt.Errorf("unsupported data type")
			}
			continue
		}
		if err := checkColumnName(name); err != nil {
			return err
		}
		// grnci:"name;type;flags"
		options := NewColumnCreateOptions()
		tags := splitValues(tag, tagSep)
		if (len(tags) >= 2) && (len(tags[1]) != 0) {
			typ = tags[1]
		}
		if (len(tags) >= 3) && (len(tags[2]) != 0) {
			options.Flags = tags[2]
		}
		if err := db.ColumnCreate(tbl, name, typ, options); err != nil {
			return err
		}
	}
	return nil
}

// LoadEx() executes `table_create`, `column_create` and `load`.
//
// LoadEx() calls TableCreate(), ColumnCreate() and Load().
// See each function for details.
//
// The grnci tag format is as follows:
//
//  - grnci:"_key;key_type;flags;default_tokenizer;normalizer;token_filters"
//  - grnci:"_value;value_type"
//  - grnci:"name;type;flags"
//
// Note that the separator is ';' because some values use ',' as its separator.
func (db *DB) LoadEx(tbl string, vals interface{}, options *LoadOptions) (int, error) {
	if err := db.check(); err != nil {
		return 0, err
	}
	if err := checkTableName(tbl); err != nil {
		return 0, err
	}
	if vals == nil {
		return 0, fmt.Errorf("vals is nil")
	}
	structType, err := getStructType(vals)
	if err != nil {
		return 0, err
	}
	if err := db.loadExCreateTable(tbl, structType); err != nil {
		return 0, err
	}
	if err := db.loadExCreateColumns(tbl, structType); err != nil {
		return 0, err
	}
	return db.Load(tbl, vals, options)
}

//
// `select`
//

// SelectOptions is a set of options for `select`.
//
// --output_columns and --drilldown are not supported.
//
// http://groonga.org/docs/reference/commands/select.html
type SelectOptions struct {
	MatchColumns             string // --match_columns
	Query                    string // --query
	Filter                   string // --filter
	Scorer                   string // --scorer
	Sortby                   string // --sortby
	Offset                   int    // --offset
	Limit                    int    // --limit
	Cache                    bool   // --cache
	MatchEscalationThreshold int    // --match_escalation_threshold
	QueryFlags               string // --query_flags
	QueryExpander            string // --query_expander
	Adjuster                 string // --adjuster
}

// NewSelectOptions() returns the default options.
func NewSelectOptions() *SelectOptions {
	return &SelectOptions{
		Limit: 10,
		Cache: true,
	}
}

// selectFindTargetFields() scans the struct of vals and extracts fields to be
// used.
func (db *DB) selectFindTargetFields(vals interface{}, options *SelectOptions) ([]int, []string, error) {
	typ := reflect.TypeOf(vals)
	if typ.Kind() != reflect.Ptr {
		return nil, nil, fmt.Errorf("unsupported value type")
	}
	typ = typ.Elem()
	if typ.Kind() != reflect.Slice {
		return nil, nil, fmt.Errorf("unsupported value type")
	}
	typ = typ.Elem()
	if typ.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("unsupported value type")
	}
	structType := typ

	ids := make([]int, 0, structType.NumField())
	names := make([]string, 0, structType.NumField())
	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		if len(field.PkgPath) != 0 {
			continue
		}
		fieldType := field.Type
		for {
			if (fieldType.Kind() == reflect.Slice) ||
				(fieldType.Kind() == reflect.Ptr) {
				fieldType = fieldType.Elem()
			} else {
				break
			}
		}
		tag := field.Tag.Get(tagKey)
		if len(tag) == 0 {
			tag = field.Tag.Get(oldTagKey)
		}
		idx := strings.IndexByte(tag, tagSep)
		if idx == -1 {
			idx = len(tag)
		}
		name := field.Name
		if idx != 0 {
			name = tag[:idx]
		}
		ids = append(ids, i)
		names = append(names, name)
	}
	return ids, names, nil
}

// selectParse() parses the result of `select`.
func (db *DB) selectParse(data []byte, vals interface{}, ids []int, names []string) error {
	var raw [][][]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	var nHits int
	if err := json.Unmarshal(raw[0][0][0], &nHits); err != nil {
		return err
	}
	fmt.Println("nHits:", nHits) // for debug

	rawHead := raw[0][1]
	nCols := len(rawHead)
	fmt.Println("nCols:", nCols) // for debug
	nameTypes := make([][]string, nCols)
	for i, msg := range rawHead {
		if err := json.Unmarshal(msg, &nameTypes[i]); err != nil {
			return err
		}
	}
	fmt.Println("nameTypes:", nameTypes) // for debug

	rawBody := raw[0][2:]
	nRecs := len(rawBody)
	fmt.Println("nRecs:", nRecs) // for debug

	// TODO: parse the body.
	return nil
}

// Select() executes `select`.
//
// If options is nil, Select() uses the default options.
//
// Note that Select() is experimental.
//
// http://groonga.org/docs/reference/commands/select.html
func (db *DB) Select(tbl string, vals interface{}, options *SelectOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if err := checkTableName(tbl); err != nil {
		return err
	}
	if options == nil {
		options = NewSelectOptions()
	}
	ids, names, err := db.selectFindTargetFields(vals, options)
	if err != nil {
		return err
	}
	args := make(map[string]string)
	args["table"] = tbl
	args["output_columns"] = strings.Join(names, ",")
	if len(options.MatchColumns) != 0 {
		args["match_columns"] = options.MatchColumns
	}
	if len(options.Query) != 0 {
		args["query"] = options.Query
	}
	if len(options.Filter) != 0 {
		args["filter"] = options.Filter
	}
	if len(options.Scorer) != 0 {
		args["scorer"] = options.Scorer
	}
	if len(options.Sortby) != 0 {
		args["sortby"] = options.Sortby
	}
	if options.Offset != 0 {
		args["offset"] = strconv.Itoa(options.Offset)
	}
	if options.Limit != 10 {
		args["limit"] = strconv.Itoa(options.Limit)
	}
	if !options.Cache {
		args["cache"] = "no"
	}
	if options.MatchEscalationThreshold != 0 {
		args["match_escalation_threshold"] =
			strconv.Itoa(options.MatchEscalationThreshold)
	}
	if len(options.QueryFlags) != 0 {
		args["query_flags"] = options.QueryFlags
	}
	if len(options.QueryExpander) != 0 {
		args["query_expander"] = options.QueryExpander
	}
	if len(options.Adjuster) != 0 {
		args["adjuster"] = options.Adjuster
	}
	str, err := db.queryEx("select", args)
	if err != nil {
		return err
	}
	fmt.Println(str) // for debug
	if err := db.selectParse([]byte(str), vals, ids, names); err != nil {
		return err
	}
	return fmt.Errorf("not implemented yet")
}

//
// `column_remove`
//

// ColumnRemoveOptions is a set of options for `column_remove`.
//
// http://groonga.org/docs/reference/commands/column_remove.html
type ColumnRemoveOptions struct {
}

// NewColumnRemoveOptions() returns the default options.
func NewColumnRemoveOptions() *ColumnRemoveOptions {
	return &ColumnRemoveOptions{}
}

// ColumnRemove() executes `column_remove`.
//
// If options is nil, ColumnRemove() uses the default options.
//
// http://groonga.org/docs/reference/commands/column_remove.html
func (db *DB) ColumnRemove(tbl, name string, options *ColumnRemoveOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if err := checkTableName(tbl); err != nil {
		return err
	}
	if err := checkColumnName(name); err != nil {
		return err
	}
	if options == nil {
		options = NewColumnRemoveOptions()
	}
	args := make(map[string]string)
	args["table"] = tbl
	args["name"] = name
	res, err := db.queryEx("column_remove", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("column_remove failed")
	}
	return nil
}

//
// `column_rename`
//

// ColumnRenameOptions is a set of options for `column_rename`.
//
// http://groonga.org/docs/reference/commands/column_rename.html
type ColumnRenameOptions struct {
}

// NewColumnRenameOptions() returns the default options.
func NewColumnRenameOptions() *ColumnRenameOptions {
	return &ColumnRenameOptions{}
}

// ColumnRename() executes `column_rename`.
//
// If options is nil, ColumnRename() uses the default options.
//
// http://groonga.org/docs/reference/commands/column_rename.html
func (db *DB) ColumnRename(tbl, name, newName string, options *ColumnRenameOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if err := checkTableName(tbl); err != nil {
		return err
	}
	if err := checkColumnName(name); err != nil {
		return err
	}
	if err := checkColumnName(newName); err != nil {
		return err
	}
	if options == nil {
		options = NewColumnRenameOptions()
	}
	args := make(map[string]string)
	args["table"] = tbl
	args["name"] = name
	args["new_name"] = newName
	res, err := db.queryEx("column_rename", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("column_rename failed")
	}
	return nil
}

//
// `table_remove`
//

// TableRemoveOptions is a set of options for `table_remove`.
//
// http://groonga.org/docs/reference/commands/table_remove.html
type TableRemoveOptions struct {
}

// NewTableRemoveOptions() returns the default options.
func NewTableRemoveOptions() *TableRemoveOptions {
	return &TableRemoveOptions{}
}

// TableRemove() executes `table_remove`.
//
// If options is nil, TableRemove() uses the default options.
//
// http://groonga.org/docs/reference/commands/table_remove.html
func (db *DB) TableRemove(name string, options *TableRemoveOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if err := checkTableName(name); err != nil {
		return err
	}
	if options == nil {
		options = NewTableRemoveOptions()
	}
	args := make(map[string]string)
	args["name"] = name
	res, err := db.queryEx("table_remove", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("table_remove failed")
	}
	return nil
}

//
// `table_rename`
//

// TableRenameOptions is a set of options for `table_rename`.
//
// http://groonga.org/docs/reference/commands/table_rename.html
type TableRenameOptions struct {
}

// NewTableRenameOptions() returns the default options.
func NewTableRenameOptions() *TableRenameOptions {
	return &TableRenameOptions{}
}

// TableRename() executes `table_rename`.
//
// If options is nil, TableRename() uses the default options.
//
// http://groonga.org/docs/reference/commands/table_rename.html
func (db *DB) TableRename(name, newName string, options *TableRenameOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if err := checkTableName(name); err != nil {
		return err
	}
	if err := checkTableName(newName); err != nil {
		return err
	}
	if options == nil {
		options = NewTableRenameOptions()
	}
	args := make(map[string]string)
	args["name"] = name
	args["new_name"] = newName
	res, err := db.queryEx("table_rename", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("table_rename failed")
	}
	return nil
}

//
// `object_exist`
//

// ObjectExistOptions is a set of options for `object_exist`.
//
// http://groonga.org/docs/reference/commands/object_exist.html
type ObjectExistOptions struct {
}

// NewObjectExistOptions() returns the default options.
func NewObjectExistOptions() *ObjectExistOptions {
	return &ObjectExistOptions{}
}

// ObjectExist() executes `object_exist`.
//
// If options is nil, ObjectExist() uses the default options.
//
// http://groonga.org/docs/reference/commands/object_exist.html
func (db *DB) ObjectExist(name string, options *ObjectExistOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if options == nil {
		options = NewObjectExistOptions()
	}
	args := make(map[string]string)
	args["name"] = name
	res, err := db.queryEx("object_exist", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("object_exist failed")
	}
	return nil
}

//
// `truncate`
//

// TruncateOptions is a set of options for `truncate`.
//
// http://groonga.org/docs/reference/commands/truncate.html
type TruncateOptions struct {
}

// NewTruncateOptions() returns the default options.
func NewTruncateOptions() *TruncateOptions {
	return &TruncateOptions{}
}

// Truncate() executes `truncate`.
//
// If options is nil, Truncate() uses the default options.
//
// http://groonga.org/docs/reference/commands/truncate.html
func (db *DB) Truncate(name string, options *TruncateOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if options == nil {
		options = NewTruncateOptions()
	}
	args := make(map[string]string)
	args["target_name"] = name
	res, err := db.queryEx("truncate", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("truncate failed")
	}
	return nil
}

//
// `thread_limit`
//

// ThreadLimitOptions is a set of options for `thread_limit`.
//
// http://groonga.org/docs/reference/commands/thread_limit.html
type ThreadLimitOptions struct {
	Max int // --max
}

// NewThreadLimitOptions() returns the default options.
func NewThreadLimitOptions() *ThreadLimitOptions {
	return &ThreadLimitOptions{}
}

// ThreadLimit() executes `thread_limit`.
//
// If options is nil, ThreadLimit() uses the default options.
//
// FIXME: Note that if db is a handle, ThreadLimit() returns 1 even though
// DB.Dup() is used. This is a limitation of grnci.
//
// http://groonga.org/docs/reference/commands/thread_limit.html
func (db *DB) ThreadLimit(options *ThreadLimitOptions) (int, error) {
	if err := db.check(); err != nil {
		return 0, err
	}
	if options == nil {
		options = NewThreadLimitOptions()
	}
	args := make(map[string]string)
	if options.Max > 0 {
		args["max"] = strconv.Itoa(options.Max)
	}
	res, err := db.queryEx("thread_limit", args)
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(string(res))
	if err != nil {
		return 0, err
	}
	if n <= 0 {
		return n, fmt.Errorf("thread_limit failed")
	}
	return n, nil
}

//
// `database_unmap`
//

// DatabaseUnmapOptions is a set of options for `database_unmap`.
//
// http://groonga.org/docs/reference/commands/database_unmap.html
type DatabaseUnmapOptions struct {
}

// NewDatabaseUnmapOptions() returns the default options.
func NewDatabaseUnmapOptions() *DatabaseUnmapOptions {
	return &DatabaseUnmapOptions{}
}

// DatabaseUnmap() executes `database_unmap`.
//
// If options is nil, DatabaseUnmap() uses the default options.
//
// http://groonga.org/docs/reference/commands/database_unmap.html
func (db *DB) DatabaseUnmap(options *DatabaseUnmapOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if options == nil {
		options = NewDatabaseUnmapOptions()
	}
	args := make(map[string]string)
	res, err := db.queryEx("database_unmap", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("database_unmap failed")
	}
	return nil
}

//
// `plugin_register`
//

// PluginRegisterOptions is a set of options for `plugin_register`.
//
// http://groonga.org/docs/reference/commands/plugin_register.html
type PluginRegisterOptions struct {
}

// NewPluginRegisterOptions() returns the default options.
func NewPluginRegisterOptions() *PluginRegisterOptions {
	return &PluginRegisterOptions{}
}

// PluginRegister() executes `plugin_register`.
//
// If options is nil, PluginRegister() uses the default options.
//
// http://groonga.org/docs/reference/commands/plugin_register.html
func (db *DB) PluginRegister(name string, options *PluginRegisterOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if options == nil {
		options = NewPluginRegisterOptions()
	}
	args := make(map[string]string)
	args["name"] = name
	res, err := db.queryEx("plugin_register", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("plugin_register failed")
	}
	return nil
}

//
// `plugin_unregister`
//

// PluginUnregisterOptions is a set of options for `plugin_unregister`.
//
// http://groonga.org/docs/reference/commands/plugin_unregister.html
type PluginUnregisterOptions struct {
}

// NewPluginUnregisterOptions() returns the default options.
func NewPluginUnregisterOptions() *PluginUnregisterOptions {
	return &PluginUnregisterOptions{}
}

// PluginUnregister() executes `plugin_unregister`.
//
// If options is nil, PluginUnregister() uses the default options.
//
// http://groonga.org/docs/reference/commands/plugin_unregister.html
func (db *DB) PluginUnregister(name string, options *PluginUnregisterOptions) error {
	if err := db.check(); err != nil {
		return err
	}
	if options == nil {
		options = NewPluginUnregisterOptions()
	}
	args := make(map[string]string)
	args["name"] = name
	res, err := db.queryEx("plugin_unregister", args)
	if err != nil {
		return err
	}
	if string(res) != "true" {
		return fmt.Errorf("plugin_unregister failed")
	}
	return nil
}
