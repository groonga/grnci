// Groonga Command Interface (Test ver.)
package grnci

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"

import (
	"bytes"
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
// Library management
//

// grnCnt is a reference count of the Groonga library.
// Init() increments `grnCnt` and Fin() decrements `grnCnt`.
var grnCnt uint32

// refLib() increments `grnCtx`.
// The Groonga library is initialized if `grnCtx` changes from 0 to 1.
func refLib() error {
	if grnCnt == math.MaxUint32 {
		return fmt.Errorf("grnCnt overflow")
	}
	if grnCnt == 0 {
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_init() failed: %s", rc)
		}
	}
	grnCnt++
	return nil
}

// unrefLib() decrements `grnCtx`.
// The Groonga library is finalized if `grnCtx` changes from 1 to 0.
func unrefLib() error {
	if grnCnt == 0 {
		return fmt.Errorf("grnCnt underflow")
	}
	grnCnt--
	if grnCnt == 0 {
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("grn_fin() failed: %s", rc)
		}
	}
	return nil
}

//
// DB handle
//

// DB is a handle to a database or a connection to a server.
type DB struct {
	ctx  *C.grn_ctx
	obj  *C.grn_obj
	host string
	port int
}

// newDB() creates an instance of DB.
// The instance must be closed by DB.Close().
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
		db.Close()
		return nil, fmt.Errorf("grn_db_create() failed")
	}
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
		db.Close()
		return nil, fmt.Errorf("grn_db_open() failed")
	}
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
		db.Close()
		return nil, fmt.Errorf("grn_ctx_connect() failed: %s", rc)
	}
	db.host = host
	db.port = port
	return db, nil
}

// Dup() duplicates a handle or a connection.
// The handle must be closed by DB.Close().
func (db *DB) Dup() (*DB, error) {
	if db.obj == nil {
		return Connect(db.host, db.port)
	}
	dupDB, err := newDB()
	if err != nil {
		return nil, err
	}
	if rc := C.grn_ctx_use(dupDB.ctx, db.obj); rc != C.GRN_SUCCESS {
		dupDB.Close()
		return nil, fmt.Errorf("grn_ctx_use() failed: %s", rc)
	}
	return dupDB, nil
}

// Close() closes a handle or a connection.
func (db *DB) Close() error {
	if db.ctx == nil {
		return fmt.Errorf("ctx is nil")
	}
	if db.obj != nil {
		C.grn_obj_unlink(db.ctx, db.obj)
		db.obj = nil
	}
	rc := C.grn_ctx_close(db.ctx)
	db.ctx = nil
	unrefLib()
	if rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ctx_close() failed: %s", rc)
	}
	return nil
}

//
// Low-level command interface
//

// send() sends a command.
func (db *DB) send(cmd string) error {
	if len(cmd) == 0 {
		return fmt.Errorf("cmd is empty")
	}
	cCmd := []byte(cmd)
	if rc := C.grn_ctx_send(db.ctx, (*C.char)(unsafe.Pointer(&cCmd[0])),
		C.uint(len(cCmd)), C.int(0)); rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ctx_send() failed: %s", rc)
	}
	return nil
}

// sendEx() sends a command with separated options.
func (db *DB) sendEx(name string, options map[string]string) error {
	if len(name) == 0 {
		return fmt.Errorf("name is empty")
	}
	buf := new(bytes.Buffer)
	if _, err := buf.WriteString(name); err != nil {
		return err
	}
	for key, val := range options {
		if len(key) == 0 {
			return fmt.Errorf("key is empty")
		}
		val = strings.Replace(val, "\\", "\\\\", -1)
		val = strings.Replace(val, "'", "\\'", -1)
		fmt.Fprintf(buf, " --%s '%s'", key, val)
	}
	return db.send(buf.String())
}

// recv() receives the result of a command sent by send().
func (db *DB) recv() (string, error) {
	var res *C.char
	var resLen C.uint
	var resFlags C.int
	if rc := C.grn_ctx_recv(db.ctx, &res, &resLen, &resFlags); rc != C.GRN_SUCCESS {
		return "", fmt.Errorf("grn_ctx_recv() failed: %s", rc)
	}
	if (resFlags & C.GRN_CTX_MORE) == 0 {
		return C.GoStringN(res, C.int(resLen)), nil
	}
	buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(res), C.int(resLen)))
	var bufErr error
	for {
		if rc := C.grn_ctx_recv(db.ctx, &res, &resLen, &resFlags); rc != C.GRN_SUCCESS {
			return "", fmt.Errorf("grn_ctx_recv() failed: %s", rc)
		}
		if bufErr == nil {
			_, bufErr = buf.Write(C.GoBytes(unsafe.Pointer(res), C.int(resLen)))
		}
		if (resFlags & C.GRN_CTX_MORE) == 0 {
			break
		}
	}
	if bufErr != nil {
		return "", bufErr
	}
	return buf.String(), nil
}

// query() executes a command.
func (db *DB) query(cmd string) (string, error) {
	if err := db.send(cmd); err != nil {
		str, _ := db.recv()
		return str, err
	}
	return db.recv()
}

// qureyEx() executes a command with separated options.
func (db *DB) queryEx(name string, options map[string]string) (string, error) {
	if err := db.sendEx(name, options); err != nil {
		bytes, _ := db.recv()
		return bytes, err
	}
	return db.recv()
}

//
// Built-in data types
//

// tagKey specifies the associated Groonga column.
const tagKey = "groonga"

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
	Lat  int32
	Long int32
}

var BoolType = reflect.TypeOf(Bool(false))
var IntType = reflect.TypeOf(Int(0))
var FloatType = reflect.TypeOf(Float(0.0))
var TimeType = reflect.TypeOf(Time(0))
var TextType = reflect.TypeOf(Text(""))
var GeoType = reflect.TypeOf(Geo{0, 0})

// writeTo() writes `val` to `buf`.
func (val *Bool) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, bool(*val))
	return err
}

// writeTo() writes `val` to `buf`.
func (val *Int) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, int64(*val))
	return err
}

// writeTo() writes `val` to `buf`.
func (val *Float) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, float64(*val))
	return err
}

// writeTo() writes `val` to `buf`.
func (val *Time) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, float64(int64(*val))/1000000.0)
	return err
}

// writeTo() writes `val` to `buf`.
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

// writeTo() writes `val` to `buf`.
func (val *Geo) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprintf(buf, "\"%d,%d\"", val.Lat, val.Long)
	return err
}

// Now() returns the current time.
func Now() Time {
	now := time.Now()
	return Time((now.Unix() * 1000000) + (now.UnixNano() / 1000))
}

// Unix() returns `sec` and `nsec` for time.Unix().
func (time Time) Unix() (sec, nsec int64) {
	sec = int64(time) / 1000000
	nsec = (int64(time) % 1000000) * 1000
	return
}

//
// `table_create`
//

type TableCreateOptions struct {
	Flags            string
	KeyType          string
	ValueType        string
	DefaultTokenizer string
	Normalizer       string
	TokenFilters     string
}

func (db *DB) TableCreate(name string, options *TableCreateOptions) error {
	return nil
}

//
// `load`
//

// loadGenHead() generates a head of `load`.
func (db *DB) loadGenHead(tbl string, vals interface{}, options *LoadOptions) (string, error) {
	buf := new(bytes.Buffer)
	if _, err := fmt.Fprintf(buf, "load --table %s", tbl); err != nil {
		return "", err
	}
	if len(options.IfExists) != 0 {
		val := strings.Replace(options.IfExists, "\\", "\\\\", -1)
		val = strings.Replace(val, "'", "\\'", -1)
		if _, err := fmt.Fprintf(buf, " --ifexists '%s'", val); err != nil {
			return "", err
		}
	}
	if len(options.colNames) != 0 {
		if _, err := buf.WriteString(" --columns '"); err != nil {
			return "", err
		}
		for i, colName := range options.colNames {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return "", err
				}
			}
			if _, err := buf.WriteString(colName); err != nil {
				return "", err
			}
		}
		if err := buf.WriteByte('\''); err != nil {
			return "", err
		}
	}
	return buf.String(), nil
}

// loadWriteScalar() writes a scalar of `load`.
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

// loadWriteScalar() writes a vector of `load`.
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

// loadWriteValue() writes a value of `load`.
func (db *DB) loadWriteValue(buf *bytes.Buffer, val *reflect.Value, options *LoadOptions) error {
	if err := buf.WriteByte('['); err != nil {
		return err
	}
	for i, fieldId := range options.fieldIds {
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

// loadGenBody() generates a body of `load`.
func (db *DB) loadGenBody(tbl string, vals interface{}, options *LoadOptions) (string, error) {
	buf := new(bytes.Buffer)
	if err := buf.WriteByte('['); err != nil {
		return "", err
	}
	val := reflect.ValueOf(vals)
	switch val.Kind() {
	case reflect.Struct:
		if err := db.loadWriteValue(buf, &val, options); err != nil {
			return "", err
		}
	case reflect.Ptr:
		elem := val.Elem()
		if err := db.loadWriteValue(buf, &elem, options); err != nil {
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
			if err := db.loadWriteValue(buf, &idxVal, options); err != nil {
				return "", err
			}
		}
	}
	if err := buf.WriteByte(']'); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// LoadOptions is a set of options for `load`.
type LoadOptions struct {
	Columns  string
	IfExists string

	fieldIds []int    // Target field IDs.
	colNames []string // Target column names.
}

// NewLoadOptions() returns a LoadOptions with the default settings.
func NewLoadOptions() *LoadOptions {
	options := new(LoadOptions)
	return options
}

// loadScanFields() scans fields.
func (db *DB) loadScanFields(vals interface{}, options *LoadOptions) error {
	valType := reflect.TypeOf(vals)
	switch valType.Kind() {
	case reflect.Ptr:
		valType = valType.Elem()
	case reflect.Slice:
		valType = valType.Elem()
	}
	if valType.Kind() != reflect.Struct {
		return fmt.Errorf("unsupported value type")
	}
	var listed map[string]bool
	if len(options.Columns) != 0 {
		listed = make(map[string]bool)
		colNames := strings.Split(options.Columns, ",")
		for _, colName := range colNames {
			listed[colName] = true
		}
	}
	fieldIds := make([]int, 0, valType.NumField())
	colNames := make([]string, 0, valType.NumField())
	for i := 0; i < valType.NumField(); i++ {
		field := valType.Field(i)
		fieldType := field.Type
		switch fieldType.Kind() {
		case reflect.Ptr:
			fieldType = fieldType.Elem()
		case reflect.Slice:
			fieldType = fieldType.Elem()
			if fieldType.Kind() == reflect.Ptr {
				fieldType = fieldType.Elem()
			}
		}
		colName := field.Name
		tagValue := field.Tag.Get(tagKey)
		switch fieldType {
		case BoolType, IntType, FloatType, TimeType, TextType, GeoType:
			if len(tagValue) != 0 {
				colName = tagValue
			}
		default:
			if len(tagValue) != 0 {
				return fmt.Errorf("unsupported data type")
			}
			continue
		}
		if (listed != nil) && !listed[colName] {
			continue
		}
		fieldIds = append(fieldIds, i)
		colNames = append(colNames, colName)
	}
	options.fieldIds = fieldIds
	options.colNames = colNames
	return nil
}

// Load() executes `load`.
func (db *DB) Load(tbl string, vals interface{}, options *LoadOptions) (int, error) {
	if options == nil {
		options = NewLoadOptions()
	}
	if err := db.loadScanFields(vals, options); err != nil {
		return 0, err
	}
	headCmd, err := db.loadGenHead(tbl, vals, options)
	if err != nil {
		return 0, err
	}
	bodyCmd, err := db.loadGenBody(tbl, vals, options)
	if err != nil {
		return 0, err
	}
	if err := db.send(headCmd); err != nil {
		db.recv()
		return 0, err
	}
	if _, err := db.recv(); err != nil {
		return 0, err
	}
	if err := db.send(bodyCmd); err != nil {
		db.recv()
		return 0, err
	}
	str, err := db.recv()
	if err != nil {
		return 0, err
	}
	cnt, err := strconv.Atoi(str)
	if err != nil {
		return 0, err
	}
	return cnt, nil
}
