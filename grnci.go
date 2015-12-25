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
			return fmt.Errorf("grn_init() failed: %d", rc)
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
			return fmt.Errorf("grn_fin() failed: %d", rc)
		}
	}
	return nil
}

//
// DB handle
//

// DB is a handle to a database or a connection to a server.
type DB struct {
	ctx *C.grn_ctx
	obj *C.grn_obj
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
		return nil, fmt.Errorf("grn_ctx_connect() failed: %d", rc)
	}
	return db, nil
}

// Dup() duplicates a handle.
// The handle must be closed by DB.Close().
//
// FIXME: Dup() cannot duplicate a connection.
func (db *DB) Dup() (*DB, error) {
	if db.obj == nil {
		return nil, fmt.Errorf("not a handle to a local DB")
	}
	db, err := newDB()
	if err != nil {
		return nil, err
	}
	if rc := C.grn_ctx_use(db.ctx, db.obj); rc != C.GRN_SUCCESS {
		db.Close()
		return nil, fmt.Errorf("grn_ctx_use() failed")
	}
	return db, nil
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
		return fmt.Errorf("grn_ctx_close() failed: %d", rc)
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
		return fmt.Errorf("grn_ctx_send() failed: %d", rc)
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
		return "", fmt.Errorf("grn_ctx_recv() failed: %d", rc)
	}
	if (resFlags & C.GRN_CTX_MORE) == 0 {
		return C.GoStringN(res, C.int(resLen)), nil
	}
	buf := bytes.NewBuffer(C.GoBytes(unsafe.Pointer(res), C.int(resLen)))
	var bufErr error
	for {
		if rc := C.grn_ctx_recv(db.ctx, &res, &resLen, &resFlags); rc != C.GRN_SUCCESS {
			return "", fmt.Errorf("grn_ctx_recv() failed: %d", rc)
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

// fieldTag specifies the associated Groonga column.
const fieldTag = "groonga"

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

// writeTo() writes `val` to `buf`.
func (val Bool) writeTo(buf *bytes.Buffer) error {
	_, err := fmt.Fprint(buf, bool(val))
	return err
}

// writeTo() writes `val` to `buf`.
func (val Int) writeTo(buf *bytes.Buffer) error {
	_, err := fmt.Fprint(buf, int64(val))
	return err
}

// writeTo() writes `val` to `buf`.
func (val Float) writeTo(buf *bytes.Buffer) error {
	_, err := fmt.Fprint(buf, float64(val))
	return err
}

// writeTo() writes `val` to `buf`.
func (val Time) writeTo(buf *bytes.Buffer) error {
	_, err := fmt.Fprint(buf, float64(int64(val))/1000000.0)
	return err
}

// writeTo() writes `val` to `buf`.
func (val Text) writeTo(buf *bytes.Buffer) error {
	str := strings.Replace(string(val), "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	_, err := fmt.Fprintf(buf, "\"%s\"", str)
	return err
}

// writeTo() writes `val` to `buf`.
func (val Geo) writeTo(buf *bytes.Buffer) error {
	_, err := fmt.Fprintf(buf, "\"%d,%d\"", val.Lat, val.Long)
	return err
}

// Now() returns the current time.
func Now() Time {
	now := time.Now()
	return Time((now.Unix() * 1000000) + (now.UnixNano() / 1000))
}

// Unix() returns `sec` and `nsec` for time.Unix().
func (time Time) Unix() (int64, int64) {
	return int64(time / 1000000), int64((time % 1000000) * 1000)
}

//
// The `load` command
//

// genLoadHead() generates a head of `load`.
func (db *DB) genLoadHead(tbl string, vals interface{}, options *LoadOptions) (string, error) {
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

// writeLoadScalar() writes a scalar of `load`.
func (db *DB) writeLoadScalar(buf *bytes.Buffer, any interface{}) error {
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
	default:
		return fmt.Errorf("unsupported data type")
	}
	return nil
}

// writeLoadScalar() writes a vector of `load`.
func (db *DB) writeLoadVector(buf *bytes.Buffer, any interface{}) error {
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
		for i, val := range vals {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprint(buf, float64(val)); err != nil {
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
	default:
		return fmt.Errorf("unsupported data type")
	}
	if err := buf.WriteByte(']'); err != nil {
		return err
	}
	return nil
}

// writeLoadValue() writes a value of `load`.
func (db *DB) writeLoadValue(buf *bytes.Buffer, val *reflect.Value, options *LoadOptions) error {
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
			} else {
				if err := db.writeLoadVector(buf, field.Interface()); err != nil {
					return err
				}
			}
		default:
			if err := db.writeLoadScalar(buf, field.Interface()); err != nil {
				return err
			}
		}
	}
	if err := buf.WriteByte(']'); err != nil {
		return err
	}
	return nil
}

// genLoadHead() generates a body of `load`.
func (db *DB) genLoadBody(tbl string, vals interface{}, options *LoadOptions) (string, error) {
	buf := new(bytes.Buffer)
	if err := buf.WriteByte('['); err != nil {
		return "", err
	}
	val := reflect.ValueOf(vals)
	switch val.Kind() {
	case reflect.Struct:
		if err := db.writeLoadValue(buf, &val, options); err != nil {
			return "", err
		}
	case reflect.Ptr:
		elem := val.Elem()
		if err := db.writeLoadValue(buf, &elem, options); err != nil {
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
			if err := db.writeLoadValue(buf, &idxVal, options); err != nil {
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

// scanFields() scans fields.
func (db *DB) scanLoadFields(vals interface{}, options *LoadOptions) error {
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
		colName := field.Tag.Get(fieldTag)
		if len(colName) == 0 {
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
	if err := db.scanLoadFields(vals, options); err != nil {
		return 0, err
	}
	headCmd, err := db.genLoadHead(tbl, vals, options)
	if err != nil {
		return 0, err
	}
	fmt.Println(headCmd) // FIXME: For debug.
	bodyCmd, err := db.genLoadBody(tbl, vals, options)
	if err != nil {
		return 0, err
	}
	fmt.Println(bodyCmd) // FIXME: For debug.
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
	fmt.Println(str) // FIXME: For debug.
	cnt, err := strconv.Atoi(str)
	if err != nil {
		return 0, err
	}
	return cnt, nil
}
