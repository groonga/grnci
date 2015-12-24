// Groonga Command Interface (Test ver.)
package grnci

// #cgo pkg-config: groonga
// #include <groonga.h>
import "C"

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unsafe"
)

// grnCnt is a reference count of the Groonga library.
// Init() increments `grnCnt` and Fin() decrements `grnCnt`.
var grnCnt uint32

// DB is a DB handle.
type DB struct {
	ctx *C.grn_ctx
	obj *C.grn_obj
}

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

// Create() creates a DB and returns its handle.
func Create(path string) (*DB, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("path is empty")
	}
	if err := refLib(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(C.int(0))
	if ctx == nil {
		unrefLib()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	cPath := []byte(path)
	obj := C.grn_db_create(ctx, (*C.char)(unsafe.Pointer(&cPath[0])), nil)
	if obj == nil {
		C.grn_ctx_close(ctx)
		unrefLib()
		return nil, fmt.Errorf("grn_db_create() failed")
	}
	return &DB{ctx, obj}, nil
}

// Open() opens a DB and returns its handle.
func Open(path string) (*DB, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("path is empty")
	}
	if err := refLib(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(C.int(0))
	if ctx == nil {
		unrefLib()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	cPath := []byte(path)
	obj := C.grn_db_open(ctx, (*C.char)(unsafe.Pointer(&cPath[0])))
	if obj == nil {
		C.grn_ctx_close(ctx)
		unrefLib()
		return nil, fmt.Errorf("grn_db_open() failed")
	}
	return &DB{ctx, obj}, nil
}

// Dup() duplicates a DB handle.
func (db *DB) Dup() (*DB, error) {
	if err := refLib(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(C.int(0))
	if ctx == nil {
		unrefLib()
		return nil, fmt.Errorf("grn_ctx_open() failed")
	}
	if rc := C.grn_ctx_use(ctx, db.obj); rc != C.GRN_SUCCESS {
		C.grn_ctx_close(ctx)
		unrefLib()
		return nil, fmt.Errorf("grn_ctx_use() failed")
	}
	return &DB{ctx, db.obj}, nil
}

// Close() closes a DB handle.
func (db *DB) Close() error {
	C.grn_obj_unlink(db.ctx, db.obj)
	rc := C.grn_ctx_close(db.ctx)
	unrefLib()
	if rc != C.GRN_SUCCESS {
		return fmt.Errorf("grn_ctx_close() failed: %d", rc)
	}
	return nil
}

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
func (db *DB) recv() ([]byte, error) {
	var res *C.char
	var resLen C.uint
	if rc := C.grn_ctx_recv(db.ctx, &res, &resLen, nil); rc != C.GRN_SUCCESS {
		return nil, fmt.Errorf("grn_ctx_recv() failed: %d", rc)
	}
	return C.GoBytes(unsafe.Pointer(res), C.int(resLen)), nil
}

// query() executes a command.
func (db *DB) query(cmd string) ([]byte, error) {
	if err := db.send(cmd); err != nil {
		bytes, _ := db.recv()
		return bytes, err
	}
	return db.recv()
}

// qureyEx() executes a command with separated options.
func (db *DB) queryEx(name string, options map[string]string) ([]byte, error) {
	if err := db.sendEx(name, options); err != nil {
		bytes, _ := db.recv()
		return bytes, err
	}
	return db.recv()
}

// Bool.
type Bool bool

// Int8, Int16, Int32 and Int64.
// UInt8, UInt16, UInt32 and UInt64.
type Int int64

// Float.
type Float float64

// Time.
type Time int64

// ShortText, Text and LongText.
type Text string

// TokyoGeoPoint and WGS84GeoPoint.
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

var VBoolType = reflect.TypeOf([]Bool{})
var VIntType = reflect.TypeOf([]Int{})
var VFloatType = reflect.TypeOf([]Float{})
var VTimeType = reflect.TypeOf([]Time{})
var VTextType = reflect.TypeOf([]Text{})
var VGeoType = reflect.TypeOf([]Geo{})

const fieldTag = "groonga"

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
	valType := reflect.TypeOf(vals)
	switch valType.Kind() {
	case reflect.Ptr:
		valType = valType.Elem()
	case reflect.Slice:
		valType = valType.Elem()
	}
	if valType.Kind() != reflect.Struct {
		return "", fmt.Errorf("unsupported value type")
	}
	nCols := 0
	for i := 0; i < valType.NumField(); i++ {
		field := valType.Field(i)
		col := field.Tag.Get(fieldTag)
		if len(col) == 0 {
			continue
		}
		if nCols == 0 {
			if _, err := fmt.Fprintf(buf, " --columns '%s", col); err != nil {
				return "", err
			}
		} else {
			if _, err := fmt.Fprintf(buf, ",%s", col); err != nil {
				return "", err
			}
		}
		nCols++
	}
	if nCols == 0 {
		return "", fmt.Errorf("no valid fields")
	}
	if err := buf.WriteByte('\''); err != nil {
		return "", nil
	}
	return buf.String(), nil
}

// writeLoadScalar() writes a scalar of `load`.
func (db *DB) writeLoadScalar(buf *bytes.Buffer, any interface{}) error {
	switch val := any.(type) {
	case Bool:
		if _, err := fmt.Fprint(buf, val); err != nil {
			return err
		}
	case Int:
		if _, err := fmt.Fprint(buf, val); err != nil {
			return err
		}
	case Float:
		if _, err := fmt.Fprint(buf, val); err != nil {
			return err
		}
	case Time:
		if _, err := fmt.Fprint(buf, float64(val)/1000000.0); err != nil {
			return err
		}
	case Text:
		str := strings.Replace(string(val), "\\", "\\\\", -1)
		str = strings.Replace(str, "\"", "\\\"", -1)
		if _, err := fmt.Fprintf(buf, "\"%s\"", str); err != nil {
			return err
		}
	case Geo:
		if _, err := fmt.Fprintf(buf, "\"%d,%d\"", val.Lat, val.Long); err != nil {
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
		for i, val := range vals {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprint(buf, val); err != nil {
				return err
			}
		}
	case []Int:
		for i, val := range vals {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprint(buf, val); err != nil {
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
			if _, err := fmt.Fprint(buf, val); err != nil {
				return err
			}
		}
	case []Time:
		for i, val := range vals {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprint(buf, float64(val)/1000000.0); err != nil {
				return err
			}
		}
	case []Text:
		for i, val := range vals {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return err
				}
			}
			str := strings.Replace(string(val), "\\", "\\\\", -1)
			str = strings.Replace(str, "\"", "\\\"", -1)
			if _, err := fmt.Fprintf(buf, "\"%s\"", str); err != nil {
				return err
			}
		}
	case []Geo:
		for i, val := range vals {
			if i != 0 {
				if err := buf.WriteByte(','); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(buf, "\"%d,%d\"", val.Lat, val.Long); err != nil {
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
func (db *DB) writeLoadValue(buf *bytes.Buffer, val *reflect.Value) error {
	if err := buf.WriteByte('['); err != nil {
		return err
	}
	valType := val.Type()
	nCols := 0
	for i := 0; i < valType.NumField(); i++ {
		field := valType.Field(i)
		if len(field.Tag.Get(fieldTag)) == 0 {
			continue
		}
		if nCols != 0 {
			if err := buf.WriteByte(','); err != nil {
				return err
			}
		}
		fieldVal := val.Field(i)
		if fieldVal.Kind() != reflect.Slice {
			if err := db.writeLoadScalar(buf, fieldVal.Interface()); err != nil {
				return err
			}
		} else {
			if err := db.writeLoadVector(buf, fieldVal.Interface()); err != nil {
				return err
			}
		}
		nCols++
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
		if err := db.writeLoadValue(buf, &val); err != nil {
			return "", err
		}
	case reflect.Ptr:
		elem := val.Elem()
		if err := db.writeLoadValue(buf, &elem); err != nil {
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
			if err := db.writeLoadValue(buf, &idxVal); err != nil {
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
	IfExists string
}

// NewLoadOptions() returns a LoadOptions with the default settings.
func NewLoadOptions() *LoadOptions {
	options := new(LoadOptions)
	return options
}

// Load() loads values.
func (db *DB) Load(tbl string, vals interface{}, options *LoadOptions) (int, error) {
	if options == nil {
		options = NewLoadOptions()
	}
	headCmd, err := db.genLoadHead(tbl, vals, options)
	if err != nil {
		return 0, err
	}
	// FIXME: For debug.
	fmt.Println(headCmd)
	bodyCmd, err := db.genLoadBody(tbl, vals, options)
	if err != nil {
		return 0, err
	}
	// FIXME: For debug.
	fmt.Println(bodyCmd)
	if err := db.send(headCmd); err != nil {
		db.recv()
		return 0, err
	}
	if err := db.send(bodyCmd); err != nil {
		db.recv()
		return 0, err
	}
	bytes, err := db.recv()
	if err != nil {
		return 0, err
	}
	cnt, err := strconv.Atoi(string(bytes))
	if err != nil {
		return 0, err
	}
	return cnt, nil
}
