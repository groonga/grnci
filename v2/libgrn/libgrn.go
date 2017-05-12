// Package libgrn provides Client using libgroonga.
package libgrn

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"
import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

var (
	// libCount is incremented by Init and decremented by Fin.
	libCount int
	// libMutex is used for exclusion control in Init and Fin.
	libMutex sync.Mutex
)

// Init initializes libgroonga.
//
// If an internal counter is zero, Init increments it and initializes libgroonga.
// Otherwise, Init only increments the internal counter.
//
// There is no need to call Init explicitly.
// libgrn calls Init when it creates a Client.
func Init() error {
	libMutex.Lock()
	defer libMutex.Unlock()
	if libCount == 0 {
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("C.grn_init failed: rc = %d", rc)
		}
	}
	libCount++
	return nil
}

// Fin finalizes libgroonga.
//
// If an internal counter is one, Fin decrements it and finalizes libgroonga.
// Otherwise, Fin only decrements the internal counter.
//
// There is no need to call Fin explicitly.
// libgrn calls Fin when it closes a Client.
func Fin() error {
	libMutex.Lock()
	defer libMutex.Unlock()
	if libCount == 0 {
		return fmt.Errorf("libCount = 0")
	}
	libCount--
	if libCount == 0 {
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return fmt.Errorf("C.grn_fin failed: rc = %d", rc)
		}
	}
	return nil
}

// ctx is a Groonga context.
type grnCtx struct {
	ctx *C.grn_ctx
}

// newGrnCtx returns a new grnCtx.
func newGrnCtx() (*grnCtx, error) {
	if err := Init(); err != nil {
		return nil, fmt.Errorf("Init failed: %v", err)
	}
	ctx := C.grn_ctx_open(C.int(0))
	if ctx == nil {
		Fin()
		return nil, errors.New("C.grn_ctx_open failed")
	}
	return &grnCtx{ctx: ctx}, nil
}

// Close closes a grnCtx.
func (c *grnCtx) Close() error {
	if rc := C.grn_ctx_close(c.ctx); rc != C.GRN_SUCCESS {
		return fmt.Errorf("C.grn_ctx_close failed: %s", rc)
	}
	if err := Fin(); err != nil {
		return fmt.Errorf("Fin failed: %v", err)
	}
	return nil
}

func (c *grnCtx) Err() error {
	if c.ctx.rc == C.GRN_SUCCESS {
		return nil
	}
	return fmt.Errorf("rc = %s: %s", c.ctx.rc, C.GoString(&c.ctx.errbuf[0]))
}

// grnDB is a DB handle.
type grnDB struct {
	obj   *C.grn_obj
	path  string
	count int
	mutex sync.Mutex
}

// createDB creates a new DB.
func createDB(ctx *grnCtx, path string) (*grnDB, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	obj := C.grn_db_create(ctx.ctx, cPath, nil)
	if obj == nil {
		return nil, fmt.Errorf("C.grn_db_create failed: %v", ctx.Err())
	}
	if cAbsPath := C.grn_obj_path(ctx.ctx, obj); cAbsPath != nil {
		path = C.GoString(cAbsPath)
	}
	return &grnDB{
		obj:   obj,
		path:  path,
		count: 1,
	}, nil
}

// openDB opens an existing DB.
func openDB(ctx *grnCtx, path string) (*grnDB, error) {
	if ctx == nil {
		return nil, errors.New("invalid argument: ctx = nil")
	}
	if path == "" {
		return nil, errors.New("invalid argument: path = ")
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	obj := C.grn_db_open(ctx.ctx, cPath)
	if obj == nil {
		return nil, fmt.Errorf("C.grn_db_create failed: %v", ctx.Err())
	}
	if cAbsPath := C.grn_obj_path(ctx.ctx, obj); cAbsPath != nil {
		path = C.GoString(cAbsPath)
	}
	return &grnDB{
		obj:   obj,
		path:  path,
		count: 1,
	}, nil
}

// Close closes a DB.
func (db *grnDB) Close(ctx *grnCtx) error {
	if db == nil {
		return errors.New("invalid self: db = nil")
	}
	if db.obj == nil {
		return errors.New("invalid self: obj = nil")
	}
	if ctx == nil {
		return errors.New("invalid argument: ctx = nil")
	}
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if db.count <= 0 {
		return fmt.Errorf("underflow: cnt = %d", db.count)
	}
	db.count--
	if db.count == 0 {
		if rc := C.grn_obj_close(ctx.ctx, db.obj); rc != C.GRN_SUCCESS {
			return fmt.Errorf("C.grn_obj_close failed: rc = %s", rc)
		}
		db.obj = nil
	}
	return nil
}

// Dup duplicates a DB handle.
func (db *grnDB) Dup() (*grnCtx, error) {
	if db == nil {
		return nil, errors.New("invalid self: db = nil")
	}
	if db.obj == nil {
		return nil, errors.New("invalid self: obj = nil")
	}
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, fmt.Errorf("newGrnCtx failed: %v", err)
	}
	C.grn_ctx_use(ctx.ctx, db.obj)
	if err := ctx.Err(); err != nil {
		ctx.Close()
		return nil, fmt.Errorf("C.grn_ctx_use failed: %v", err)
	}
	db.mutex.Lock()
	db.count++
	db.mutex.Unlock()
	return ctx, nil
}
