// Package libgrn provides GQTP clients and DB handles using libgroonga.
package libgrn

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"
import (
	"reflect"
	"sync"
	"unsafe"

	"github.com/groonga/grnci/v2"
)

const (
	flagMore  = byte(0x01)
	flagTail  = byte(0x02)
	flagHead  = byte(0x04)
	flagQuiet = byte(0x08)
	flagQuit  = byte(0x10)
)

var (
	// libCount is incremented by Init and decremented by Fin.
	libCount int
	// libMutex is used for exclusion control in Init and Fin.
	libMutex sync.Mutex
	// initFinDisabled represents whether or not Init and Fin are disabled.
	initFinDisabled bool
)

// Init initializes libgroonga.
//
// If an internal counter is zero, Init increments it and initializes libgroonga.
// Otherwise, Init only increments the internal counter.
//
// There is no need to call Init explicitly.
// libgrn calls Init when it creates a Conn.
func Init() error {
	libMutex.Lock()
	defer libMutex.Unlock()
	if !initFinDisabled && libCount == 0 {
		if rc := C.grn_init(); rc != C.GRN_SUCCESS {
			return grnci.NewGroongaError(grnci.ResultCode(rc), map[string]interface{}{
				"method": "C.grn_init",
				"error":  "Failed to initialize libgroonga.",
			})
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
// libgrn calls Fin when it closes a Conn.
func Fin() error {
	libMutex.Lock()
	defer libMutex.Unlock()
	if libCount <= 0 {
		return grnci.NewError(grnci.OperationError, map[string]interface{}{
			"libCount": libCount,
			"error":    "libCount must be greater than 0.",
		})
	}
	libCount--
	if !initFinDisabled && libCount == 0 {
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return grnci.NewGroongaError(grnci.ResultCode(rc), map[string]interface{}{
				"method": "C.grn_fin",
				"error":  "Failed to finalize libgroonga.",
			})
		}
	}
	return nil
}

// DisableInitFin disables Init and Fin.
// If another package initializes and finalizes libgroonga,
// DisableInitFin must be called before creation of the first Conn.
func DisableInitFin() {
	initFinDisabled = true
}

// grnCtx wraps C.grn_ctx.
type grnCtx struct {
	ctx *C.grn_ctx
}

// newGrnCtx returns a new grnCtx.
func newGrnCtx() (*grnCtx, error) {
	if err := Init(); err != nil {
		return nil, err
	}
	ctx := C.grn_ctx_open(C.int(0))
	if ctx == nil {
		Fin()
		return nil, grnci.NewError(grnci.UnexpectedError, map[string]interface{}{
			"method": "C.grn_ctx_open",
		})
	}
	return &grnCtx{ctx: ctx}, nil
}

// Close closes the grnCtx.
func (c *grnCtx) Close() error {
	if rc := C.grn_ctx_close(c.ctx); rc != C.GRN_SUCCESS {
		return grnci.NewGroongaError(grnci.ResultCode(rc), map[string]interface{}{
			"method": "C.grn_ctx_close",
		})
	}
	if err := Fin(); err != nil {
		return err
	}
	return nil
}

// Err returns the stored error.
func (c *grnCtx) Err(method string) error {
	if c.ctx.rc == C.GRN_SUCCESS {
		return nil
	}
	data := map[string]interface{}{
		"method": method,
	}
	if c.ctx.errline != 0 {
		data["line"] = int(c.ctx.errline)
	}
	if c.ctx.errfile != nil {
		data["file"] = C.GoString(c.ctx.errfile)
	}
	if c.ctx.errfunc != nil {
		data["function"] = C.GoString(c.ctx.errfunc)
	}
	if c.ctx.errbuf[0] != 0 {
		data["error"] = C.GoString(&c.ctx.errbuf[0])
	}
	return grnci.NewGroongaError(grnci.ResultCode(c.ctx.rc), data)
}

// Send sends data with flags.
// The behavior depends on the grnCtx.
func (c *grnCtx) Send(data []byte, flags byte) error {
	var p *C.char
	if len(data) != 0 {
		p = (*C.char)(unsafe.Pointer(&data[0]))
	}
	// C.grn_ctx_send always returns 0.
	C.grn_ctx_send(c.ctx, p, C.uint(len(data)), C.int(flags))
	if err := c.Err("C.grn_ctx_send"); err != nil {
		return err
	}
	return nil
}

// Recv receives data with flags.
// The data will be destroyed by the next operation on the grnCtx.
func (c *grnCtx) Recv() (data []byte, flags byte, err error) {
	var cPtr *C.char
	var cLen C.uint
	var cFlags C.int
	// C.grn_ctx_recv always returns 0 if c.ctx is not nil.
	C.grn_ctx_recv(c.ctx, &cPtr, &cLen, &cFlags)
	head := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	head.Data = uintptr(unsafe.Pointer(cPtr))
	head.Len = int(cLen)
	head.Cap = int(cLen)
	if err = c.Err("C.grn_ctx_recv"); err != nil {
		return
	}
	flags = byte(cFlags)
	return
}

// grnDB wraps a C.grn_obj referring to a DB object.
type grnDB struct {
	obj   *C.grn_obj
	count int
	mutex sync.Mutex
}

// createGrnDB creates a new DB.
func createGrnDB(ctx *grnCtx, path string) (*grnDB, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	obj := C.grn_db_create(ctx.ctx, cPath, nil)
	if obj == nil {
		if err := ctx.Err("C.grn_db_create"); err != nil {
			return nil, err
		}
		return nil, grnci.NewError(grnci.UnexpectedError, map[string]interface{}{
			"method": "C.grn_db_create",
		})
	}
	return &grnDB{
		obj:   obj,
		count: 1,
	}, nil
}

// openGrnDB opens an existing DB.
func openGrnDB(ctx *grnCtx, path string) (*grnDB, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	obj := C.grn_db_open(ctx.ctx, cPath)
	if obj == nil {
		if err := ctx.Err("C.grn_db_open"); err != nil {
			return nil, err
		}
		return nil, grnci.NewError(grnci.UnexpectedError, map[string]interface{}{
			"method": "C.grn_db_open",
		})
	}
	return &grnDB{
		obj:   obj,
		count: 1,
	}, nil
}

// Close closes the grnDB.
func (db *grnDB) Close(ctx *grnCtx) error {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	if db.count <= 0 {
		return grnci.NewError(grnci.OperationError, map[string]interface{}{
			"count": db.count,
			"error": "count must be greater than 0.",
		})
	}
	db.count--
	if db.count == 0 {
		if rc := C.grn_obj_close(ctx.ctx, db.obj); rc != C.GRN_SUCCESS {
			if err := ctx.Err("C.grn_obj_close"); err != nil {
				return grnci.EnhanceError(err, map[string]interface{}{
					"rc": int(rc),
				})
			}
			return grnci.NewGroongaError(grnci.ResultCode(rc), map[string]interface{}{
				"method": "C.grn_obj_close",
			})
		}
		db.obj = nil
	}
	return nil
}

// Dup returns a new grnCtx to handle the grnDB.
func (db *grnDB) Dup() (*grnCtx, error) {
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	// C.grn_ctx_use returns ctx.ctx.rc.
	C.grn_ctx_use(ctx.ctx, db.obj)
	if err := ctx.Err("C.grn_ctx_use"); err != nil {
		ctx.Close()
		return nil, err
	}
	db.mutex.Lock()
	db.count++
	db.mutex.Unlock()
	return ctx, nil
}
