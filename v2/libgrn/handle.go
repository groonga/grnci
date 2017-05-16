package libgrn

// #cgo pkg-config: groonga
// #include <groonga.h>
// #include <stdlib.h>
import "C"
import (
	"fmt"
	"io/ioutil"
	"unsafe"

	"github.com/groonga/grnci/v2"
)

// Handle is a handle for a local DB.
type Handle struct {
	ctx *grnCtx
	db  *grnDB
}

// Open opens a local DB and returns a handle.
func Open(path string) (*Handle, error) {
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	db, err := openGrnDB(ctx, path)
	if err != nil {
		ctx.Close()
		return nil, err
	}
	return &Handle{
		ctx: ctx,
		db:  db,
	}, nil
}

// Create creates a local DB and returns a hendle.
func Create(path string) (*Handle, error) {
	ctx, err := newGrnCtx()
	if err != nil {
		return nil, err
	}
	db, err := createGrnDB(ctx, path)
	if err != nil {
		ctx.Close()
		return nil, err
	}
	return &Handle{
		ctx: ctx,
		db:  db,
	}, nil
}

// Close closes a handle.
func (h *Handle) Close() error {
	if err := h.db.Close(h.ctx); err != nil {
		return err
	}
	if err := h.ctx.Close(); err != nil {
		return err
	}
	return nil
}

// Dup duplicates a handle.
func (h *Handle) Dup() (*Handle, error) {
	ctx, err := h.db.Dup()
	if err != nil {
		return nil, err
	}
	return &Handle{
		ctx: ctx,
		db:  h.db,
	}, nil
}

// send sends data.
func (h *Handle) send(data []byte) error {
	var p *C.char
	if len(data) != 0 {
		p = (*C.char)(unsafe.Pointer(&data[0]))
	}
	rc := C.grn_rc(C.grn_ctx_send(h.ctx.ctx, p, C.uint(len(data)), C.int(0)))
	if (rc != C.GRN_SUCCESS) || (h.ctx.ctx.rc != C.GRN_SUCCESS) {
		return fmt.Errorf("C.grn_ctx_send failed: rc = %d", rc)
	}
	return nil
}

// recv receives data.
func (h *Handle) recv() ([]byte, error) {
	var resp *C.char
	var respLen C.uint
	var respFlags C.int
	rc := C.grn_rc(C.grn_ctx_recv(h.ctx.ctx, &resp, &respLen, &respFlags))
	if (rc != C.GRN_SUCCESS) || (h.ctx.ctx.rc != C.GRN_SUCCESS) {
		return nil, fmt.Errorf("C.grn_ctx_recv failed: rc = %d", rc)
	}
	return C.GoBytes(unsafe.Pointer(resp), C.int(respLen)), nil
}

// Query sends a request and receives a response.
//
// TODO: error handling
func (h *Handle) Query(req *grnci.Request) (*grnci.Response, error) {
	cmd, err := req.Assemble()
	if err != nil {
		return nil, err
	}
	if err := h.send(cmd); err != nil {
		respBytes, _ := h.recv()
		resp, _ := grnci.NewResponse(respBytes)
		return resp, err
	}
	respBytes, err := h.recv()
	if (req.Body == nil) || (err != nil) {
		resp, _ := grnci.NewResponse(respBytes)
		return resp, err
	}
	if len(respBytes) != 0 {
		resp, _ := grnci.NewResponse(respBytes)
		return resp, fmt.Errorf("unexpected response")
	}
	body, _ := ioutil.ReadAll(req.Body)
	if err := h.send(body); err != nil {
		respBytes, _ := h.recv()
		resp, _ := grnci.NewResponse(respBytes)
		return resp, err
	}
	respBytes, _ = h.recv()
	resp, _ := grnci.NewResponse(respBytes)
	return resp, nil
}
