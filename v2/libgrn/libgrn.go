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
			return grnci.NewError(grnci.ErrorCode(rc), "C.grn_init failed.", nil)
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
		return grnci.NewError(grnci.OperationError, "libCount must be greater than 0.", map[string]interface{}{
			"libCount": libCount,
		})
	}
	libCount--
	if !initFinDisabled && libCount == 0 {
		if rc := C.grn_fin(); rc != C.GRN_SUCCESS {
			return grnci.NewError(grnci.ErrorCode(rc), "C.grn_fin failed.", nil)
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
		return nil, grnci.NewError(grnci.UnexpectedError, "C.grn_ctx_open failed.", nil)
	}
	return &grnCtx{ctx: ctx}, nil
}

// Close closes the grnCtx.
func (c *grnCtx) Close() error {
	if rc := C.grn_ctx_close(c.ctx); rc != C.GRN_SUCCESS {
		return grnci.NewError(grnci.ErrorCode(rc), "C.grn_ctx_close failed.", nil)
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
	data := make(map[string]interface{})
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
	return grnci.NewError(grnci.ErrorCode(c.ctx.rc), method+" failed.", data)
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
		return nil, grnci.NewError(grnci.UnexpectedError, "C.grn_db_create failed.", nil)
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
		return nil, grnci.NewError(grnci.UnexpectedError, "C.grn_db_open failed.", nil)
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
		return grnci.NewError(grnci.OperationError, "count must be greater than 0.", map[string]interface{}{
			"count": db.count,
		})
	}
	db.count--
	if db.count == 0 {
		if rc := C.grn_obj_close(ctx.ctx, db.obj); rc != C.GRN_SUCCESS {
			if err := ctx.Err("C.grn_obj_close"); err != nil {
				return err
			}
			return grnci.NewError(grnci.ErrorCode(rc), "C.grn_obj_close failed.", map[string]interface{}{})
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

// Log levels from lower to higher.
const (
	LogNone    = int(C.GRN_LOG_NONE)
	LogEmerg   = int(C.GRN_LOG_EMERG)
	LogAlert   = int(C.GRN_LOG_ALERT)
	LogCrit    = int(C.GRN_LOG_CRIT)
	LogError   = int(C.GRN_LOG_ERROR)
	LogWarning = int(C.GRN_LOG_WARNING)
	LogNotice  = int(C.GRN_LOG_NOTICE)
	LogInfo    = int(C.GRN_LOG_INFO)
	LogDebug   = int(C.GRN_LOG_DEBUG)
	LogDump    = int(C.GRN_LOG_DUMP)
)

// Log flags.
const (
	LogTime     = int(C.GRN_LOG_TIME)
	LogTitle    = int(C.GRN_LOG_TITLE)
	LogMessage  = int(C.GRN_LOG_MESSAGE)
	LogLocation = int(C.GRN_LOG_LOCATION)
	LogPID      = int(C.GRN_LOG_PID)
)

// LogOptions stores options for Log.
type LogOptions struct {
	// MaxLevel specifies the maximum log level.
	// If MaxLevel < 0, Log does not change the maximum log level.
	// Otherwise, Log changes the maximum log level.
	//
	// The default setting is LogNotice.
	MaxLevel int

	// Flags specifies the log flags.
	// If Flags < 0, Log does not change the log flags.
	// Otherwise, Log changes the log flags.
	//
	// The default setting is LogTime|LogMessage.
	Flags int

	// RotationThreshold specifies the log rotation size threshold in bytes.
	// If RotationThreshold < 0, Log does not change the setting of log roration.
	// Else if RotationThreshold == 0, log rotation is disabled.
	// Otherwise, log rotation is enabled.
	//
	// If log rotation is enabled, the logger creates the next log file
	// when the size of the current log file exceeds the threshold.
	// The path to the next log file has the suffix which represents the local time.
	// For example, if the log path is "groonga.log", the first log file is "groonga.log"
	// and other log files are named "groonga.log.2006-01-02-15-04-05-999999".
	//
	// The default setting is 0.
	RotationThreshold int
}

// NewLogOptions returns the default LogOptions which does not change any settings.
func NewLogOptions() *LogOptions {
	return &LogOptions{
		MaxLevel:          -1,
		Flags:             -1,
		RotationThreshold: -1,
	}
}

// Log configures logging.
// If path == "", Log disables logging.
// Otherwise, Log enables logging.
// If options == nil, Log uses the default LogOptions.
func Log(path string, options *LogOptions) {
	if options == nil {
		options = NewLogOptions()
	}
	if options.MaxLevel >= 0 {
		C.grn_default_logger_set_max_level(C.grn_log_level(options.MaxLevel))
	}
	if options.Flags >= 0 {
		C.grn_default_logger_set_flags(C.int(options.Flags))
	}
	if options.RotationThreshold >= 0 {
		C.grn_default_logger_set_rotate_threshold_size(C.off_t(options.RotationThreshold))
	}
	if path == "" {
		C.grn_default_logger_set_path(nil)
	} else {
		cPath := C.CString(path)
		defer C.free(unsafe.Pointer(cPath))
		C.grn_default_logger_set_path(cPath)
	}
}

// Query log flags
const (
	QueryLogNone        = int(C.GRN_QUERY_LOG_NONE)
	QueryLogCommand     = int(C.GRN_QUERY_LOG_COMMAND)
	QueryLogResultCode  = int(C.GRN_QUERY_LOG_RESULT_CODE)
	QueryLogDestination = int(C.GRN_QUERY_LOG_DESTINATION)
	QueryLogCache       = int(C.GRN_QUERY_LOG_CACHE)
	QueryLogSize        = int(C.GRN_QUERY_LOG_SIZE)
	QueryLogScore       = int(C.GRN_QUERY_LOG_SCORE)
	QueryLogAll         = int(C.GRN_QUERY_LOG_ALL)
)

// QueryLogOptions stores options for QueryLog.
type QueryLogOptions struct {
	// Flags specifies the query log flags.
	// If Flags < 0, QueryLog does not change the query log flags.
	// Otherwise, QueryLog changes the query log flags.
	//
	// The default setting is QueryLogAll.
	Flags int

	// RotationThreshold specifies the query log rotation size threshold in bytes.
	// If RotationThreshold < 0, QueryLog does not change the setting of query log roration.
	// Else if RotationThreshold == 0, query log rotation is disabled.
	// Otherwise, query log rotation is enabled.
	//
	// If query log rotation is enabled, the logger creates the next query log file
	// when the size of the current log file exceeds the threshold.
	// The path to the next log file has the suffix which represents the local time.
	// For example, if the log path is "query.log", the first log file is "query.log"
	// and other log files are named "query.log.2006-01-02-15-04-05-999999".
	//
	// The default setting is 0.
	RotationThreshold int
}

// NewQueryLogOptions returns the default QueryLogOptions which does not change any settings.
func NewQueryLogOptions() *QueryLogOptions {
	return &QueryLogOptions{
		Flags:             -1,
		RotationThreshold: -1,
	}
}

// QueryLog configures query logging.
// If path == "", QueryLog disables query logging.
// Otherwise, QueryLog enables query logging.
// If options == nil, QueryLog uses the default QueryLogOptions.
func QueryLog(path string, options *QueryLogOptions) {
	if options == nil {
		options = NewQueryLogOptions()
	}
	if options.Flags >= 0 {
		C.grn_default_query_logger_set_flags(C.uint(options.Flags))
	}
	if options.RotationThreshold >= 0 {
		C.grn_default_query_logger_set_rotate_threshold_size(C.off_t(options.RotationThreshold))
	}
	if path == "" {
		C.grn_default_query_logger_set_path(nil)
	} else {
		cPath := C.CString(path)
		defer C.free(unsafe.Pointer(cPath))
		C.grn_default_query_logger_set_path(cPath)
	}
}
