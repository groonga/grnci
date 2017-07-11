package grnci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"
	"time"
)

// DB is a wrapper to provide a high-level command interface.
type DB struct {
	Handler
}

// NewDB returns a new DB that wraps the specified client or handle.
func NewDB(h Handler) *DB {
	return &DB{Handler: h}
}

// recvBool reads the bool result from resp.
func (db *DB) recvBool(resp Response) (bool, Response, error) {
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return false, resp, err
	}
	var result bool
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return false, resp, nil
		}
		return false, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// recvInt reads the int result from resp.
func (db *DB) recvInt(resp Response) (int, Response, error) {
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return 0, resp, err
	}
	var result int
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return 0, resp, nil
		}
		return 0, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// recvInt reads the string result from resp.
func (db *DB) recvString(resp Response) (string, Response, error) {
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return "", resp, err
	}
	var result string
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return "", resp, nil
		}
		return "", resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// CacheLimit executes cache_limit.
// If max < 0, max is not passed to cache_limit.
func (db *DB) CacheLimit(max int) (int, Response, error) {
	var params map[string]interface{}
	if max >= 0 {
		params = map[string]interface{}{
			"max": max,
		}
	}
	resp, err := db.Invoke("cache_limit", params, nil)
	if err != nil {
		return 0, nil, err
	}
	return db.recvInt(resp)
}

// ColumnCopy executes column_copy.
func (db *DB) ColumnCopy(from, to string) (bool, Response, error) {
	i := strings.IndexByte(from, '.')
	if i == -1 {
		return false, nil, NewError(CommandError, map[string]interface{}{
			"from":  from,
			"error": "The from must contain a dot.",
		})
	}
	fromTable := from[:i]
	fromName := from[i+1:]
	if i = strings.IndexByte(to, '.'); i == -1 {
		return false, nil, NewError(CommandError, map[string]interface{}{
			"to":    to,
			"error": "The to must contain a dot.",
		})
	}
	toTable := to[:i]
	toName := to[i+1:]
	resp, err := db.Invoke("column_copy", map[string]interface{}{
		"from_table": fromTable,
		"from_name":  fromName,
		"to_table":   toTable,
		"to_name":    toName,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// ColumnCreate executes column_create.
func (db *DB) ColumnCreate(name, typ string, flags []string) (bool, Response, error) {
	i := strings.IndexByte(name, '.')
	if i == -1 {
		return false, nil, NewError(CommandError, map[string]interface{}{
			"name":  name,
			"error": "The name must contain a dot.",
		})
	}
	params := map[string]interface{}{
		"table": name[:i],
		"name":  name[i+1:],
	}
	typFlag := "COLUMN_SCALAR"
	var srcs []string
	if strings.HasPrefix(typ, "[]") {
		typFlag = "COLUMN_VECTOR"
		typ = typ[2:]
	} else if idx := strings.IndexByte(typ, '.'); idx != -1 {
		typFlag = "COLUMN_INDEX"
		srcs = strings.Split(typ[idx+1:], ",")
		typ = typ[:idx]
	}
	flags = append(flags, typFlag)
	if len(srcs) > 1 {
		flags = append(flags, "WITH_SECTION")
	}
	params["flags"] = flags
	params["type"] = typ
	if srcs != nil {
		params["source"] = srcs
	}
	resp, err := db.Invoke("column_create", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBColumn is a result of column_list.
type DBColumn struct {
	ID      uint32   `json:"id"`
	Name    string   `json:"name"`
	Path    string   `json:"path"`
	Type    string   `json:"type"`
	Flags   []string `json:"flags"`
	Domain  string   `json:"domain"`
	Range   string   `json:"range"`
	Sources []string `json:"source"`
}

// ColumnList executes column_list.
func (db *DB) ColumnList(tbl string) ([]DBColumn, Response, error) {
	resp, err := db.Invoke("column_list", map[string]interface{}{
		"table": tbl,
	}, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result [][]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	if len(result) == 0 {
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"error": "The result is empty.",
		})
	}
	var fields []string
	for _, meta := range result[0] {
		if values, ok := meta.([]interface{}); ok {
			if field, ok := values[0].(string); ok {
				fields = append(fields, field)
			}
		}
	}
	var columns []DBColumn
	for _, values := range result[1:] {
		var column DBColumn
		for i := 0; i < len(fields) && i < len(values); i++ {
			switch fields[i] {
			case "id":
				if v, ok := values[i].(float64); ok {
					column.ID = uint32(v)
				}
			case "name":
				if v, ok := values[i].(string); ok {
					column.Name = v
				}
			case "path":
				if v, ok := values[i].(string); ok {
					column.Path = v
				}
			case "type":
				if v, ok := values[i].(string); ok {
					column.Type = v
				}
			case "flags":
				if v, ok := values[i].(string); ok {
					column.Flags = strings.Split(v, "|")
				}
			case "domain":
				if v, ok := values[i].(string); ok {
					column.Domain = v
				}
			case "range":
				if v, ok := values[i].(string); ok {
					column.Range = v
				}
			case "source":
				if vs, ok := values[i].([]interface{}); ok {
					for _, v := range vs {
						if v, ok := v.(string); ok {
							column.Sources = append(column.Sources, v)
						}
					}
				}
			}
		}
		columns = append(columns, column)
	}
	return columns, resp, nil
}

// ColumnRemove executes column_remove.
func (db *DB) ColumnRemove(name string) (bool, Response, error) {
	i := strings.IndexByte(name, '.')
	if i == -1 {
		return false, nil, NewError(CommandError, map[string]interface{}{
			"name":  name,
			"error": "The name must contain a dot.",
		})
	}
	resp, err := db.Invoke("column_remove", map[string]interface{}{
		"table": name[:i],
		"name":  name[i+1:],
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// ColumnRename executes column_rename.
func (db *DB) ColumnRename(name, newName string) (bool, Response, error) {
	i := strings.IndexByte(name, '.')
	if i == -1 {
		return false, nil, NewError(CommandError, map[string]interface{}{
			"name":  name,
			"error": "The name must contain a dot.",
		})
	}
	if j := strings.IndexByte(newName, '.'); j != -1 {
		if i != j || name[:i] != newName[:i] {
			return false, nil, NewError(CommandError, map[string]interface{}{
				"name":    name,
				"newName": newName,
				"error":   "The names have different table names.",
			})
		}
		newName = newName[j+1:]
	}
	resp, err := db.Invoke("column_rename", map[string]interface{}{
		"table":    name[:i],
		"name":     name[i+1:],
		"new_name": newName,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// ConfigDelete executes config_delete.
func (db *DB) ConfigDelete(key, value string) (bool, Response, error) {
	resp, err := db.Invoke("config_delete", map[string]interface{}{
		"key": key,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// ConfigGet executes config_get.
func (db *DB) ConfigGet(key string) (string, Response, error) {
	resp, err := db.Invoke("config_get", map[string]interface{}{
		"key": key,
	}, nil)
	if err != nil {
		return "", nil, err
	}
	return db.recvString(resp)
}

// ConfigSet executes config_set.
func (db *DB) ConfigSet(key, value string) (bool, Response, error) {
	resp, err := db.Invoke("config_set", map[string]interface{}{
		"key":   key,
		"value": value,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DatabaseUnmap executes database_unmap.
func (db *DB) DatabaseUnmap() (bool, Response, error) {
	resp, err := db.Invoke("delete", nil, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DeleteByID executes delete.
func (db *DB) DeleteByID(tbl string, id int) (bool, Response, error) {
	resp, err := db.Invoke("delete", map[string]interface{}{
		"table": tbl,
		"id":    id,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DeleteByKey executes delete.
func (db *DB) DeleteByKey(tbl string, key interface{}) (bool, Response, error) {
	resp, err := db.Invoke("delete", map[string]interface{}{
		"table": tbl,
		"key":   key,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DeleteByFilter executes delete.
func (db *DB) DeleteByFilter(tbl, filter string) (bool, Response, error) {
	resp, err := db.Invoke("delete", map[string]interface{}{
		"table":  tbl,
		"filter": filter,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBDumpOptions stores options for DB.Dump.
type DBDumpOptions struct {
	Tables      string // --table
	DumpPlugins bool   // --dump_plugins
	DumpSchema  bool   // --dump_schema
	DumpRecords bool   // --dump_records
	DumpIndexes bool   // --dump_indexes
}

// NewDBDumpOptions returns the default DBDumpOptions.
func NewDBDumpOptions() *DBDumpOptions {
	return &DBDumpOptions{
		DumpPlugins: true,
		DumpSchema:  true,
		DumpRecords: true,
		DumpIndexes: true,
	}
}

// Dump executes dump.
// On success, it is the caller's responsibility to close the result.
func (db *DB) Dump(options *DBDumpOptions) (io.ReadCloser, Response, error) {
	if options == nil {
		options = NewDBDumpOptions()
	}
	params := map[string]interface{}{
		"dump_plugins": options.DumpPlugins,
		"dump_schema":  options.DumpSchema,
		"dump_records": options.DumpRecords,
		"dump_indexes": options.DumpIndexes,
	}
	if options.Tables != "" {
		params["tables"] = options.Tables
	}
	resp, err := db.Invoke("dump", params, nil)
	if err != nil {
		return nil, nil, err
	}
	return resp, resp, err
}

// DBIOFlushOptions stores options for DB.IOFlush.
type DBIOFlushOptions struct {
	TargetName string // --target_name
	Recursive  bool   // --recursive
	OnlyOpened bool   // --only_opened
}

// NewDBIOFlushOptions returns the default DBIOFlushOptions.
func NewDBIOFlushOptions() *DBIOFlushOptions {
	return &DBIOFlushOptions{
		Recursive: true,
	}
}

// IOFlush executes io_flush.
func (db *DB) IOFlush(options *DBIOFlushOptions) (bool, Response, error) {
	if options == nil {
		options = NewDBIOFlushOptions()
	}
	params := map[string]interface{}{
		"recursive":   options.Recursive,
		"only_opened": options.OnlyOpened,
	}
	if options.TargetName != "" {
		params["target_name"] = options.TargetName
	}
	resp, err := db.Invoke("io_flush", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBLoadOptions stores options for DB.Load.
// http://groonga.org/docs/reference/commands/load.html
type DBLoadOptions struct {
	Columns  []string // --columns
	IfExists string   // --ifexists
}

// NewDBLoadOptions returns the default DBLoadOptions.
func NewDBLoadOptions() *DBLoadOptions {
	return &DBLoadOptions{}
}

// Load executes load.
func (db *DB) Load(tbl string, values io.Reader, options *DBLoadOptions) (int, Response, error) {
	params := map[string]interface{}{
		"table": tbl,
	}
	if options == nil {
		options = NewDBLoadOptions()
	}
	if options.Columns != nil {
		params["columns"] = options.Columns
	}
	if options.IfExists != "" {
		params["ifexists"] = options.IfExists
	}
	resp, err := db.Invoke("load", params, values)
	if err != nil {
		return 0, nil, err
	}
	return db.recvInt(resp)
}

// appendRow appends the JSON-encoded row to buf nad returns the exetended buffer.
func (db *DB) appendRow(body []byte, row reflect.Value, cfs []*ColumnField) []byte {
	body = append(body, '[')
	for i, fi := range cfs {
		if i != 0 {
			body = append(body, ',')
		}
		body = AppendJSONValue(body, row.Field(fi.Index))
	}
	body = append(body, ']')
	return body
}

// appendRows appends the JSON-encoded rows to buf nad returns the exetended buffer.
func (db *DB) appendRows(body []byte, rows reflect.Value, cfs []*ColumnField) []byte {
	n := rows.Len()
	for i := 0; i < n; i++ {
		if i != 0 {
			body = append(body, ',')
		}
		row := rows.Index(i)
		body = db.appendRow(body, row, cfs)
	}
	return body
}

// LoadRows executes load.
func (db *DB) LoadRows(tbl string, rows interface{}, options *DBLoadOptions) (int, Response, error) {
	if options == nil {
		options = NewDBLoadOptions()
	}
	rs, err := GetRowStruct(rows)
	if err != nil {
		return 0, nil, err
	}
	var cfs []*ColumnField
	if options.Columns == nil {
		for _, cf := range rs.Columns {
			if cf.Loadable {
				options.Columns = append(options.Columns, cf.Name)
				cfs = append(cfs, cf)
			}
		}
	} else {
		for _, col := range options.Columns {
			cf, ok := rs.ColumnsByName[col]
			if !ok {
				return 0, nil, NewError(CommandError, map[string]interface{}{
					"column": col,
					"error":  "The column has no associated field.",
				})
			}
			cfs = append(cfs, cf)
		}
	}

	body := []byte("[")
	v := reflect.ValueOf(rows)
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return 0, nil, NewError(CommandError, map[string]interface{}{
				"rows":  nil,
				"error": "The rows must not be nil.",
			})
		}
		v = v.Elem()
		if v.Kind() != reflect.Struct {
			return 0, nil, NewError(CommandError, map[string]interface{}{
				"type":  reflect.TypeOf(rows).Name(),
				"error": "The type is not supported.",
			})
		}
		body = db.appendRow(body, v, cfs)
	case reflect.Array, reflect.Slice:
		body = db.appendRows(body, v, cfs)
	case reflect.Struct:
		body = db.appendRow(body, v, cfs)
	default:
		return 0, nil, NewError(CommandError, map[string]interface{}{
			"type":  reflect.TypeOf(rows).Name(),
			"error": "The type is not supported.",
		})
	}
	body = append(body, ']')
	return db.Load(tbl, bytes.NewReader(body), options)
}

// LockAcquire executes lock_acquire.
func (db *DB) LockAcquire(target string) (bool, Response, error) {
	var params map[string]interface{}
	if target != "" {
		params = map[string]interface{}{
			"target_name": target,
		}
	}
	resp, err := db.Invoke("lock_acquire", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// LockClear executes lock_clear.
func (db *DB) LockClear(target string) (bool, Response, error) {
	var params map[string]interface{}
	if target != "" {
		params = map[string]interface{}{
			"target_name": target,
		}
	}
	resp, err := db.Invoke("lock_clear", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// LockRelease executes lock_release.
func (db *DB) LockRelease(target string) (bool, Response, error) {
	var params map[string]interface{}
	if target != "" {
		params = map[string]interface{}{
			"target_name": target,
		}
	}
	resp, err := db.Invoke("lock_release", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// LogLevel executes log_level.
func (db *DB) LogLevel(level string) (bool, Response, error) {
	resp, err := db.Invoke("log_level", map[string]interface{}{
		"level": level,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// LogPut executes log_put.
func (db *DB) LogPut(level, msg string) (bool, Response, error) {
	resp, err := db.Invoke("log_put", map[string]interface{}{
		"level":   level,
		"message": msg,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// LogReopen executes log_reopen.
func (db *DB) LogReopen() (bool, Response, error) {
	resp, err := db.Invoke("log_reopen", nil, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBLogicalCountOptions stores options for DB.LogicalCount.
type DBLogicalCountOptions struct {
	Min       time.Time //--min
	MinBorder bool      // --min_border
	Max       time.Time // --max
	MaxBorder bool      // --max_border
	Filter    string    // --filter
}

// NewDBLogicalCountOptions returns the default DBLogicalCountOptions.
func NewDBLogicalCountOptions() *DBLogicalCountOptions {
	return &DBLogicalCountOptions{
		MinBorder: true,
		MaxBorder: true,
	}
}

// LogicalCount executes logical_count.
func (db *DB) LogicalCount(logicalTable, shardKey string, options *DBLogicalCountOptions) (int, Response, error) {
	params := map[string]interface{}{
		"logical_table": logicalTable,
		"shard_key":     shardKey,
	}
	if options == nil {
		options = NewDBLogicalCountOptions()
	}
	if !options.Min.IsZero() {
		params["min"] = options.Min
	}
	params["min_border"] = options.MinBorder
	if !options.Max.IsZero() {
		params["max"] = options.Max
	}
	params["max_border"] = options.MaxBorder
	if options.Filter != "" {
		params["filter"] = options.Filter
	}
	resp, err := db.Invoke("logical_count", params, nil)
	if err != nil {
		return 0, nil, err
	}
	return db.recvInt(resp)
}

// DBLogicalParameters is a result of logical_parameters.
type DBLogicalParameters struct {
	RangeIndex string `json:"range_index"`
}

// LogicalParameters executes logical_parameters.
func (db *DB) LogicalParameters(rangeIndex string) (*DBLogicalParameters, Response, error) {
	var params map[string]interface{}
	if rangeIndex != "" {
		params = map[string]interface{}{
			"range_index": rangeIndex,
		}
	}
	resp, err := db.Invoke("logical_parameters", params, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result DBLogicalParameters
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return &result, resp, nil
}

// LogicalRangeFilter executes logical_range_filter.
func (db *DB) LogicalRangeFilter() (bool, Response, error) {
	// TODO
	return false, nil, nil
}

// DBLogicalSelectOptions stores options for DB.LogicalSelect.
// http://groonga.org/docs/reference/commands/logical_select.html
type DBLogicalSelectOptions struct {
	Min                    time.Time //--min
	MinBorder              bool      // --min_border
	Max                    time.Time // --max
	MaxBorder              bool      // --max_border
	Filter                 string    // --filter
	SortKeys               []string  // --sort_keys
	OutputColumns          []string  // --output_columns
	Offset                 int       // --offset
	Limit                  int       // --limit
	Drilldown              []string  // --drilldown
	DrilldownSortKeys      []string  // --drilldown_sort_keys
	DrilldownOutputColumns []string  // --drilldown_output_columns
	DrilldownOffset        int       // --drilldown_offset
	DrilldownLimit         int       // --drilldown_limit
	DrilldownCalcTypes     []string  // --drilldown_calc_types
	DrilldownCalcTarget    string    // --drilldown_calc_target
	MatchColumns           []string  // --match_columns
	Query                  string    // --query
	DrilldownFilter        string    // --drilldown_filter
	Columns                map[string]*DBSelectOptionsColumn
	Drilldowns             map[string]*DBSelectOptionsDrilldown
}

// NewDBLogicalSelectOptions returns the default DBLogicalSelectOptions.
func NewDBLogicalSelectOptions() *DBLogicalSelectOptions {
	return &DBLogicalSelectOptions{
		Limit:          10,
		DrilldownLimit: 10,
	}
}

// LogicalSelect executes logical_select.
func (db *DB) LogicalSelect(logicalTable, shardKey string, options *DBLogicalSelectOptions) (io.ReadCloser, Response, error) {
	if options == nil {
		options = NewDBLogicalSelectOptions()
	}
	params := map[string]interface{}{
		"command_version": 2,
		"logical_table":   logicalTable,
		"shard_key":       shardKey,
	}
	if options.MatchColumns != nil {
		params["match_columns"] = options.MatchColumns
	}
	if options.Query != "" {
		params["query"] = options.Query
	}
	if options.Filter != "" {
		params["filter"] = options.Filter
	}
	if options.SortKeys != nil {
		params["sort_keys"] = options.SortKeys
	}
	if options.OutputColumns != nil {
		params["output_columns"] = options.OutputColumns
	}
	if options.OutputColumns != nil {
		params["query"] = options.Query
	}
	if options.Offset != 0 {
		params["offset"] = options.Offset
	}
	if options.Limit != 10 {
		params["limit"] = options.Limit
	}
	if options.Drilldown != nil {
		params["drilldown"] = options.Drilldown
	}
	if options.DrilldownSortKeys != nil {
		params["drilldown_sort_keys"] = options.DrilldownSortKeys
	}
	if options.DrilldownOutputColumns != nil {
		params["drilldown_output_columns"] = options.DrilldownOutputColumns
	}
	if options.DrilldownOffset != 0 {
		params["drilldown_offset"] = options.DrilldownOffset
	}
	if options.DrilldownLimit != 10 {
		params["drilldown_limit"] = options.DrilldownLimit
	}
	if options.DrilldownCalcTypes != nil {
		params["drilldown_calc_types"] = options.DrilldownCalcTypes
	}
	if options.DrilldownCalcTarget != "" {
		params["drilldown_calc_target"] = options.DrilldownCalcTarget
	}
	if options.DrilldownFilter != "" {
		params["drilldown_filter"] = options.DrilldownFilter
	}
	for name, col := range options.Columns {
		col.setParams("--columns["+name+"]", params)
	}
	for label, drilldown := range options.Drilldowns {
		drilldown.setParams("--drilldowns["+label+"]", params)
	}
	resp, err := db.Invoke("logical_select", params, nil)
	if err != nil {
		return nil, nil, err
	}
	return resp, resp, err
}

// LogicalSelectRows executes logical_select.
func (db *DB) LogicalSelectRows(logicalTable, shardKey string, rows interface{}, options *DBLogicalSelectOptions) (int, Response, error) {
	if options == nil {
		options = NewDBLogicalSelectOptions()
	}
	rs, err := GetRowStruct(rows)
	if err != nil {
		return 0, nil, err
	}
	var cfs []*ColumnField
	if options.OutputColumns == nil {
		cfs = rs.Columns
		for _, cf := range cfs {
			options.OutputColumns = append(options.OutputColumns, cf.Name)
		}
	} else {
		for _, col := range options.OutputColumns {
			cf, ok := rs.ColumnsByName[col]
			if !ok {
				return 0, nil, NewError(CommandError, map[string]interface{}{
					"column": col,
					"error":  "The column has no associated field.",
				})
			}
			cfs = append(cfs, cf)
		}
	}
	result, resp, err := db.LogicalSelect(logicalTable, shardKey, options)
	if err != nil {
		return 0, nil, err
	}
	defer result.Close()
	data, err := ioutil.ReadAll(result)
	if err != nil {
		return 0, resp, err
	}
	if resp.Err() != nil {
		return 0, resp, err
	}
	n, err := db.parseRows(rows, data, cfs)
	return n, resp, err
}

// DBLogicalShard is a result of logical_shard_list.
type DBLogicalShard struct {
	Name string `json:"name"`
}

// LogicalShardList executes logical_shard_list.
func (db *DB) LogicalShardList(logicalTable string) ([]DBLogicalShard, Response, error) {
	resp, err := db.Invoke("logical_shard_list", map[string]interface{}{
		"logical_table": logicalTable,
	}, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result []DBLogicalShard
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// DBLogicalTableRemoveOptions stores options for DB.LogicalTableRemove.
type DBLogicalTableRemoveOptions struct {
	Min       time.Time //--min
	MinBorder bool      // --min_border
	Max       time.Time // --max
	MaxBorder bool      // --max_border
	Dependent bool      // --dependent
	Force     bool      // --force
}

// NewDBLogicalTableRemoveOptions returns the default DBLogicalTableRemoveOptions.
func NewDBLogicalTableRemoveOptions() *DBLogicalTableRemoveOptions {
	return &DBLogicalTableRemoveOptions{
		MinBorder: true,
		MaxBorder: true,
	}
}

// LogicalTableRemove executes logical_table_remove.
func (db *DB) LogicalTableRemove(logicalTable, shardKey string, options *DBLogicalTableRemoveOptions) (bool, Response, error) {
	params := map[string]interface{}{
		"logical_table": logicalTable,
		"shard_key":     shardKey,
	}
	if options == nil {
		options = NewDBLogicalTableRemoveOptions()
	}
	if !options.Min.IsZero() {
		params["min"] = options.Min
	}
	params["min_border"] = options.MinBorder
	if !options.Max.IsZero() {
		params["max"] = options.Max
	}
	params["max_border"] = options.MaxBorder
	params["dependent"] = options.Dependent
	params["force"] = options.Force
	resp, err := db.Invoke("logical_table_remove", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBNormalizedText is a result of normalize.
type DBNormalizedText struct {
	Normalized string   `json:"normalized"`
	Types      []string `json:"types"`
	Checks     []int    `json:"checks"`
}

// Normalize executes normalize.
func (db *DB) Normalize(normalizer, str string, flags []string) (*DBNormalizedText, Response, error) {
	params := map[string]interface{}{
		"normalizer": normalizer,
		"string":     str,
	}
	if flags != nil {
		params["flags"] = flags
	}
	resp, err := db.Invoke("normalize", params, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result DBNormalizedText
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return &result, resp, nil
}

// DBNormalizer is a result of tokenizer_list.
type DBNormalizer struct {
	Name string `json:"name"`
}

// NormalizerList executes normalizer_list.
func (db *DB) NormalizerList() ([]DBNormalizer, Response, error) {
	resp, err := db.Invoke("normalizer_list", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result []DBNormalizer
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// ObjectExist executes object_exist.
func (db *DB) ObjectExist(name string) (bool, Response, error) {
	resp, err := db.Invoke("object_exist", map[string]interface{}{
		"name": name,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBObjectIDName is a part of DBObject*.
type DBObjectIDName struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// DBObjectType is a part of DBObject*.
type DBObjectType struct {
	ID   int            `json:"id"`
	Name string         `json:"name"`
	Type DBObjectIDName `json:"type"`
	Size int            `json:"size"`
}

// DBObjectColumnType is a part of DBObjectColumn.
type DBObjectColumnType struct {
	Name string         `json:"name"`
	Raw  DBObjectIDName `json:"raw"`
}

// DBObjectColumnStatistics is a part of DBObjectColumn.
type DBObjectColumnStatistics struct {
	MaxSectionID              int   `json:"max_section_id"`
	NGarbageSegments          int   `json:"n_garbage_segments"`
	MaxArraySegmentID         int   `json:"max_array_segment_id"`
	NArraySegments            int   `json:"n_array_segments"`
	MaxBufferSegmentID        int   `json:"max_buffer_segment_id"`
	NBufferSegments           int   `json:"n_buffer_segments"`
	MaxInUsePhysicalSegmentID int   `json:"max_in_use_physical_segment_id"`
	NUnmanagedSegments        int   `json:"n_unmanaged_segments"`
	TotalChunkSize            int   `json:"total_chunk_size"`
	MaxInUseChunkID           int   `json:"max_in_use_chunk_id"`
	NGarbageChunks            []int `json:"n_garbage_chunks"`
}

// DBObjectColumnValue is a part of DBObjectColumn.
type DBObjectColumnValue struct {
	Type       DBObjectType             `json:"type"`
	Section    bool                     `json:"section"`
	Weight     bool                     `json:"weight"`
	Position   bool                     `json:"position"`
	Size       int                      `json:"size"`
	Statistics DBObjectColumnStatistics `json:"statistics"`
}

// DBObjectColumnSource is a par of DBObjectColumn.
type DBObjectColumnSource struct {
	ID       int           `json:"id"`
	Name     string        `json:"name"`
	Table    DBObjectTable `json:"table"`
	FullName string        `json:"full_name"`
}

// DBObjectColumn is a result of object_inspect.
type DBObjectColumn struct {
	ID       int                    `json:"id"`
	Name     string                 `json:"name"`
	Table    DBObjectTable          `json:"table"`
	FullName string                 `json:"full_name"`
	Type     DBObjectColumnType     `json:"type"`
	Value    DBObjectColumnValue    `json:"value"`
	Sources  []DBObjectColumnSource `json:"sources"`
}

// DBObjectKey is a part of DBObjectTable.
type DBObjectKey struct {
	Type         DBObjectType `json:"type"`
	TotalSize    int          `json:"total_size"`
	MaxTotalSize int          `json:"max_total_size"`
}

// DBObjectValue is a part of DBObjectTable.
type DBObjectValue struct {
	Type DBObjectType `json:"type"`
}

// DBObjectTable stores a result of object_inspect.
type DBObjectTable struct {
	ID       int            `json:"id"`
	Name     string         `json:"name"`
	Type     DBObjectIDName `json:"type"`
	Key      DBObjectKey    `json:"key"`
	Value    DBObjectValue  `json:"value"`
	NRecords int            `json:"n_records"`
}

// DBObjectDatabase stores a result of object_inspect.
type DBObjectDatabase struct {
	Type      DBObjectIDName `json:"type"`
	NameTable DBObjectTable  `json:"name_table"`
}

// ObjectInspect executes object_inspect.
func (db *DB) ObjectInspect(name string) (interface{}, Response, error) {
	var params map[string]interface{}
	if name != "" {
		params = map[string]interface{}{
			"name": name,
		}
	}
	resp, err := db.Invoke("object_inspect", params, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	switch {
	case name == "": // Database
		var result DBObjectDatabase
		if err := json.Unmarshal(jsonData, &result); err != nil {
			if resp.Err() != nil {
				return nil, resp, nil
			}
			return nil, resp, NewError(ResponseError, map[string]interface{}{
				"method": "json.Unmarshal",
				"error":  err.Error(),
			})
		}
		return &result, resp, nil
	case strings.Contains(name, "."): // Column
		var result DBObjectColumn
		if err := json.Unmarshal(jsonData, &result); err != nil {
			if resp.Err() != nil {
				return nil, resp, nil
			}
			return nil, resp, NewError(ResponseError, map[string]interface{}{
				"method": "json.Unmarshal",
				"error":  err.Error(),
			})
		}
		return &result, resp, nil
	default: // Table of type
		type SizeNRecords struct {
			Size     *int `json:"size"`
			NRecords *int `json:"n_records"`
		}
		var sizeNRecords SizeNRecords
		if err := json.Unmarshal(jsonData, &sizeNRecords); err != nil {
			if resp.Err() != nil {
				return nil, resp, nil
			}
			return nil, resp, NewError(ResponseError, map[string]interface{}{
				"method": "json.Unmarshal",
				"error":  err.Error(),
			})
		}
		switch {
		case sizeNRecords.Size != nil:
			var result DBObjectType
			if err := json.Unmarshal(jsonData, &result); err != nil {
				if resp.Err() != nil {
					return nil, resp, nil
				}
				return nil, resp, NewError(ResponseError, map[string]interface{}{
					"method": "json.Unmarshal",
					"error":  err.Error(),
				})
			}
			return &result, resp, nil
		case sizeNRecords.NRecords != nil:
			var result DBObjectTable
			if err := json.Unmarshal(jsonData, &result); err != nil {
				if resp.Err() != nil {
					return nil, resp, nil
				}
				return nil, resp, NewError(ResponseError, map[string]interface{}{
					"method": "json.Unmarshal",
					"error":  err.Error(),
				})
			}
			return &result, resp, nil
		default:
			if resp.Err() != nil {
				return nil, resp, nil
			}
			return nil, resp, NewError(ResponseError, map[string]interface{}{
				"command": "object_inspect",
				"error":   "The response format is not invalid.",
			})
		}
	}
}

// DBObjectFlags is a part of DBObject.
type DBObjectFlags struct {
	Names string `json:"names"`
	Value int    `json:"value"`
}

// DBObject stores options for DB.ObjectList.
type DBObject struct {
	ID           int              `json:"id"`
	Name         string           `json:"name"`
	Opened       bool             `json:"opened"`
	ValueSize    int              `json:"value_size"`
	NElements    int              `json:"n_elements"`
	Type         DBObjectIDName   `json:"type"`
	Flags        DBObjectFlags    `json:"flags"`
	Path         string           `json:"path"`
	Size         int              `json:"size"`
	PluginID     int              `json:"plugin_id"`
	Range        *DBObjectIDName  `json:"range"`
	TokenFilters []DBObjectIDName `json:"token_filters"`
	Sources      []DBObjectIDName `json:"sources"`
}

// ObjectList executes object_list.
func (db *DB) ObjectList() (map[string]*DBObject, Response, error) {
	resp, err := db.Invoke("object_list", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result map[string]*DBObject
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// ObjectRemove executes object_remove.
func (db *DB) ObjectRemove(name string, force bool) (bool, Response, error) {
	resp, err := db.Invoke("object_remove", map[string]interface{}{
		"name":  name,
		"force": force,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// PluginRegister executes plugin_register.
func (db *DB) PluginRegister(name string) (bool, Response, error) {
	resp, err := db.Invoke("plugin_register", map[string]interface{}{
		"name": name,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// PluginUnregister executes plugin_unregister.
func (db *DB) PluginUnregister(name string) (bool, Response, error) {
	resp, err := db.Invoke("plugin_unregister", map[string]interface{}{
		"name": name,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// Quit executes quit.
func (db *DB) Quit() (bool, Response, error) {
	resp, err := db.Invoke("quit", nil, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// Reindex executes reindex.
func (db *DB) Reindex(target string) (bool, Response, error) {
	var params map[string]interface{}
	if target != "" {
		params = map[string]interface{}{
			"target_name": target,
		}
	}
	resp, err := db.Invoke("reindex", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// Restore reads commands from r and executes the commands.
// If w is not nil, responses are written into w.
// Restore returns the number of commands executed and the first error.
func (db *DB) Restore(r io.Reader, w io.Writer, stopOnError bool) (n int, err error) {
	cr := NewCommandReader(r)
	for {
		cmd, e := cr.Read()
		if e != nil {
			if e != io.EOF && err == nil {
				err = e
			}
			return
		}
		n++
		resp, e := db.Query(cmd)
		if e == nil {
			if w != nil {
				if _, e = io.Copy(w, resp); e != nil && err == nil {
					if _, ok := e.(*Error); !ok {
						e = NewError(UnknownError, map[string]interface{}{
							"error": e.Error(),
						})
					}
					err = e
				}
				if _, e := w.Write([]byte("\n")); e != nil && err == nil {
					err = NewError(UnknownError, map[string]interface{}{
						"error": e.Error(),
					})
				}
			}
			if e = resp.Close(); e == nil {
				e = resp.Err()
			}
		}
		if e != nil && err == nil {
			err = e
		}
		if err != nil && stopOnError {
			return
		}
	}
}

// RequestCancel executes request_cancel.
func (db *DB) RequestCancel(id int) (bool, Response, error) {
	resp, err := db.Invoke("request_cancel", map[string]interface{}{
		"id": id,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return false, resp, err
	}
	type Result struct {
		ID       int  `json:"id"`
		Canceled bool `json:"canceled"`
	}
	var result Result
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return false, resp, nil
		}
		return false, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result.Canceled, resp, nil
}

// RubyEval executes ruby_eval.
func (db *DB) RubyEval(script string) (interface{}, Response, error) {
	resp, err := db.Invoke("ruby_eval", map[string]interface{}{
		"script": script,
	}, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	type Result struct {
		Value interface{} `json:"vlaue"`
	}
	var result Result
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return false, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result.Value, resp, nil
}

// RubyLoad executes ruby_load.
func (db *DB) RubyLoad(path string) (interface{}, Response, error) {
	resp, err := db.Invoke("ruby_load", map[string]interface{}{
		"path": path,
	}, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	type Result struct {
		Value interface{} `json:"vlaue"`
	}
	var result Result
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return false, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result.Value, resp, nil
}

// DBSchemaPlugin is a part of DBSchema.
type DBSchemaPlugin struct {
	Name string `json:"name"`
}

// DBSchemaType is a part of DBSchema.
type DBSchemaType struct {
	Name           string `json:"name"`
	Size           int    `json:"size"`
	CanBeKeyType   bool   `json:"can_be_key_type"`
	CanBeValueType bool   `json:"can_be_value_type"`
}

// DBSchemaTokenizer is a part of DBSchema.
type DBSchemaTokenizer struct {
	Name string `json:"name"`
}

// DBSchemaNormalizer is a part of DBSchema.
type DBSchemaNormalizer struct {
	Name string `json:"name"`
}

// DBSchemaTokenFilter is a part of DBSchema.
type DBSchemaTokenFilter struct {
	Name string `json:"name"`
}

// DBSchemaKeyType is a part of DBSchema.
type DBSchemaKeyType struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DBSchemaValueType is a part of DBSchema.
type DBSchemaValueType struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// DBSchemaIndex is a part of DBSchema.
type DBSchemaIndex struct {
	Table    string `json:"table"`
	Section  int    `json:"section"`
	Name     string `json:"name"`
	FullName string `json:"full_name"`
}

// DBSchemaCommand is a part of DBSchema.
type DBSchemaCommand struct {
	Name        string            `json:"name"`
	Arguments   map[string]string `json:"arguments"`
	CommandLine string            `json:"command_line"`
}

// DBSchemaSource is a part of DBSchema.
type DBSchemaSource struct {
	Name     string `json:"name"`
	Table    string `json:"table"`
	FullName string `json:"full_name"`
}

// DBSchemaColumn is a part of DBSchema.
type DBSchemaColumn struct {
	Name      string            `json:"name"`
	Table     string            `json:"table"`
	FullName  string            `json:"full_name"`
	Type      string            `json:"type"`
	ValueType DBSchemaValueType `json:"value_type"`
	Compress  string            `json:"compress"`
	Section   bool              `json:"section"`
	Weight    bool              `json:"weight"`
	Position  bool              `json:"position"`
	Sources   []DBSchemaSource  `json:"sources"`
	Indexes   []DBSchemaIndex   `json:"indexes"`
	Command   DBSchemaCommand   `json:"command"`
}

// DBSchemaTable is a part of DBSchema.
type DBSchemaTable struct {
	Name         string                    `json:"name"`
	Type         string                    `json:"type"`
	KeyType      *DBSchemaKeyType          `json:"key_type"`
	ValueType    *DBSchemaValueType        `json:"value_type"`
	Tokenizer    *DBSchemaTokenizer        `json:"tokenizer"`
	Normalizer   *DBSchemaNormalizer       `json:"normalizer"`
	TokenFilters []DBSchemaTokenFilter     `json:"token_filters"`
	Indexes      []DBSchemaIndex           `json:"indexes"`
	Command      DBSchemaCommand           `json:"command"`
	Columns      map[string]DBSchemaColumn `json:"columns"`
}

// DBSchema is a result of schema.
type DBSchema struct {
	Plugins      map[string]DBSchemaPlugin      `json:"plugins"`
	Types        map[string]DBSchemaType        `json:"types"`
	Tokenizers   map[string]DBSchemaTokenizer   `json:"tokenizers"`
	Normalizers  map[string]DBSchemaNormalizer  `json:"normalizers"`
	TokenFilters map[string]DBSchemaTokenFilter `json:"token_filters"`
	Tables       map[string]DBSchemaTable       `json:"tables"`
}

// Schema executes schema.
func (db *DB) Schema() (*DBSchema, Response, error) {
	resp, err := db.Invoke("schema", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result DBSchema
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return &result, resp, nil
}

// DBSelectOptionsColumn stores --columns[NAME].
type DBSelectOptionsColumn struct {
	Stage           string   // --columns[NAME].stage
	Flags           []string // --columns[NAME].flags
	Type            string   // --columns[NAME].type
	Value           string   // --columns[NAME].value
	WindowSortKeys  []string // --columns[NAME].window.sort_keys
	WindowGroupKeys []string // --columns[NAME].window.group_keys
}

// NewDBSelectOptionsColumn returns the default DBSelectOptionsColumn.
func NewDBSelectOptionsColumn() *DBSelectOptionsColumn {
	return &DBSelectOptionsColumn{}
}

// setParams sets options to params.
func (options *DBSelectOptionsColumn) setParams(prefix string, params map[string]interface{}) {
	// FIXME: slice options are not supported.
	params[prefix+".stage"] = options.Stage
	if options.Flags != nil {
		params[prefix+".flags"] = options.Flags
	}
	params[prefix+".type"] = options.Type
	params[prefix+".value"] = options.Value
	if options.WindowSortKeys != nil {
		params[prefix+".window.sort_keys"] = options.WindowSortKeys
	}
	if options.WindowGroupKeys != nil {
		params[prefix+".window.group_keys"] = options.WindowGroupKeys
	}
}

// DBSelectOptionsDrilldown stores --drilldowns[LABEL].
type DBSelectOptionsDrilldown struct {
	Keys          []string // --drilldowns[LABEL].keys
	SortKeys      []string // --drilldowns[LABEL].sort_keys
	OutputColumns []string // --drilldowns[LABEL].output_columns
	Offset        int      // --drilldowns[LABEL].offset
	Limit         int      // --drilldowns[LABEL].limit
	CalcTypes     []string // --drilldowns[LABEL].calc_types
	CalcTarget    string   // --drilldowns[LABEL].calc_target
	Filter        string   // --drilldowns[LABEL].filter
	Columns       map[string]*DBSelectOptionsColumn
}

// NewDBSelectOptionsDrilldown returns the default DBSelectOptionsDrilldown.
func NewDBSelectOptionsDrilldown() *DBSelectOptionsDrilldown {
	return &DBSelectOptionsDrilldown{
		Limit: 10,
	}
}

// setParams sets options to params.
func (options *DBSelectOptionsDrilldown) setParams(prefix string, params map[string]interface{}) {
	// FIXME: slice options are not supported.
	params[prefix+".keys"] = options.Keys
	if options.SortKeys != nil {
		params[prefix+".sort_keys"] = options.SortKeys
	}
	if options.OutputColumns != nil {
		params[prefix+".output_columns"] = options.OutputColumns
	}
	params[prefix+".offset"] = options.Offset
	params[prefix+".limit"] = options.Limit
	if options.CalcTypes != nil {
		params[prefix+".calc_types"] = options.CalcTypes
	}
	params[prefix+".calc_target"] = options.CalcTarget
	params[prefix+".filter"] = options.Filter
	for name, col := range options.Columns {
		col.setParams(prefix+".columns["+name+"]", params)
	}
}

// DBSelectOptions stores options for DB.Select.
// http://groonga.org/docs/reference/commands/select.html
type DBSelectOptions struct {
	MatchColumns             []string // --match_columns
	Query                    string   // --query
	Filter                   string   // --filter
	Scorer                   string   // --scorer
	SortKeys                 []string // --sort_keys
	OutputColumns            []string // --output_columns
	Offset                   int      // --offset
	Limit                    int      // --limit
	Drilldown                []string // --drilldown
	DrilldownSortKeys        []string // --drilldown_sort_keys
	DrilldownOutputColumns   []string // --drilldown_output_columns
	DrilldownOffset          int      // --drilldown_offset
	DrilldownLimit           int      // --drilldown_limit
	Cache                    bool     // --cache
	MatchEscalationThreshold int      // --match_escalation_threshold
	QueryExpansion           string   // --query_expansion
	QueryFlags               []string // --query_flags
	QueryExpander            string   // --query_expander
	Adjuster                 string   // --adjuster
	DrilldownCalcTypes       []string // --drilldown_calc_types
	DrilldownCalcTarget      string   // --drilldown_calc_target
	DrilldownFilter          string   // --drilldown_filter
	Columns                  map[string]*DBSelectOptionsColumn
	Drilldowns               map[string]*DBSelectOptionsDrilldown
}

// NewDBSelectOptions returns the default DBSelectOptions.
func NewDBSelectOptions() *DBSelectOptions {
	return &DBSelectOptions{
		Limit:          10,
		DrilldownLimit: 10,
	}
}

// Select executes select.
// On success, it is the caller's responsibility to close the result.
func (db *DB) Select(tbl string, options *DBSelectOptions) (io.ReadCloser, Response, error) {
	if options == nil {
		options = NewDBSelectOptions()
	}
	params := map[string]interface{}{
		"command_version": 2,
		"table":           tbl,
	}
	if options.MatchColumns != nil {
		params["match_columns"] = options.MatchColumns
	}
	if options.Query != "" {
		params["query"] = options.Query
	}
	if options.Filter != "" {
		params["filter"] = options.Filter
	}
	if options.Scorer != "" {
		params["scorer"] = options.Scorer
	}
	if options.SortKeys != nil {
		params["sort_keys"] = options.SortKeys
	}
	if options.OutputColumns != nil {
		params["output_columns"] = options.OutputColumns
	}
	if options.OutputColumns != nil {
		params["query"] = options.Query
	}
	if options.Offset != 0 {
		params["offset"] = options.Offset
	}
	if options.Limit != 10 {
		params["limit"] = options.Limit
	}
	if options.Drilldown != nil {
		params["drilldown"] = options.Drilldown
	}
	if options.DrilldownSortKeys != nil {
		params["drilldown_sort_keys"] = options.DrilldownSortKeys
	}
	if options.DrilldownOutputColumns != nil {
		params["drilldown_output_columns"] = options.DrilldownOutputColumns
	}
	if options.DrilldownOffset != 0 {
		params["drilldown_offset"] = options.DrilldownOffset
	}
	if options.DrilldownLimit != 10 {
		params["drilldown_limit"] = options.DrilldownLimit
	}
	if !options.Cache {
		params["cache"] = options.Cache
	}
	if options.MatchEscalationThreshold != 0 {
		params["match_escalation_threshold"] = options.MatchEscalationThreshold
	}
	if options.QueryExpansion != "" {
		params["query_expansion"] = options.QueryExpansion
	}
	if options.QueryFlags != nil {
		params["query_flags"] = options.QueryFlags
	}
	if options.QueryExpander != "" {
		params["query_expander"] = options.QueryExpander
	}
	if options.Adjuster != "" {
		params["adjuster"] = options.Adjuster
	}
	if options.DrilldownCalcTypes != nil {
		params["drilldown_calc_types"] = options.DrilldownCalcTypes
	}
	if options.DrilldownCalcTarget != "" {
		params["drilldown_calc_target"] = options.DrilldownCalcTarget
	}
	if options.DrilldownFilter != "" {
		params["drilldown_filter"] = options.DrilldownFilter
	}
	for name, col := range options.Columns {
		col.setParams("--columns["+name+"]", params)
	}
	for label, drilldown := range options.Drilldowns {
		drilldown.setParams("--drilldowns["+label+"]", params)
	}
	resp, err := db.Invoke("select", params, nil)
	if err != nil {
		return nil, nil, err
	}
	return resp, resp, err
}

// parseRows parses rows.
func (db *DB) parseRows(rows interface{}, data []byte, cfs []*ColumnField) (int, error) {
	var raw [][][]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return 0, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}

	var nHits int
	if err := json.Unmarshal(raw[0][0][0], &nHits); err != nil {
		return 0, err
	}

	rawCols := raw[0][1]
	nCols := len(rawCols)
	if nCols != len(cfs) {
		// Remove _score from fields if _score does not exist in the response.
		for i, cf := range cfs {
			if cf.Name == "_score" {
				hasScore := false
				for _, rawCol := range rawCols {
					var nameType []string
					if err := json.Unmarshal(rawCol, &nameType); err != nil {
						return 0, NewError(ResponseError, map[string]interface{}{
							"method": "json.Unmarshal",
							"error":  err.Error(),
						})
					}
					if nameType[0] == "_score" {
						hasScore = true
						break
					}
				}
				if !hasScore {
					for j := i + 1; j < len(cfs); j++ {
						cfs[j-1] = cfs[j]
					}
					cfs = cfs[:len(cfs)-1]
				}
				break
			}
		}
		if nCols != len(cfs) {
			return 0, NewError(ResponseError, map[string]interface{}{
				"nFields": len(cfs),
				"nCols":   nCols,
				"error":   "nFields and nColumns must be same.",
			})
		}
	}
	// FIXME: the following check disallows functions.
	//	for i, rawCol := range rawCols {
	//		var nameType []string
	//		if err := json.Unmarshal(rawCol, &nameType); err != nil {
	//			return 0, err
	//		}
	//		if nameType[0] != fields[i].ColumnName() {
	//			return 0, fmt.Errorf("column %#v expected but column %#v actual",
	//				fields[i].ColumnName(), nameType[0])
	//		}
	//	}

	rawRecs := raw[0][2:]
	nRecs := len(rawRecs)

	recs := reflect.ValueOf(rows).Elem()
	recs.Set(reflect.MakeSlice(recs.Type(), nRecs, nRecs))
	for i := 0; i < nRecs; i++ {
		rec := recs.Index(i)
		for j, cf := range cfs {
			ptr := rec.Field(cf.Index).Addr()
			switch v := ptr.Interface().(type) {
			case *bool:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *float32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *float64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *string:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *time.Time:
				var f float64
				if err := json.Unmarshal(rawRecs[i][j], &f); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
				*v = time.Unix(int64(f), int64(f*1000000)%1000000)
			case *[]bool:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]float32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]float64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]string:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]time.Time:
				var f []float64
				if err := json.Unmarshal(rawRecs[i][j], &f); err != nil {
					return 0, NewError(ResponseError, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
				*v = make([]time.Time, len(f))
				for i := range f {
					(*v)[i] = time.Unix(int64(f[i]), int64(f[i]*1000000)%1000000)
				}
			}
		}
	}
	return nHits, nil

}

// SelectRows executes select.
func (db *DB) SelectRows(tbl string, rows interface{}, options *DBSelectOptions) (int, Response, error) {
	if options == nil {
		options = NewDBSelectOptions()
	}
	rs, err := GetRowStruct(rows)
	if err != nil {
		return 0, nil, err
	}
	var cfs []*ColumnField
	if options.OutputColumns == nil {
		cfs = rs.Columns
		for _, cf := range cfs {
			options.OutputColumns = append(options.OutputColumns, cf.Name)
		}
	} else {
		for _, col := range options.OutputColumns {
			cf, ok := rs.ColumnsByName[col]
			if !ok {
				return 0, nil, NewError(CommandError, map[string]interface{}{
					"column": col,
					"error":  "The column has no associated field.",
				})
			}
			cfs = append(cfs, cf)
		}
	}
	result, resp, err := db.Select(tbl, options)
	if err != nil {
		return 0, nil, err
	}
	defer result.Close()
	data, err := ioutil.ReadAll(result)
	if err != nil {
		return 0, resp, err
	}
	if resp.Err() != nil {
		return 0, resp, err
	}
	n, err := db.parseRows(rows, data, cfs)
	if err != nil {
		if resp.Err() != nil {
			return n, resp, nil
		}
		return n, resp, err
	}
	return n, resp, nil
}

// Shutdown executes shutdown.
func (db *DB) Shutdown() (bool, Response, error) {
	resp, err := db.Invoke("shutdown", nil, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBStatus is a response of status.
type DBStatus struct {
	AllocCount            int           `json:"alloc_count"`
	CacheHitRate          float64       `json:"cache_hit_rate"`
	CommandVersion        int           `json:"command_version"`
	DefaultCommandVersion int           `json:"default_command_version"`
	MaxCommandVersion     int           `json:"max_command_version"`
	NQueries              int           `json:"n_queries"`
	StartTime             time.Time     `json:"start_time"`
	Uptime                time.Duration `json:"uptime"`
	Version               string        `json:"version"`
}

// Status executes status.
func (db *DB) Status() (*DBStatus, Response, error) {
	resp, err := db.Invoke("status", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var data map[string]interface{}
	if err := json.Unmarshal(jsonData, &data); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	var result DBStatus
	if v, ok := data["alloc_count"]; ok {
		if v, ok := v.(float64); ok {
			result.AllocCount = int(v)
		}
	}
	if v, ok := data["cache_hit_rate"]; ok {
		if v, ok := v.(float64); ok {
			result.CacheHitRate = v
		}
	}
	if v, ok := data["command_version"]; ok {
		if v, ok := v.(float64); ok {
			result.CommandVersion = int(v)
		}
	}
	if v, ok := data["default_command_version"]; ok {
		if v, ok := v.(float64); ok {
			result.DefaultCommandVersion = int(v)
		}
	}
	if v, ok := data["max_command_version"]; ok {
		if v, ok := v.(float64); ok {
			result.MaxCommandVersion = int(v)
		}
	}
	if v, ok := data["n_queries"]; ok {
		if v, ok := v.(float64); ok {
			result.NQueries = int(v)
		}
	}
	if v, ok := data["start_time"]; ok {
		if v, ok := v.(float64); ok {
			result.StartTime = time.Unix(int64(v), 0)
		}
	}
	if v, ok := data["uptime"]; ok {
		if v, ok := v.(float64); ok {
			result.Uptime = time.Duration(time.Duration(v) * time.Second)
		}
	}
	if v, ok := data["version"]; ok {
		if v, ok := v.(string); ok {
			result.Version = v
		}
	}
	return &result, resp, nil
}

// TableCopy executes table_copy.
func (db *DB) TableCopy(from, to string) (bool, Response, error) {
	resp, err := db.Invoke("table_copy", map[string]interface{}{
		"from": from,
		"to":   to,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBTableCreateOptions stores options for DB.TableCreate.
// http://groonga.org/docs/reference/commands/table_create.html
type DBTableCreateOptions struct {
	Flags            []string // --flags
	KeyType          string   // --key_type
	ValueType        string   // --value_type
	DefaultTokenizer string   // --default_tokenizer
	Normalizer       string   // --normalizer
	TokenFilters     []string // --token_filters
}

// NewDBTableCreateOptions returns the default DBTableCreateOptions.
func NewDBTableCreateOptions() *DBTableCreateOptions {
	return &DBTableCreateOptions{}
}

// TableCreate executes table_create.
func (db *DB) TableCreate(name string, options *DBTableCreateOptions) (bool, Response, error) {
	if options == nil {
		options = NewDBTableCreateOptions()
	}
	params := map[string]interface{}{
		"name": name,
	}
	flags := options.Flags
	var keyFlag string
	if options.Flags != nil {
		for _, flag := range flags {
			switch flag {
			case "TABLE_NO_KEY":
				if keyFlag != "" {
					return false, nil, NewError(CommandError, map[string]interface{}{
						"flags": flags,
						"error": "The combination of flags is wrong.",
					})
				}
				if options.KeyType != "" {
					return false, nil, NewError(CommandError, map[string]interface{}{
						"flags":    flags,
						"key_type": options.KeyType,
						"error":    "TABLE_NO_KEY denies key_type.",
					})
				}
				keyFlag = flag
			case "TABLE_HASH_KEY", "TABLE_PAT_KEY", "TABLE_DAT_KEY":
				if keyFlag != "" {
					return false, nil, NewError(CommandError, map[string]interface{}{
						"flags": flags,
						"error": "The combination of flags is wrong.",
					})
				}
				if options.KeyType == "" {
					return false, nil, NewError(CommandError, map[string]interface{}{
						"flags":    flags,
						"key_type": options.KeyType,
						"error":    fmt.Sprintf("%s requires key_type.", flag),
					})
				}
				keyFlag = flag
			}
		}
	}
	if keyFlag == "" {
		if options.KeyType == "" {
			keyFlag = "TABLE_NO_KEY"
		} else {
			keyFlag = "TABLE_HASH_KEY"
		}
		if len(flags) == 0 {
			flags = append(flags, keyFlag)
		}
	}
	params["flags"] = flags
	if options.KeyType != "" {
		params["key_type"] = options.KeyType
	}
	if options.ValueType != "" {
		params["value_type"] = options.ValueType
	}
	if options.DefaultTokenizer != "" {
		params["default_tokenizer"] = options.DefaultTokenizer
	}
	if options.Normalizer != "" {
		params["normalizer"] = options.Normalizer
	}
	if options.TokenFilters != nil {
		params["token_filters"] = options.TokenFilters
	}
	resp, err := db.Invoke("table_create", params, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBTable is a result of table_list.
type DBTable struct {
	ID               uint32   `json:"id"`
	Name             string   `json:"name"`
	Path             string   `json:"path"`
	Flags            []string `json:"flags"`
	Domain           string   `json:"domain"`
	Range            string   `json:"range"`
	DefaultTokenizer string   `json:"default_tokenizer"`
	Normalizer       string   `json:"normalizer"`
}

// TableList executes table_list.
func (db *DB) TableList() ([]DBTable, Response, error) {
	resp, err := db.Invoke("table_list", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result [][]interface{}
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	if len(result) == 0 {
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"error": "The result is empty.",
		})
	}
	var fields []string
	for _, meta := range result[0] {
		if values, ok := meta.([]interface{}); ok {
			if field, ok := values[0].(string); ok {
				fields = append(fields, field)
			}
		}
	}
	var tables []DBTable
	for _, values := range result[1:] {
		var table DBTable
		for i := 0; i < len(fields) && i < len(values); i++ {
			switch fields[i] {
			case "id":
				if v, ok := values[i].(float64); ok {
					table.ID = uint32(v)
				}
			case "name":
				if v, ok := values[i].(string); ok {
					table.Name = v
				}
			case "path":
				if v, ok := values[i].(string); ok {
					table.Path = v
				}
			case "flags":
				if v, ok := values[i].(string); ok {
					table.Flags = strings.Split(v, "|")
				}
			case "domain":
				if v, ok := values[i].(string); ok {
					table.Domain = v
				}
			case "range":
				if v, ok := values[i].(string); ok {
					table.Range = v
				}
			case "default_tokenizer":
				if v, ok := values[i].(string); ok {
					table.DefaultTokenizer = v
				}
			case "normalizer":
				if v, ok := values[i].(string); ok {
					table.Normalizer = v
				}
			}
		}
		tables = append(tables, table)
	}
	return tables, resp, nil
}

// TableRemove executes table_remove.
func (db *DB) TableRemove(name string, dependent bool) (bool, Response, error) {
	resp, err := db.Invoke("table_remove", map[string]interface{}{
		"name":      name,
		"dependent": dependent,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// TableRename executes table_rename.
func (db *DB) TableRename(name, newName string) (bool, Response, error) {
	resp, err := db.Invoke("table_rename", map[string]interface{}{
		"name":     name,
		"new_name": newName,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}

// DBTableTokenizeOptions is options of DB.TableTokenize.
type DBTableTokenizeOptions struct {
	Flags       []string
	Mode        string
	IndexColumn string
}

// NewDBTableTokenizeOptions returns the default DBTableTokenizeOptions.
func NewDBTableTokenizeOptions() *DBTableTokenizeOptions {
	return &DBTableTokenizeOptions{}
}

// DBToken is a result of table_tokenize and tokenize.
type DBToken struct {
	Position    int    `json:"position"`
	ForcePrefix bool   `json:"force_prefix"`
	Value       string `json:"value"`
}

// TableTokenize executes tokenize.
func (db *DB) TableTokenize(tbl, str string, options *DBTableTokenizeOptions) ([]DBToken, Response, error) {
	if options == nil {
		options = NewDBTableTokenizeOptions()
	}
	params := map[string]interface{}{
		"table":  tbl,
		"string": str,
	}
	if options.Flags != nil {
		params["flags"] = options.Flags
	}
	if options.Mode != "" {
		params["mode"] = options.Mode
	}
	if options.IndexColumn != "" {
		params["index_column"] = options.IndexColumn
	}
	resp, err := db.Invoke("table_tokenize", params, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result []DBToken
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// ThreadLimit executes thread_limit.
// If max < 0, max is not passed to thread_limit.
func (db *DB) ThreadLimit(max int) (int, Response, error) {
	var params map[string]interface{}
	if max >= 0 {
		params = map[string]interface{}{
			"max": max,
		}
	}
	resp, err := db.Invoke("thread_limit", params, nil)
	if err != nil {
		return 0, nil, err
	}
	return db.recvInt(resp)
}

// DBTokenizeOptions is options of DB.Tokenize.
type DBTokenizeOptions struct {
	Normalizer   string
	Flags        []string
	Mode         string
	TokenFilters []string
}

// NewDBTokenizeOptions returns the default DBTokenizeOptions.
func NewDBTokenizeOptions() *DBTokenizeOptions {
	return &DBTokenizeOptions{}
}

// Tokenize executes tokenize.
func (db *DB) Tokenize(tokenizer, str string, options *DBTokenizeOptions) ([]DBToken, Response, error) {
	if options == nil {
		options = NewDBTokenizeOptions()
	}
	params := map[string]interface{}{
		"tokenizer": tokenizer,
		"string":    str,
	}
	if options.Normalizer != "" {
		params["normalizer"] = options.Normalizer
	}
	if options.Flags != nil {
		params["flags"] = options.Flags
	}
	if options.Mode != "" {
		params["mode"] = options.Mode
	}
	if options.TokenFilters != nil {
		params["token_filters"] = options.TokenFilters
	}
	resp, err := db.Invoke("tokenize", params, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result []DBToken
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// DBTokenizer is a result of tokenizer_list.
type DBTokenizer struct {
	Name string `json:"name"`
}

// TokenizerList executes tokenizer_list.
func (db *DB) TokenizerList() ([]DBTokenizer, Response, error) {
	resp, err := db.Invoke("tokenizer_list", nil, nil)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Close()
	jsonData, err := ioutil.ReadAll(resp)
	if err != nil {
		return nil, resp, err
	}
	var result []DBTokenizer
	if err := json.Unmarshal(jsonData, &result); err != nil {
		if resp.Err() != nil {
			return nil, resp, nil
		}
		return nil, resp, NewError(ResponseError, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// Truncate executes truncate.
func (db *DB) Truncate(target string) (bool, Response, error) {
	resp, err := db.Invoke("truncate", map[string]interface{}{
		"target_name": target,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	return db.recvBool(resp)
}
