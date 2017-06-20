package grnci

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
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
		return false, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return 0, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return "", resp, NewError(InvalidResponse, map[string]interface{}{
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
		return false, nil, NewError(InvalidCommand, map[string]interface{}{
			"from":  from,
			"error": "The from must contain a dot.",
		})
	}
	fromTable := from[:i]
	fromName := from[i+1:]
	if i = strings.IndexByte(to, '.'); i == -1 {
		return false, nil, NewError(InvalidCommand, map[string]interface{}{
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
		return false, nil, NewError(InvalidCommand, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	if len(result) == 0 {
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return false, nil, NewError(InvalidCommand, map[string]interface{}{
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
		return false, nil, NewError(InvalidCommand, map[string]interface{}{
			"name":  name,
			"error": "The name must contain a dot.",
		})
	}
	if j := strings.IndexByte(newName, '.'); j != -1 {
		if i != j || name[:i] != newName[:i] {
			return false, nil, NewError(InvalidCommand, map[string]interface{}{
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
// On success, it is the caller's responsibility to close the response.
func (db *DB) Dump(options *DBDumpOptions) (Response, error) {
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
	return db.Invoke("dump", params, nil)
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

// encodeRow encodes a row.
func (db *DB) encodeRow(body []byte, row reflect.Value, fis []*StructFieldInfo) []byte {
	body = append(body, '[')
	for i, fi := range fis {
		if i != 0 {
			body = append(body, ',')
		}
		body = encodeValue(body, row.Field(fi.Index))
	}
	body = append(body, ']')
	return body
}

// encodeRows encodes rows.
func (db *DB) encodeRows(body []byte, rows reflect.Value, fis []*StructFieldInfo) []byte {
	n := rows.Len()
	for i := 0; i < n; i++ {
		if i != 0 {
			body = append(body, ',')
		}
		row := rows.Index(i)
		body = db.encodeRow(body, row, fis)
	}
	log.Printf("body = %s", body)
	return body
}

// LoadRows executes load.
func (db *DB) LoadRows(tbl string, rows interface{}, options *DBLoadOptions) (int, Response, error) {
	if options == nil {
		options = NewDBLoadOptions()
	}
	si, err := GetStructInfo(rows)
	if err != nil {
		return 0, nil, err
	}
	var fis []*StructFieldInfo
	if options.Columns == nil {
		fis = si.Fields
		for _, fi := range fis {
			options.Columns = append(options.Columns, fi.ColumnName)
		}
	} else {
		for _, col := range options.Columns {
			fi, ok := si.FieldsByColumnName[col]
			if !ok {
				return 0, nil, NewError(InvalidCommand, map[string]interface{}{
					"column": col,
					"error":  "The column has no assciated field.",
				})
			}
			fis = append(fis, fi)
		}
	}

	body := []byte("[")
	v := reflect.ValueOf(rows)
	switch v.Kind() {
	case reflect.Ptr:
		if v.IsNil() {
			return 0, nil, NewError(InvalidCommand, map[string]interface{}{
				"rows":  nil,
				"error": "The rows must not be nil.",
			})
		}
		v = v.Elem()
		if v.Kind() != reflect.Struct {
			return 0, nil, NewError(InvalidCommand, map[string]interface{}{
				"type":  reflect.TypeOf(rows).Name(),
				"error": "The type is not supported.",
			})
		}
		body = db.encodeRow(body, v, fis)
	case reflect.Array, reflect.Slice:
		body = db.encodeRows(body, v, fis)
	case reflect.Struct:
		body = db.encodeRow(body, v, fis)
	default:
		return 0, nil, NewError(InvalidCommand, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return &result, resp, nil
}

// DBSelectOptionsColumn stores --columns[NAME].
type DBSelectOptionsColumn struct {
	Stage string // --columns[NAME].stage
	Type  string // --columns[NAME].type
	Value string // --columns[NAME].value
}

// NewDBSelectOptionsColumn returns the default DBSelectOptionsColumn.
func NewDBSelectOptionsColumn() *DBSelectOptionsColumn {
	return &DBSelectOptionsColumn{}
}

// DBSelectOptionsDrilldownColumn stores --drilldowns[LABEL].columns[NAME].
type DBSelectOptionsDrilldownColumn struct {
	Stage           string   // --drilldowns[LABEL].columns[NAME].stage
	Flags           string   // --drilldowns[LABEL].columns[NAME].flags
	Type            string   // --drilldowns[LABEL].columns[NAME].type
	Value           string   // --drilldowns[LABEL].columns[NAME].value
	WindowSortKeys  []string // --drilldowns[LABEL].columns[NAME].window.sort_keys
	WindowGroupKeys []string // --drilldowns[LABEL].columns[NAME].window.group_keys
}

// NewDBSelectOptionsDrilldownColumn returns the default DBSelectOptionsDrilldownColumn.
func NewDBSelectOptionsDrilldownColumn() *DBSelectOptionsDrilldownColumn {
	return &DBSelectOptionsDrilldownColumn{}
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
	Columns       map[string]*DBSelectOptionsDrilldownColumn
}

// NewDBSelectOptionsDrilldown returns the default DBSelectOptionsDrilldown.
func NewDBSelectOptionsDrilldown() *DBSelectOptionsDrilldown {
	return &DBSelectOptionsDrilldown{
		Limit: 10,
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
// On success, it is the caller's responsibility to close the response.
func (db *DB) Select(tbl string, options *DBSelectOptions) (Response, error) {
	if options == nil {
		options = NewDBSelectOptions()
	}
	params := map[string]interface{}{
		"table": tbl,
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
	return db.Invoke("select", params, nil)
}

// parseRows parses rows.
func (db *DB) parseRows(rows interface{}, data []byte, fis []*StructFieldInfo) (int, error) {
	var raw [][][]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return 0, NewError(InvalidResponse, map[string]interface{}{
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
	if nCols != len(fis) {
		// Remove _score from fields if _score does not exist in the response.
		for i, field := range fis {
			if field.ColumnName == "_score" {
				hasScore := false
				for _, rawCol := range rawCols {
					var nameType []string
					if err := json.Unmarshal(rawCol, &nameType); err != nil {
						return 0, NewError(InvalidResponse, map[string]interface{}{
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
					for j := i + 1; j < len(fis); j++ {
						fis[j-1] = fis[j]
					}
					fis = fis[:len(fis)-1]
				}
				break
			}
		}
		if nCols != len(fis) {
			return 0, NewError(InvalidResponse, map[string]interface{}{
				"nFields": len(fis),
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
		for j, field := range fis {
			ptr := rec.Field(field.Index).Addr()
			switch v := ptr.Interface().(type) {
			case *bool:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *int64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *uint64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *float32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *float64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *string:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *time.Time:
				var f float64
				if err := json.Unmarshal(rawRecs[i][j], &f); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
				*v = time.Unix(int64(f), int64(f*1000000)%1000000)
			case *[]bool:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]int64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint8:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint16:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]uint64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]float32:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]float64:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]string:
				if err := json.Unmarshal(rawRecs[i][j], v); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
						"method": "json.Unmarshal",
						"error":  err.Error(),
					})
				}
			case *[]time.Time:
				var f []float64
				if err := json.Unmarshal(rawRecs[i][j], &f); err != nil {
					return 0, NewError(InvalidResponse, map[string]interface{}{
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
	si, err := GetStructInfo(rows)
	if err != nil {
		return 0, nil, err
	}
	var fis []*StructFieldInfo
	if options.OutputColumns == nil {
		fis = si.Fields
		for _, fi := range fis {
			options.OutputColumns = append(options.OutputColumns, fi.ColumnName)
		}
	} else {
		for _, col := range options.OutputColumns {
			fi, ok := si.FieldsByColumnName[col]
			if !ok {
				return 0, nil, NewError(InvalidCommand, map[string]interface{}{
					"column": col,
					"error":  "The column has no assciated field.",
				})
			}
			fis = append(fis, fi)
		}
	}
	resp, err := db.Select(tbl, options)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Close()
	data, err := ioutil.ReadAll(resp)
	if err != nil {
		return 0, resp, err
	}
	if resp.Err() != nil {
		return 0, resp, err
	}
	n, err := db.parseRows(rows, data, fis)
	return n, resp, err
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
					return false, nil, NewError(InvalidCommand, map[string]interface{}{
						"flags": flags,
						"error": "The combination of flags is wrong.",
					})
				}
				if options.KeyType != "" {
					return false, nil, NewError(InvalidCommand, map[string]interface{}{
						"flags":    flags,
						"key_type": options.KeyType,
						"error":    "TABLE_NO_KEY denies key_type.",
					})
				}
				keyFlag = flag
			case "TABLE_HASH_KEY", "TABLE_PAT_KEY", "TABLE_DAT_KEY":
				if keyFlag != "" {
					return false, nil, NewError(InvalidCommand, map[string]interface{}{
						"flags": flags,
						"error": "The combination of flags is wrong.",
					})
				}
				if options.KeyType == "" {
					return false, nil, NewError(InvalidCommand, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	if len(result) == 0 {
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
		return nil, resp, NewError(InvalidResponse, map[string]interface{}{
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
