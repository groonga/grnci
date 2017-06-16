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

// ColumnCreate executes column_create.
func (db *DB) ColumnCreate(tbl, name, typ string, flags []string) (bool, Response, error) {
	cmd, err := NewCommand("column_create", map[string]interface{}{
		"table": tbl,
		"name":  name,
	})
	if err != nil {
		return false, nil, err
	}
	typFlag := "COLUMN_SCALAR"
	withSection := false
	src := ""
	if strings.HasPrefix(typ, "[]") {
		typFlag = "COLUMN_VECTOR"
		typ = typ[2:]
	} else if idx := strings.IndexByte(typ, '.'); idx != -1 {
		typFlag = "COLUMN_INDEX"
		src = typ[idx+1:]
		typ = typ[:idx]
		if idx := strings.IndexByte(src, ','); idx != -1 {
			withSection = true
		}
	}
	flags = append(flags, typFlag)
	if withSection {
		flags = append(flags, "WITH_SECTION")
	}
	if err := cmd.SetParam("flags", flags); err != nil {
		return false, nil, err
	}
	if err := cmd.SetParam("type", typ); err != nil {
		return false, nil, err
	}
	if src != "" {
		if err := cmd.SetParam("source", src); err != nil {
			return false, nil, err
		}
	}
	resp, err := db.Query(cmd)
	if err != nil {
		return false, nil, err
	}
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

// ColumnRemove executes column_remove.
func (db *DB) ColumnRemove(tbl, name string) (bool, Response, error) {
	cmd, err := NewCommand("column_remove", map[string]interface{}{
		"table": tbl,
		"name":  name,
	})
	if err != nil {
		return false, nil, err
	}
	resp, err := db.Query(cmd)
	if err != nil {
		return false, nil, err
	}
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

// DeleteByID executes delete.
func (db *DB) DeleteByID(tbl string, id int) (bool, Response, error) {
	cmd, err := NewCommand("delete", map[string]interface{}{
		"table": tbl,
		"id":    id,
	})
	if err != nil {
		return false, nil, err
	}
	resp, err := db.Query(cmd)
	if err != nil {
		return false, nil, err
	}
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

// DeleteByKey executes delete.
func (db *DB) DeleteByKey(tbl string, key interface{}) (bool, Response, error) {
	cmd, err := NewCommand("delete", map[string]interface{}{
		"table": tbl,
		"key":   key,
	})
	if err != nil {
		return false, nil, err
	}
	resp, err := db.Query(cmd)
	if err != nil {
		return false, nil, err
	}
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

// DeleteByFilter executes delete.
func (db *DB) DeleteByFilter(tbl, filter string) (bool, Response, error) {
	cmd, err := NewCommand("delete", map[string]interface{}{
		"table":  tbl,
		"filter": filter,
	})
	if err != nil {
		return false, nil, err
	}
	resp, err := db.Query(cmd)
	if err != nil {
		return false, nil, err
	}
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
	cmd, err := NewCommand("load", params)
	if err != nil {
		return 0, nil, err
	}
	cmd.SetBody(values)
	resp, err := db.Query(cmd)
	if err != nil {
		return 0, nil, err
	}
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

// encodeRow encodes a row.
func (db *DB) encodeRow(body []byte, row reflect.Value, fis []*StructFieldInfo) []byte {
	// TODO
	return body
}

// encodeRows encodes rows.
func (db *DB) encodeRows(rows reflect.Value, fis []*StructFieldInfo) ([]byte, error) {
	body := []byte("[")
	for rows.Kind() == reflect.Ptr {
		rows = rows.Elem()
	}
	switch rows.Kind() {
	case reflect.Array, reflect.Slice:
		for i := 0; i < rows.Len(); i++ {
			row := rows.Index(i)
			for row.Kind() == reflect.Ptr {
				row = row.Elem()
			}
			body = db.encodeRow(body, row, fis)
		}
	case reflect.Struct:
	}
	body = append(body, ']')
	return body, nil
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
					"error": "",
				})
			}
			fis = append(fis, fi)
		}
	}
	body, err := db.encodeRows(reflect.ValueOf(rows), fis)
	if err != nil {
		return 0, nil, err
	}
	return db.Load(tbl, bytes.NewReader(body), options)
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

// SelectRows executes select.
func (db *DB) SelectRows(tbl string, rows interface{}, options *DBSelectOptions) (int, Response, error) {
	// TODO
	return 0, nil, nil
}

// DBStatusResult is a response of status.
type DBStatusResult struct {
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
func (db *DB) Status() (*DBStatusResult, Response, error) {
	resp, err := db.Exec("status", nil)
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
	var result DBStatusResult
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

// TableRemove executes table_remove.
func (db *DB) TableRemove(name string, dependent bool) (bool, Response, error) {
	cmd, err := NewCommand("table_remove", map[string]interface{}{
		"name":      name,
		"dependent": dependent,
	})
	if err != nil {
		return false, nil, err
	}
	resp, err := db.Query(cmd)
	if err != nil {
		return false, nil, err
	}
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

// Truncate executes truncate.
func (db *DB) Truncate(target string) (bool, Response, error) {
	resp, err := db.Invoke("truncate", map[string]interface{}{
		"target_name": target,
	}, nil)
	if err != nil {
		return false, nil, err
	}
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
