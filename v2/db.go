package grnci

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
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
func (db *DB) ColumnCreate(tbl, name, typ, flags string) (bool, Response, error) {
	req, err := NewRequest("column_create", map[string]interface{}{
		"table": tbl,
		"name":  name,
	}, nil)
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
	if flags == "" {
		flags = typFlag
	} else {
		flags += "|" + typFlag
	}
	if withSection {
		flags += "|WITH_SECTION"
	}
	if err := req.AddParam("flags", flags); err != nil {
		return false, nil, err
	}
	if err := req.AddParam("type", typ); err != nil {
		return false, nil, err
	}
	if src != "" {
		if err := req.AddParam("source", src); err != nil {
			return false, nil, err
		}
	}
	resp, err := db.Query(req)
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
		return false, resp, NewError(StatusInvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// ColumnRemove executes column_remove.
func (db *DB) ColumnRemove(tbl, name string) (bool, Response, error) {
	req, err := NewRequest("column_remove", map[string]interface{}{
		"table": tbl,
		"name":  name,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	resp, err := db.Query(req)
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
		return false, resp, NewError(StatusInvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// DumpOptions stores options for DB.Dump.
type DumpOptions struct {
	Tables      string // --table
	DumpPlugins string // --dump_plugins
	DumpSchema  string // --dump_schema
	DumpRecords string // --dump_records
	DumpIndexes string // --dump_indexes
}

// NewDumpOptions returns the default DumpOptions.
func NewDumpOptions() *DumpOptions {
	return &DumpOptions{
		DumpPlugins: "yes",
		DumpSchema:  "yes",
		DumpRecords: "yes",
		DumpIndexes: "yes",
	}
}

// Dump executes dump.
// On success, it is the caller's responsibility to close the response.
func (db *DB) Dump(options *DumpOptions) (Response, error) {
	if options == nil {
		options = NewDumpOptions()
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
	req, err := NewRequest("dump", params, nil)
	if err != nil {
		return nil, err
	}
	return db.Query(req)
}

// LoadOptions stores options for DB.Load.
// http://groonga.org/docs/reference/commands/load.html
type LoadOptions struct {
	Columns  string // --columns
	IfExists string // --ifexists
}

// Load executes load.
func (db *DB) Load(tbl string, values io.Reader, options *LoadOptions) (int, Response, error) {
	params := map[string]interface{}{
		"table": tbl,
	}
	if options != nil {
		if options.Columns != "" {
			params["columns"] = options.Columns
		}
		if options.IfExists != "" {
			params["ifexists"] = options.IfExists
		}
	}
	req, err := NewRequest("load", params, values)
	if err != nil {
		return 0, nil, err
	}
	resp, err := db.Query(req)
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
		return 0, resp, NewError(StatusInvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// For --columns[NAME].stage, type, value.
// type SelectOptionsColumn struct {
// 	Stage string // --columns[NAME].stage
// 	Type  string // --columns[NAME].type
// 	Value string // --columns[NAME].value
// }

// For --drilldowns[LABEL].columns[NAME].
// type SelectOptionsDorilldownColumn struct {
// 	Stage           string // --drilldowns[LABEL].columns[NAME].stage
// 	Flags           string // --drilldowns[LABEL].columns[NAME].flags
// 	Type            string // --drilldowns[LABEL].columns[NAME].type
// 	Value           string // --drilldowns[LABEL].columns[NAME].value
// 	WindowSortKeys  string // --drilldowns[LABEL].columns[NAME].window.sort_keys
// 	WindowGroupKeys string // --drilldowns[LABEL].columns[NAME].window.group_keys
// }

// For --drilldowns[LABEL].keys, sort_keys, output_columns, offset, limit, calc_types, calc_target, filter, columns[].
// type SelectOptionsDrilldown struct {
// 	Keys          string // --drilldowns[LABEL].keys
// 	SortKeys      string // --drilldowns[LABEL].sort_keys
// 	OutputColumns string // --drilldowns[LABEL].output_columns
// 	Offset        int    // --drilldowns[LABEL].offset
// 	Limit         int    // --drilldowns[LABEL].limit
// 	CalcTypes     string // --drilldowns[LABEL].calc_types
// 	CalcTarget    string // --drilldowns[LABEL].calc_target
// 	Filter        string // --drilldowns[LABEL].filter
// 	Columns       map[string]*SelectOptionsDorilldownColumn
// }

// NewSelectOptionsDrilldown returns the default SelectOptionsDrilldown.
// func NewSelectOptionsDrilldown() *SelectOptionsDrilldown {
// 	return &SelectOptionsDrilldown{
// 		Limit: 10,
// 	}
// }

// SelectOptions stores options for DB.Select.
// http://groonga.org/docs/reference/commands/select.html
type SelectOptions struct {
	MatchColumns             string // --match_columns
	Query                    string // --query
	Filter                   string // --filter
	Scorer                   string // --scorer
	SortKeys                 string // --sort_keys
	OutputColumns            string // --output_columns
	Offset                   int    // --offset
	Limit                    int    // --limit
	Drilldown                string // --drilldown
	DrilldownSortKeys        string // --drilldown_sort_keys
	DrilldownOutputColumns   string // --drilldown_output_columns
	DrillDownOffset          int    // drilldown_offset
	DrillDownLimit           int    // drilldown_limit
	Cache                    bool   // --cache
	MatchEscalationThreshold int    // --match_escalation_threshold
	QueryExpansion           string // --query_expansion
	QueryFlags               string // --query_flags
	QueryExpander            string // --query_expander
	Adjuster                 string // --adjuster
	DrilldownCalcTypes       string // --drilldown_calc_types
	DrilldownCalcTarget      string // --drilldown_calc_target
	DrilldownFilter          string // --drilldown_filter
	// Columns    map[string]*SelectOptionsColumn    // --columns[NAME]
	// Drilldowns map[string]*SelectOptionsDrilldown // --drilldowns[LABEL]
}

// NewSelectOptions returns the default SelectOptions.
func NewSelectOptions() *SelectOptions {
	return &SelectOptions{
		Limit:          10,
		DrillDownLimit: 10,
	}
}

// Select executes select.
// On success, it is the caller's responsibility to close the response.
func (db *DB) Select(tbl string, options *SelectOptions) (Response, error) {
	if options == nil {
		options = NewSelectOptions()
	}
	params := map[string]interface{}{
		"table": tbl,
	}
	// TODO: copy entries from options to params.
	req, err := NewRequest("dump", params, nil)
	if err != nil {
		return nil, err
	}
	return db.Query(req)
}

// StatusResult is a response of status.
type StatusResult struct {
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
func (db *DB) Status() (*StatusResult, Response, error) {
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
		return nil, resp, NewError(StatusInvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	var result StatusResult
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

// TableCreateOptions stores options for DB.TableCreate.
// http://groonga.org/docs/reference/commands/table_create.html
type TableCreateOptions struct {
	Flags            string // --flags
	KeyType          string // --key_type
	ValueType        string // --value_type
	DefaultTokenizer string // --default_tokenizer
	Normalizer       string // --normalizer
	TokenFilters     string // --token_filters
}

// TableCreate executes table_create.
func (db *DB) TableCreate(name string, options *TableCreateOptions) (bool, Response, error) {
	if options == nil {
		options = &TableCreateOptions{}
	}
	params := map[string]interface{}{
		"name": name,
	}
	flags, keyFlag := "", ""
	if options.Flags != "" {
		for _, flag := range strings.Split(options.Flags, "|") {
			switch flag {
			case "TABLE_NO_KEY":
				if keyFlag != "" {
					return false, nil, fmt.Errorf("TABLE_NO_KEY must not be set with %s", keyFlag)
				}
				if options.KeyType != "" {
					return false, nil, fmt.Errorf("TABLE_NO_KEY disallows KeyType")
				}
				keyFlag = flag
			case "TABLE_HASH_KEY", "TABLE_PAT_KEY", "TABLE_DAT_KEY":
				if keyFlag != "" {
					return false, nil, fmt.Errorf("%s must not be set with %s", flag, keyFlag)
				}
				if options.KeyType == "" {
					return false, nil, fmt.Errorf("%s requires KeyType", flag)
				}
				keyFlag = flag
			}
		}
		flags = options.Flags
	}
	if keyFlag == "" {
		if options.KeyType == "" {
			keyFlag = "TABLE_NO_KEY"
		} else {
			keyFlag = "TABLE_HASH_KEY"
		}
		if flags == "" {
			flags = keyFlag
		} else {
			flags += "|" + keyFlag
		}
	}
	if flags != "" {
		params["flags"] = flags
	}
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
	if options.TokenFilters != "" {
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
		return false, resp, NewError(StatusInvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}

// TableRemove executes table_remove.
func (db *DB) TableRemove(name string, dependent bool) (bool, Response, error) {
	req, err := NewRequest("table_remove", map[string]interface{}{
		"name":      name,
		"dependent": dependent,
	}, nil)
	if err != nil {
		return false, nil, err
	}
	resp, err := db.Query(req)
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
		return false, resp, NewError(StatusInvalidResponse, map[string]interface{}{
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
		return false, resp, NewError(StatusInvalidResponse, map[string]interface{}{
			"method": "json.Unmarshal",
			"error":  err.Error(),
		})
	}
	return result, resp, nil
}
