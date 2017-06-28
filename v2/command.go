package grnci

import (
	"io"
	"reflect"
	"sort"
	"strings"
	"time"
)

// formatParamValue is a function to format a parameter value.
type formatParamValue func(value interface{}) (string, error)

// formatParamValueDefault is the default formatParamValue.
var formatParamValueDefault = func(value interface{}) (string, error) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Bool:
		return formatBool(v.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return formatInt(v.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return formatUint(v.Uint()), nil
	case reflect.Float32:
		return formatFloat(v.Float(), 32), nil
	case reflect.Float64:
		return formatFloat(v.Float(), 64), nil
	case reflect.String:
		return formatString(v.String()), nil
	case reflect.Struct:
		switch v := value.(type) {
		case time.Time:
			return formatTime(v), nil
		case Geo:
			return formatGeo(v), nil
		}
	}
	return "", NewError(InvalidCommand, map[string]interface{}{
		"value": value,
		"type":  reflect.TypeOf(value).Name(),
		"error": "The type is not supported.",
	})
}

// formatParamValueYesNo formats an 3/no value.
func formatParamValueYesNo(value interface{}) (string, error) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			return "yes", nil
		}
		return "no", nil
	case reflect.String:
		switch v := v.String(); v {
		case "yes", "no":
			return v, nil
		default:
			return "", NewError(InvalidCommand, map[string]interface{}{
				"value": v,
				"error": "The value must be yes or no.",
			})
		}
	default:
		return "", NewError(InvalidCommand, map[string]interface{}{
			"value": value,
			"type":  reflect.TypeOf(value).Name(),
			"error": "The type is not supported.",
		})
	}
}

// formatParamValueCSV formats comma-separated values.
func formatParamValueCSV(value interface{}) (string, error) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return formatString(v.String()), nil
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			break
		}
		var buf []byte
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, ',')
			}
			buf = append(buf, formatString(v.Index(i).String())...)
		}
		return string(buf), nil
	}
	return "", NewError(InvalidCommand, map[string]interface{}{
		"value": value,
		"type":  reflect.TypeOf(value).Name(),
		"error": "The type is not supported.",
	})
}

// formatParamValueFlags formats pipe-separated values.
func formatParamValueFlags(value interface{}) (string, error) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return formatString(v.String()), nil
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			break
		}
		var buf []byte
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, '|')
			}
			buf = append(buf, formatString(v.Index(i).String())...)
		}
		return string(buf), nil
	}
	return "", NewError(InvalidCommand, map[string]interface{}{
		"value": value,
		"type":  reflect.TypeOf(value).Name(),
		"error": "The type is not supported.",
	})
}

// formatParamValueMatchColumns formats pipe-separated values.
func formatParamValueMatchColumns(value interface{}) (string, error) {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return formatString(v.String()), nil
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			break
		}
		var buf []byte
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, "||"...)
			}
			buf = append(buf, formatString(v.Index(i).String())...)
		}
		return string(buf), nil
	}
	return "", NewError(InvalidCommand, map[string]interface{}{
		"value": value,
		"type":  reflect.TypeOf(value).Name(),
		"error": "The type is not supported.",
	})
}

// formatParamValueBorder formats an include/exclude value.
func formatParamValueBorder(value interface{}) (string, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return "include", nil
		}
		return "exclude", nil
	case string:
		switch v {
		case "include", "exclude":
			return v, nil
		default:
			return "", NewError(InvalidCommand, map[string]interface{}{
				"value": v,
				"error": "The value must be include or exclude.",
			})
		}
	default:
		return "", NewError(InvalidCommand, map[string]interface{}{
			"value": value,
			"type":  reflect.TypeOf(value).Name(),
			"error": "The type is not supported.",
		})
	}
}

// formatParamValueJSON returns the JSON-encoded value.
func formatParamValueJSON(value interface{}) (string, error) {
	return string(jsonAppendValue(nil, reflect.ValueOf(value))), nil
}

type paramFormat struct {
	key      string           // Parameter key
	format   formatParamValue // Custom function to format a parameter value.
	required bool             // Whether or not the parameter is required
}

// newParamFormat returns a new paramFormat.
func newParamFormat(key string, format formatParamValue, required bool) *paramFormat {
	return &paramFormat{
		key:      key,
		format:   format,
		required: required,
	}
}

// Format formats a parameter value.
func (pf *paramFormat) Format(value interface{}) (string, error) {
	if pf.format != nil {
		return pf.format(value)
	}
	return formatParamValueDefault(value)
}

// formatParam is a function to format a parameter.
type formatParam func(key string, value interface{}) (string, error)

// formatParamDefault is the default formatParam.
func formatParamDefault(key string, value interface{}) (string, error) {
	if key == "" {
		return "", NewError(InvalidCommand, map[string]interface{}{
			"key":   key,
			"error": "The key must not be empty.",
		})
	}
	for _, c := range key {
		switch {
		case c >= 'a' && c <= 'z':
		case c == '_':
		default:
			return "", NewError(InvalidCommand, map[string]interface{}{
				"key":   key,
				"error": "The key must consist of [a-z_].",
			})
		}
	}
	fv, err := formatParamValueDefault(value)
	if err != nil {
		return "", EnhanceError(err, map[string]interface{}{
			"key": key,
		})
	}
	return fv, nil
}

// formatParamSelect formats a parameter of select.
func formatParamSelect(key string, value interface{}) (string, error) {
	if key == "" {
		return "", NewError(InvalidCommand, map[string]interface{}{
			"key":   key,
			"error": "The key must not be empty.",
		})
	}
	for _, c := range key {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		default:
			switch c {
			case '#', '@', '-', '_', '.', '[', ']':
			default:
				return "", NewError(InvalidCommand, map[string]interface{}{
					"key":   key,
					"error": "The key must consist of [0-9A-Za-z#@-_.[]].",
				})
			}
		}
	}
	fv, err := formatParamValueDefault(value)
	if err != nil {
		return "", EnhanceError(err, map[string]interface{}{
			"key": key,
		})
	}
	return fv, nil
}

type commandFormat struct {
	format         formatParam             // Custom function to format a parameter
	params         []*paramFormat          // Fixed parameters
	paramsByKey    map[string]*paramFormat // Index for params
	requiredParams []*paramFormat          // Required parameters
}

// newCommandFormat returns a new commandFormat.
func newCommandFormat(format formatParam, params ...*paramFormat) *commandFormat {
	paramsByKey := make(map[string]*paramFormat)
	var requiredParams []*paramFormat
	for _, param := range params {
		paramsByKey[param.key] = param
		if param.required {
			requiredParams = append(requiredParams, param)
		}
	}
	return &commandFormat{
		format:         format,
		params:         params,
		paramsByKey:    paramsByKey,
		requiredParams: requiredParams,
	}
}

// Format formats a parameter.
func (cf *commandFormat) Format(key string, value interface{}) (string, error) {
	if pf, ok := cf.paramsByKey[key]; ok {
		return pf.Format(value)
	}
	if cf.format != nil {
		return cf.format(key, value)
	}
	return formatParamDefault(key, value)
}

// commandFormats defines the available commands.
// The contents are set in initCommandFormats.
var commandFormats = map[string]*commandFormat{
	"cache_limit": newCommandFormat(
		nil,
		newParamFormat("max", nil, false),
	),
	"check": newCommandFormat(
		nil,
		newParamFormat("obj", nil, true),
	),
	"clearlock": newCommandFormat(
		nil,
		newParamFormat("objname", nil, true),
	),
	"column_copy": newCommandFormat(
		nil,
		newParamFormat("from_table", nil, true),
		newParamFormat("from_name", nil, true),
		newParamFormat("to_table", nil, true),
		newParamFormat("to_name", nil, true),
	),
	"column_create": newCommandFormat(
		nil,
		newParamFormat("table", nil, true),
		newParamFormat("name", nil, true),
		newParamFormat("flags", formatParamValueFlags, true),
		newParamFormat("type", nil, true),
		newParamFormat("source", formatParamValueCSV, false),
	),
	"column_list": newCommandFormat(
		nil,
		newParamFormat("table", nil, true),
	),
	"column_remove": newCommandFormat(nil,
		newParamFormat("table", nil, true),
		newParamFormat("name", nil, true),
	),
	"column_rename": newCommandFormat(nil,
		newParamFormat("table", nil, true),
		newParamFormat("name", nil, true),
		newParamFormat("new_name", nil, true),
	),
	"config_delete": newCommandFormat(
		nil,
		newParamFormat("key", nil, true),
	),
	"config_get": newCommandFormat(
		nil,
		newParamFormat("key", nil, true),
	),
	"config_set": newCommandFormat(
		nil,
		newParamFormat("key", nil, true),
		newParamFormat("value", nil, true),
	),
	"database_unmap": newCommandFormat(nil),
	"define_selector": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
		newParamFormat("table", nil, true),
		newParamFormat("match_columns", formatParamValueMatchColumns, false),
		newParamFormat("query", nil, false),
		newParamFormat("filter", nil, false),
		newParamFormat("scorer", nil, false),
		newParamFormat("sortby", formatParamValueCSV, false),
		newParamFormat("output_columns", formatParamValueCSV, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("drilldown", formatParamValueCSV, false),
		newParamFormat("drilldown_sortby", formatParamValueCSV, false),
		newParamFormat("drilldown_output_columns", formatParamValueCSV, false),
		newParamFormat("drilldown_offset", nil, false),
		newParamFormat("drilldown_limit", nil, false),
	),
	"defrag": newCommandFormat(
		nil,
		newParamFormat("objname", nil, true),
		newParamFormat("threshold", nil, true),
	),
	"delete": newCommandFormat(
		nil,
		newParamFormat("table", nil, true),
		newParamFormat("key", formatParamValueJSON, false),
		newParamFormat("id", nil, false),
		newParamFormat("filter", nil, false),
	),
	"dump": newCommandFormat(
		nil,
		newParamFormat("tables", formatParamValueCSV, false),
		newParamFormat("dump_plugins", formatParamValueYesNo, false),
		newParamFormat("dump_schema", formatParamValueYesNo, false),
		newParamFormat("dump_records", formatParamValueYesNo, false),
		newParamFormat("dump_indexes", formatParamValueYesNo, false),
	),
	"io_flush": newCommandFormat(
		nil,
		newParamFormat("target_name", nil, false),
		newParamFormat("recursive", nil, false),
	),
	"load": newCommandFormat(
		nil,
		newParamFormat("values", nil, false), // values may be passed as a body.
		newParamFormat("table", nil, true),
		newParamFormat("columns", formatParamValueCSV, false),
		newParamFormat("ifexists", nil, false),
		newParamFormat("input_type", nil, false),
	),
	"lock_acquire": newCommandFormat(
		nil,
		newParamFormat("target_name", nil, false),
	),
	"lock_clear": newCommandFormat(
		nil,
		newParamFormat("target_name", nil, false),
	),
	"lock_release": newCommandFormat(
		nil,
		newParamFormat("target_name", nil, false),
	),
	"log_level": newCommandFormat(
		nil,
		newParamFormat("level", nil, true),
	),
	"log_put": newCommandFormat(
		nil,
		newParamFormat("level", nil, true),
		newParamFormat("message", nil, true),
	),
	"log_reopen": newCommandFormat(nil),
	"logical_count": newCommandFormat(
		nil,
		newParamFormat("logical_table", nil, true),
		newParamFormat("shard_key", nil, true),
		newParamFormat("min", nil, false),
		newParamFormat("min_border", formatParamValueBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamValueBorder, false),
		newParamFormat("filter", nil, false),
	),
	"logical_parameters": newCommandFormat(
		nil,
		newParamFormat("range_index", nil, false),
	),
	"logical_range_filter": newCommandFormat(
		nil,
		newParamFormat("logical_table", nil, true),
		newParamFormat("shard_key", nil, true),
		newParamFormat("min", nil, false),
		newParamFormat("min_border", formatParamValueBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamValueBorder, false),
		newParamFormat("order", nil, false),
		newParamFormat("filter", nil, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("output_columns", formatParamValueCSV, false),
		newParamFormat("use_range_index", nil, false),
	),
	"logical_select": newCommandFormat(
		nil,
		newParamFormat("logical_table", nil, true),
		newParamFormat("shard_key", nil, true),
		newParamFormat("min", nil, false),
		newParamFormat("min_border", formatParamValueBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamValueBorder, false),
		newParamFormat("filter", nil, false),
		newParamFormat("sortby", formatParamValueCSV, false),
		newParamFormat("output_columns", formatParamValueCSV, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("drilldown", nil, false),
		newParamFormat("drilldown_sortby", formatParamValueCSV, false),
		newParamFormat("drilldown_output_columns", formatParamValueCSV, false),
		newParamFormat("drilldown_offset", nil, false),
		newParamFormat("drilldown_limit", nil, false),
		newParamFormat("drilldown_calc_types", formatParamValueCSV, false),
		newParamFormat("drilldown_calc_target", nil, false),
		newParamFormat("sort_keys", formatParamValueCSV, false),
		newParamFormat("drilldown_sort_keys", formatParamValueCSV, false),
		newParamFormat("match_columns", formatParamValueMatchColumns, false),
		newParamFormat("query", nil, false),
		newParamFormat("drilldown_filter", nil, false),
	),
	"logical_shard_list": newCommandFormat(
		nil,
		newParamFormat("logical_table", nil, true),
	),
	"logical_table_remove": newCommandFormat(
		nil,
		newParamFormat("logical_table", nil, true),
		newParamFormat("shard_key", nil, true),
		newParamFormat("min", nil, false),
		newParamFormat("min_border", formatParamValueBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamValueBorder, false),
		newParamFormat("dependent", formatParamValueYesNo, false),
		newParamFormat("force", formatParamValueYesNo, false),
	),
	"normalize": newCommandFormat(
		nil,
		newParamFormat("normalizer", nil, true),
		newParamFormat("string", nil, true),
		newParamFormat("flags", formatParamValueFlags, false),
	),
	"normalizer_list": newCommandFormat(nil),
	"object_exist": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
	),
	"object_inspect": newCommandFormat(
		nil,
		newParamFormat("name", nil, false),
	),
	"object_list": newCommandFormat(nil),
	"object_remove": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
		newParamFormat("force", formatParamValueYesNo, false),
	),
	"plugin_register": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
	),
	"plugin_unregister": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
	),
	"query_expand": newCommandFormat(nil), // TODO
	"quit":         newCommandFormat(nil),
	"range_filter": newCommandFormat(nil), // TODO
	"register": newCommandFormat(
		nil,
		newParamFormat("path", nil, true),
	),
	"reindex": newCommandFormat(
		nil,
		newParamFormat("target_name", nil, false),
	),
	"request_cancel": newCommandFormat(
		nil,
		newParamFormat("id", nil, true),
	),
	"ruby_eval": newCommandFormat(
		nil,
		newParamFormat("script", nil, true),
	),
	"ruby_load": newCommandFormat(
		nil,
		newParamFormat("path", nil, true),
	),
	"schema": newCommandFormat(nil),
	"select": newCommandFormat(
		formatParamSelect,
		newParamFormat("table", nil, true),
		newParamFormat("match_columns", formatParamValueMatchColumns, false),
		newParamFormat("query", nil, false),
		newParamFormat("filter", nil, false),
		newParamFormat("scorer", nil, false),
		newParamFormat("sortby", formatParamValueCSV, false),
		newParamFormat("output_columns", formatParamValueCSV, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("drilldown", nil, false),
		newParamFormat("drilldown_sortby", formatParamValueCSV, false),
		newParamFormat("drilldown_output_columns", formatParamValueCSV, false),
		newParamFormat("drilldown_offset", nil, false),
		newParamFormat("drilldown_limit", nil, false),
		newParamFormat("cache", formatParamValueYesNo, false),
		newParamFormat("match_escalation_threshold", nil, false),
		newParamFormat("query_expansion", nil, false),
		newParamFormat("query_flags", formatParamValueFlags, false),
		newParamFormat("query_expander", nil, false),
		newParamFormat("adjuster", nil, false),
		newParamFormat("drilldown_calc_types", formatParamValueCSV, false),
		newParamFormat("drilldown_calc_target", nil, false),
		newParamFormat("drilldown_filter", nil, false),
		newParamFormat("sort_keys", formatParamValueCSV, false),
		newParamFormat("drilldown_sort_keys", formatParamValueCSV, false),
	),
	"shutdown": newCommandFormat(
		nil,
		newParamFormat("mode", nil, false),
	),
	"status": newCommandFormat(nil),
	"suggest": newCommandFormat(
		nil,
		newParamFormat("types", formatParamValueFlags, true),
		newParamFormat("table", nil, true),
		newParamFormat("column", nil, true),
		newParamFormat("query", nil, true),
		newParamFormat("sortby", formatParamValueCSV, false),
		newParamFormat("output_columns", formatParamValueCSV, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("frequency_threshold", nil, false),
		newParamFormat("conditional_probability_threshold", nil, false),
		newParamFormat("prefix_search", nil, false),
	),
	"table_copy": newCommandFormat(
		nil,
		newParamFormat("from_name", nil, true),
		newParamFormat("to_name", nil, true),
	),
	"table_create": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
		newParamFormat("flags", formatParamValueFlags, false),
		newParamFormat("key_type", nil, false),
		newParamFormat("value_type", nil, false),
		newParamFormat("default_tokenizer", nil, false),
		newParamFormat("normalizer", nil, false),
		newParamFormat("token_filters", formatParamValueCSV, false),
	),
	"table_list": newCommandFormat(nil),
	"table_remove": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
		newParamFormat("dependent", formatParamValueYesNo, false),
	),
	"table_rename": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
		newParamFormat("new_name", nil, true),
	),
	"table_tokenize": newCommandFormat(
		nil,
		newParamFormat("table", nil, true),
		newParamFormat("string", nil, true),
		newParamFormat("flags", formatParamValueFlags, false),
		newParamFormat("mode", nil, false),
		newParamFormat("index_column", nil, false),
	),
	"thread_limit": newCommandFormat(
		nil,
		newParamFormat("max", nil, false),
	),
	"tokenize": newCommandFormat(
		nil,
		newParamFormat("tokenizer", nil, true),
		newParamFormat("string", nil, true),
		newParamFormat("normalizer", nil, false),
		newParamFormat("flags", formatParamValueFlags, false),
		newParamFormat("mode", nil, false),
		newParamFormat("token_filters", formatParamValueCSV, false),
	),
	"tokenizer_list": newCommandFormat(nil),
	"truncate": newCommandFormat(
		nil,
		newParamFormat("target_name", nil, true),
	),
}

// Command is a command.
type Command struct {
	name   string            // Command name
	format *commandFormat    // Command format
	params map[string]string // Parameters
	index  int               // Number of unnamed parameters
	body   io.Reader         // Command body
}

// newCommand returns a new Command.
func newCommand(name string) (*Command, error) {
	format, ok := commandFormats[name]
	if !ok {
		return nil, NewError(InvalidCommand, map[string]interface{}{
			"name":  name,
			"error": "The name is not defined.",
		})
	}
	return &Command{
		name:   name,
		format: format,
		params: make(map[string]string),
	}, nil
}

// NewCommand formats params and returns a new Command.
func NewCommand(name string, params map[string]interface{}) (*Command, error) {
	c, err := newCommand(name)
	if err != nil {
		return nil, err
	}
	for k, v := range params {
		if err := c.SetParam(k, v); err != nil {
			return nil, EnhanceError(err, map[string]interface{}{
				"name": name,
			})
		}
	}
	return c, nil

}

// unescapeCommandByte returns an unescaped space character.
func unescapeCommandByte(b byte) byte {
	switch b {
	case 'b':
		return '\b'
	case 't':
		return '\t'
	case 'r':
		return '\r'
	case 'n':
		return '\n'
	default:
		return b
	}
}

// tokenizeCommand tokenizes a command.
func tokenizeCommand(cmd string) ([]string, error) {
	var tokens []string
	var token []byte
	s := cmd
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		if s == "" {
			break
		}
		switch s[0] {
		case '"', '\'':
			i := 1
			for ; i < len(s); i++ {
				if s[i] == s[0] {
					i++
					break
				}
				if s[i] != '\\' {
					token = append(token, s[i])
					continue
				}
				i++
				if i == len(s) {
					return nil, NewError(InvalidCommand, map[string]interface{}{
						"command": cmd,
						"error":   "The command ends with an unclosed token.",
					})
				}
				token = append(token, unescapeCommandByte(s[i]))
			}
			s = s[i:]
		default:
			i := 0
		Loop:
			for ; i < len(s); i++ {
				switch s[i] {
				case ' ', '\t', '\r', '\n', '"', '\'':
					break Loop
				case '\\':
					i++
					if i == len(s) {
						return nil, NewError(InvalidCommand, map[string]interface{}{
							"command": cmd,
							"error":   "The command ends with an escape character.",
						})
					}
					token = append(token, unescapeCommandByte(s[i]))
				default:
					token = append(token, s[i])
				}
			}
			s = s[i:]
		}
		tokens = append(tokens, string(token))
		token = token[:0]
	}
	return tokens, nil
}

// ParseCommand parses cmd and returns a new Command.
func ParseCommand(cmd string) (*Command, error) {
	tokens, err := tokenizeCommand(cmd)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return nil, NewError(InvalidCommand, map[string]interface{}{
			"command": cmd,
			"error":   "The command has no tokens.",
		})
	}
	c, err := newCommand(tokens[0])
	if err != nil {
		return nil, EnhanceError(err, map[string]interface{}{
			"command": cmd,
		})
	}
	for i := 1; i < len(tokens); i++ {
		var k, v string
		if strings.HasPrefix(tokens[i], "--") {
			k = tokens[i][2:]
			i++
			if i >= len(tokens) {
				return nil, NewError(InvalidCommand, map[string]interface{}{
					"command": cmd,
					"key":     k,
					"error":   "The key requires a value.",
				})
			}
		}
		v = tokens[i]
		if err := c.SetParam(k, v); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Name returns the command name.
func (c *Command) Name() string {
	return c.name
}

// Params returns the command parameters.
func (c *Command) Params() map[string]string {
	return c.params
}

// Body returns the command body.
func (c *Command) Body() io.Reader {
	return c.body
}

// NeedsBody returns whether or not the command requires a body.
func (c *Command) NeedsBody() bool {
	if c.name == "load" {
		if _, ok := c.params["values"]; !ok {
			return true
		}
	}
	return false
}

// Check checks whether or not the command has required parameters.
func (c *Command) Check() error {
	for _, pf := range c.format.requiredParams {
		if _, ok := c.params[pf.key]; !ok {
			return NewError(InvalidCommand, map[string]interface{}{
				"name":  c.name,
				"key":   pf.key,
				"error": "The command requires the key.",
			})
		}
	}
	if c.NeedsBody() {
		if c.body == nil {
			return NewError(InvalidCommand, map[string]interface{}{
				"name":  c.name,
				"error": "The command requires a body",
			})
		}
	}
	return nil
}

// SetParam adds or removes a parameter.
// If value == nil, it adds a parameter.
// Otherwise, it removes a parameter.
func (c *Command) SetParam(key string, value interface{}) error {
	if value == nil {
		if _, ok := c.params[key]; !ok {
			return NewError(InvalidCommand, map[string]interface{}{
				"name":  c.name,
				"key":   key,
				"error": "The key does not exist.",
			})
		}
		delete(c.params, key)
		return nil
	}
	if key == "" {
		if c.index >= len(c.format.params) {
			return NewError(InvalidCommand, map[string]interface{}{
				"name":  c.name,
				"index": c.index,
				"error": "The index is too large.",
			})
		}
		pf := c.format.params[c.index]
		fv, err := pf.Format(value)
		if err != nil {
			return EnhanceError(err, map[string]interface{}{
				"name": c.name,
				"key":  key,
			})
		}
		c.params[pf.key] = fv
		c.index++
		return nil
	}
	fv, err := c.format.Format(key, value)
	if err != nil {
		return EnhanceError(err, map[string]interface{}{
			"name": c.name,
		})
	}
	c.params[key] = fv
	return nil
}

// SetBody sets a body.
func (c *Command) SetBody(body io.Reader) {
	c.body = body
}

// String assembles the command name and parameters.
func (c *Command) String() string {
	cmd := []byte(c.name)
	keys := make([]string, 0, len(c.params))
	for k := range c.params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := c.params[k]
		cmd = append(cmd, " --"...)
		cmd = append(cmd, k...)
		cmd = append(cmd, " '"...)
		for i := 0; i < len(v); i++ {
			switch v[i] {
			case '\'', '\\', '\b', '\t', '\r', '\n':
				cmd = append(cmd, '\\')
			}
			cmd = append(cmd, v[i])
		}
		cmd = append(cmd, '\'')
	}
	return string(cmd)
}
