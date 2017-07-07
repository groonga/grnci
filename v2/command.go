package grnci

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// commandSpaces is a set of characters handled as spaces in commands.
const commandSpaces = "\t\n\r "

// formatParamValue is the default function to format a parameter value.
func formatParamValue(key string, value interface{}) (string, error) {
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			return "yes", nil
		}
		return "no", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10), nil
	case reflect.Float32:
		return strconv.FormatFloat(v.Float(), 'g', -1, 32), nil
	case reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'g', -1, 64), nil
	case reflect.String:
		return v.String(), nil
	default:
		return "", NewError(InvalidCommand, map[string]interface{}{
			"key":   key,
			"value": value,
			"type":  reflect.TypeOf(value).Name(),
			"error": "The type is not supported.",
		})
	}
}

// formatParamBoolean formats a boolean value.
func formatParamBoolean(key string, value interface{}, t, f string) (string, error) {
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.Bool:
		if v.Bool() {
			return t, nil
		}
		return f, nil
	case reflect.String:
		switch v := v.String(); v {
		case t, f:
			return v, nil
		default:
			return "", NewError(InvalidCommand, map[string]interface{}{
				"key":   key,
				"value": v,
				"error": fmt.Sprintf("The value must be %s or %s.", t, f),
			})
		}
	default:
		return "", NewError(InvalidCommand, map[string]interface{}{
			"key":   key,
			"value": value,
			"type":  reflect.TypeOf(value).Name(),
			"error": "The type is not supported.",
		})
	}
}

// formatParamYesNo formats an yes/no value.
func formatParamYesNo(key string, value interface{}) (string, error) {
	return formatParamBoolean(key, value, "yes", "no")
}

// formatParamBorder formats an include/exclude value.
func formatParamBorder(key string, value interface{}) (string, error) {
	return formatParamBoolean(key, value, "include", "exclude")
}

// formatParamDelim formats values separated by delim.
func formatParamDelim(key string, value interface{}, delim string) (string, error) {
	switch v := reflect.ValueOf(value); v.Kind() {
	case reflect.String:
		return v.String(), nil
	case reflect.Array, reflect.Slice:
		if v.Type().Elem().Kind() != reflect.String {
			break
		}
		var buf []byte
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, delim...)
			}
			buf = append(buf, v.Index(i).String()...)
		}
		return string(buf), nil
	}
	return "", NewError(InvalidCommand, map[string]interface{}{
		"key":   key,
		"value": value,
		"type":  reflect.TypeOf(value).Name(),
		"error": "The type is not supported.",
	})
}

// formatParamCSV formats comma-separated values.
func formatParamCSV(key string, value interface{}) (string, error) {
	return formatParamDelim(key, value, ",")
}

// formatParamFlags formats pipe-separated values.
func formatParamFlags(key string, value interface{}) (string, error) {
	return formatParamDelim(key, value, "|")
}

// formatParamMatchColumns formats "||"-separated values (--match_columns).
func formatParamMatchColumns(key string, value interface{}) (string, error) {
	return formatParamDelim(key, value, "||")
}

// formatParamJSON returns the JSON-encoded value (delete --key).
func formatParamJSON(key string, value interface{}) (string, error) {
	return EncodeJSON(value), nil
}

type paramFormat struct {
	key      string      // Parameter key
	format   formatParam // Custom function to format a parameter.
	required bool        // Whether or not the parameter is required
}

// newParamFormat returns a new paramFormat.
func newParamFormat(key string, format formatParam, required bool) *paramFormat {
	return &paramFormat{
		key:      key,
		format:   format,
		required: required,
	}
}

// Format formats a parameter.
func (pf *paramFormat) Format(value interface{}) (string, error) {
	if pf.format != nil {
		return pf.format(pf.key, value)
	}
	return formatParamDefault(pf.key, value)
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
	return formatParamValue(key, value)
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
	// For parameters with variable keys, such as --columns[NAME] and --drilldowns[LABEL].
	switch {
	case strings.HasSuffix(key, "flags"):
		return formatParamFlags(key, value)
	case strings.HasSuffix(key, "keys"), // keys, sort_keys and group_keys
		strings.HasSuffix(key, "output_columns"),
		strings.HasSuffix(key, "calc_types"):
		return formatParamCSV(key, value)
	default:
		return formatParamValue(key, value)
	}
}

// commandFormat is the format of a command.
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

// getCommandFormat returns the format of the specified command.
func getCommandFormat(name string) *commandFormat {
	return commandFormats[name]
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
		newParamFormat("flags", formatParamFlags, true),
		newParamFormat("type", nil, true),
		newParamFormat("source", formatParamCSV, false),
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
		newParamFormat("match_columns", formatParamMatchColumns, false),
		newParamFormat("query", nil, false),
		newParamFormat("filter", nil, false),
		newParamFormat("scorer", nil, false),
		newParamFormat("sortby", formatParamCSV, false),
		newParamFormat("output_columns", formatParamCSV, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("drilldown", formatParamCSV, false),
		newParamFormat("drilldown_sortby", formatParamCSV, false),
		newParamFormat("drilldown_output_columns", formatParamCSV, false),
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
		newParamFormat("key", formatParamJSON, false),
		newParamFormat("id", nil, false),
		newParamFormat("filter", nil, false),
	),
	"dump": newCommandFormat(
		nil,
		newParamFormat("tables", formatParamCSV, false),
		newParamFormat("dump_plugins", formatParamYesNo, false),
		newParamFormat("dump_schema", formatParamYesNo, false),
		newParamFormat("dump_records", formatParamYesNo, false),
		newParamFormat("dump_indexes", formatParamYesNo, false),
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
		newParamFormat("columns", formatParamCSV, false),
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
		newParamFormat("min_border", formatParamBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamBorder, false),
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
		newParamFormat("min_border", formatParamBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamBorder, false),
		newParamFormat("order", nil, false),
		newParamFormat("filter", nil, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("output_columns", formatParamCSV, false),
		newParamFormat("use_range_index", nil, false),
		// TODO: --cache is not supported yet.
	),
	"logical_select": newCommandFormat(
		formatParamSelect,
		newParamFormat("logical_table", nil, true),
		newParamFormat("shard_key", nil, true),
		newParamFormat("min", nil, false),
		newParamFormat("min_border", formatParamBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamBorder, false),
		newParamFormat("filter", nil, false),
		newParamFormat("sortby", formatParamCSV, false),
		newParamFormat("output_columns", formatParamCSV, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("drilldown", nil, false),
		newParamFormat("drilldown_sortby", formatParamCSV, false),
		newParamFormat("drilldown_output_columns", formatParamCSV, false),
		newParamFormat("drilldown_offset", nil, false),
		newParamFormat("drilldown_limit", nil, false),
		newParamFormat("drilldown_calc_types", formatParamCSV, false),
		newParamFormat("drilldown_calc_target", nil, false),
		newParamFormat("sort_keys", formatParamCSV, false),
		newParamFormat("drilldown_sort_keys", formatParamCSV, false),
		newParamFormat("match_columns", formatParamMatchColumns, false),
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
		newParamFormat("min_border", formatParamBorder, false),
		newParamFormat("max", nil, false),
		newParamFormat("max_border", formatParamBorder, false),
		newParamFormat("dependent", formatParamYesNo, false),
		newParamFormat("force", formatParamYesNo, false),
	),
	"normalize": newCommandFormat(
		nil,
		newParamFormat("normalizer", nil, true),
		newParamFormat("string", nil, true),
		newParamFormat("flags", formatParamFlags, false),
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
		newParamFormat("force", formatParamYesNo, false),
	),
	"plugin_register": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
	),
	"plugin_unregister": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
	),
	"query_expand": newCommandFormat(nil), // TODO: not documented.
	"quit":         newCommandFormat(nil),
	"range_filter": newCommandFormat(nil), // TODO: not documented.
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
		newParamFormat("match_columns", formatParamMatchColumns, false),
		newParamFormat("query", nil, false),
		newParamFormat("filter", nil, false),
		newParamFormat("scorer", nil, false),
		newParamFormat("sortby", formatParamCSV, false),
		newParamFormat("output_columns", formatParamCSV, false),
		newParamFormat("offset", nil, false),
		newParamFormat("limit", nil, false),
		newParamFormat("drilldown", nil, false),
		newParamFormat("drilldown_sortby", formatParamCSV, false),
		newParamFormat("drilldown_output_columns", formatParamCSV, false),
		newParamFormat("drilldown_offset", nil, false),
		newParamFormat("drilldown_limit", nil, false),
		newParamFormat("cache", formatParamYesNo, false),
		newParamFormat("match_escalation_threshold", nil, false),
		newParamFormat("query_expansion", nil, false),
		newParamFormat("query_flags", formatParamFlags, false),
		newParamFormat("query_expander", nil, false),
		newParamFormat("adjuster", nil, false),
		newParamFormat("drilldown_calc_types", formatParamCSV, false),
		newParamFormat("drilldown_calc_target", nil, false),
		newParamFormat("drilldown_filter", nil, false),
		newParamFormat("sort_keys", formatParamCSV, false),
		newParamFormat("drilldown_sort_keys", formatParamCSV, false),
	),
	"shutdown": newCommandFormat(
		nil,
		newParamFormat("mode", nil, false),
	),
	"status": newCommandFormat(nil),
	"suggest": newCommandFormat(
		nil,
		newParamFormat("types", formatParamFlags, true),
		newParamFormat("table", nil, true),
		newParamFormat("column", nil, true),
		newParamFormat("query", nil, true),
		newParamFormat("sortby", formatParamCSV, false),
		newParamFormat("output_columns", formatParamCSV, false),
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
		newParamFormat("flags", formatParamFlags, false),
		newParamFormat("key_type", nil, false),
		newParamFormat("value_type", nil, false),
		newParamFormat("default_tokenizer", nil, false),
		newParamFormat("normalizer", nil, false),
		newParamFormat("token_filters", formatParamCSV, false),
	),
	"table_list": newCommandFormat(nil),
	"table_remove": newCommandFormat(
		nil,
		newParamFormat("name", nil, true),
		newParamFormat("dependent", formatParamYesNo, false),
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
		newParamFormat("flags", formatParamFlags, false),
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
		newParamFormat("flags", formatParamFlags, false),
		newParamFormat("mode", nil, false),
		newParamFormat("token_filters", formatParamCSV, false),
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
	format := getCommandFormat(name)
	if format == nil {
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
	case 't':
		return '\t'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
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
		s = strings.TrimLeft(s, commandSpaces)
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
				case ' ', '\t', '\n', '\r', '"', '\'':
					break Loop
				case '\\':
					i++
					if i == len(s) {
						return nil, NewError(InvalidCommand, map[string]interface{}{
							"command": cmd,
							"error":   "The command ends with an escape character.",
						})
					}
					switch s[i] {
					case '\n':
					case '\r':
						if i+1 < len(s) && s[i+1] == '\n' {
							i++
						}
					default:
						token = append(token, unescapeCommandByte(s[i]))
					}
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
			case '\t':
				cmd = append(cmd, `\t`...)
			case '\n':
				cmd = append(cmd, `\n`...)
			case '\r':
				cmd = append(cmd, `\r`...)
			case '\'':
				cmd = append(cmd, `\'`...)
			case '\\':
				cmd = append(cmd, `\\`...)
			default:
				cmd = append(cmd, v[i])
			}
		}
		cmd = append(cmd, '\'')
	}
	return string(cmd)
}

// commandBodyReader is a reader for command bodies.
type commandBodyReader struct {
	reader *CommandReader // Underlying reader
	stack  []byte         // Stack for special symbols
	line   []byte         // Current line
	left   []byte         // Remaining bytes of the current line
	err    error          // Last error
}

// newCommandBodyReader returns a new commandBodyReader.
func newCommandBodyReader(cr *CommandReader) *commandBodyReader {
	return &commandBodyReader{
		reader: cr,
		stack:  make([]byte, 0, 8),
	}
}

// checkLine checks the current line.
func (br *commandBodyReader) checkLine() error {
	var top byte
	if len(br.stack) != 0 {
		top = br.stack[len(br.stack)-1]
	}
	for i := 0; i < len(br.line); i++ {
		switch top {
		case 0: // The first non-space byte must be '[' or '{'.
			switch br.line[i] {
			case '[', '{':
				top = br.line[i] + 2 // Convert a bracket from left to right.
				br.stack = append(br.stack, top)
			case ' ', '\t', '\r', '\n':
			default:
				return io.EOF
			}
		case '"', '\'':
			switch br.line[i] {
			case '\\':
				if i+1 < len(br.line) { // Skip the next byte if possible.
					i++
				}
			case top: // Close the quoted string.
				br.stack = br.stack[len(br.stack)-1:]
				top = br.stack[len(br.stack)-1]
			}
		default:
			switch br.line[i] {
			case '"', '\'':
				top = br.line[i]
				br.stack = append(br.stack, top)
			case '[', '{':
				top = br.line[i] + 2 // Convert a bracket from left to right.
				br.stack = append(br.stack, top)
			case ']', '}':
				if br.line[i] != top {
					return io.EOF
				}
			}
		}
	}
	return nil
}

// Read reads up to len(p) bytes into p.
func (br *commandBodyReader) Read(p []byte) (n int, err error) {
	if len(br.left) == 0 && br.err != nil {
		return 0, br.err
	}
	cr := br.reader
	for n < len(p) {
		if len(br.left) == 0 {
			if err = br.checkLine(); err != nil {
				cr.err = err
				return
			}
			br.line, err = cr.readLine()
			if err != nil {
				return
			}
			br.left = br.line
		}
		m := copy(p[n:], br.left)
		br.left = br.left[m:]
		n += m
	}
	return
}

// CommandReader is designed to read commands from the underlying io.Reader.
//
// The following is an example of reading commands from a dump file.
//
//   f, err := os.Open("db.dump")
//   if err != nil {
//     // Failed to open the dump file.
//   }
//   defer f.Close()
//   cr := grnci.NewCommandReader(f)
//   for {
//     cmd, err := cr.Read()
//     if err != nil {
//       if err != io.EOF {
//         // Failed to read or parse a command.
//       }
//       break
//     }
//     // Do something using cmd.
//   }
type CommandReader struct {
	reader io.Reader // Underlying reader
	buf    []byte    // Buffer
	left   []byte    // Unprocessed bytes in buf
	err    error     // Last reader error
}

// NewCommandReader returns a new CommandReader.
func NewCommandReader(r io.Reader) *CommandReader {
	return &CommandReader{
		reader: r,
		buf:    make([]byte, 1024),
	}
}

// fill reads data from the underlying reader and fills the buffer.
func (cr *CommandReader) fill() error {
	if cr.err != nil {
		return cr.err
	}
	if len(cr.left) == len(cr.buf) {
		// Extend the buffer because it is full.
		cr.buf = make([]byte, len(cr.buf)*2)
	}
	copy(cr.buf, cr.left)
	n, err := cr.reader.Read(cr.buf[len(cr.left):])
	if err != nil {
		cr.err = err
		if err != io.EOF {
			cr.err = NewError(InvalidCommand, map[string]interface{}{
				"error": err.Error(),
			})
		}
	}
	cr.left = cr.buf[:len(cr.left)+n]
	if n == 0 {
		return cr.err
	}
	return nil
}

// readLine reads the next line.
func (cr *CommandReader) readLine() ([]byte, error) {
	if len(cr.left) == 0 && cr.err != nil {
		return nil, cr.err
	}
	i := 0
	for {
		if i == len(cr.left) {
			cr.fill()
			if i == len(cr.left) {
				if i == 0 {
					return nil, cr.err
				}
				line := cr.left
				cr.left = cr.left[len(cr.left):]
				return line, nil
			}
		}
		switch cr.left[i] {
		case '\\':
			i++
			if i == len(cr.left) {
				cr.fill()
			}
			if i == len(cr.left) {
				line := cr.left
				cr.left = cr.left[len(cr.left):]
				return line, nil
			}
		case '\r':
			if i+1 == len(cr.left) {
				cr.fill()
			}
			if i+1 < len(cr.left) && cr.left[i+1] == '\n' {
				i++
			}
			line := cr.left[:i+1]
			cr.left = cr.left[i+1:]
			return line, nil
		case '\n':
			line := cr.left[:i+1]
			cr.left = cr.left[i+1:]
			return line, nil
		}
		i++
	}
}

// Read reads the next command and returns the result of ParseCommand.
// If the command has a body, the whole content must be read before the next Read.
//
// The possible errors are as follows:
//   - the next command is not available or
//   - ParseCommand returns an error.
// If the underlying io.Reader returns io.EOF and the read bytes are exhausted,
// Read returns io.EOF.
// Otherwise, Read returns *Error.
func (cr *CommandReader) Read() (*Command, error) {
	if len(cr.left) == 0 && cr.err != nil {
		return nil, cr.err
	}
	for {
		line, err := cr.readLine()
		if err != nil {
			return nil, err
		}
		cmd := bytes.TrimLeft(line, commandSpaces)
		if len(cmd) != 0 {
			cmd, err := ParseCommand(string(cmd))
			if err != nil {
				return nil, err
			}
			if cmd.NeedsBody() {
				cmd.SetBody(newCommandBodyReader(cr))
			}
			return cmd, nil
		}
	}
}
