package grnci

// TODO: add functions to check parameters.

// checkParamKeyDefault is the default function to check parameter keys.
func checkParamKeyDefault(k string) error {
	if k == "" {
		return NewError(StatusInvalidCommand, map[string]interface{}{
			"key":   k,
			"error": "len(key) must not be 0.",
		})
	}
	for i := 0; i < len(k); i++ {
		switch {
		case k[i] >= '0' && k[i] <= '9':
		case k[i] >= 'a' && k[i] <= 'z':
		case k[i] >= 'A' && k[i] <= 'Z':
		default:
			switch k[i] {
			case '#', '@', '-', '_', '.', '[', ']':
			default:
				return NewError(StatusInvalidCommand, map[string]interface{}{
					"key":   k,
					"error": "key must consist of [0-9a-zA-Z#@-_.[]].",
				})
			}
		}
	}
	return nil
}

// checkParamValueDefault is the default function to check parameter values.
func checkParamValueDefault(v string) error {
	return nil
}

// checkParamDefault is the default function to check parameters.
func checkParamDefault(k, v string) error {
	if err := checkParamKeyDefault(k); err != nil {
		return EnhanceError(err, map[string]interface{}{
			"value": v,
		})
	}
	if err := checkParamValueDefault(v); err != nil {
		return EnhanceError(err, map[string]interface{}{
			"key": k,
		})
	}
	return nil
}

// checkCommand checks whether s is valid as a command.
func checkCommand(s string) error {
	if _, ok := CommandRules[s]; ok {
		return nil
	}
	if s == "" {
		return NewError(StatusInvalidCommand, map[string]interface{}{
			"command": s,
			"error":   "len(command) must not be 0.",
		})
	}
	for i := 0; i < len(s); i++ {
		if !(s[i] >= 'a' && s[i] <= 'z') && s[i] != '_' {
			return NewError(StatusInvalidCommand, map[string]interface{}{
				"command": s,
				"error":   "command must consist of [a-z_].",
			})
		}
	}
	return nil
}

// ParamRule is a parameter rule.
type ParamRule struct {
	Key          string               // Parameter key
	ValueChecker func(v string) error // Function to check parameter values
	Required     bool                 // Whether the parameter is required
}

// NewParamRule returns a new ParamRule.
func NewParamRule(key string, valueChecker func(v string) error, required bool) *ParamRule {
	return &ParamRule{
		Key:          key,
		ValueChecker: valueChecker,
		Required:     required,
	}
}

// CheckValue checks a parameter value.
func (pr *ParamRule) CheckValue(v string) error {
	if pr.ValueChecker != nil {
		return pr.ValueChecker(v)
	}
	return checkParamValueDefault(v)
}

// CommandRule is a command rule.
type CommandRule struct {
	ParamChecker  func(k, v string) error // Function to check uncommon parameters
	ParamRules    []*ParamRule            // Ordered common parameters
	ParamRulesMap map[string]*ParamRule   // Index for ParamRules
}

// GetCommandRule returns the command rule for the specified command.
func GetCommandRule(cmd string) *CommandRule {
	if cr := CommandRules[cmd]; cr != nil {
		return cr
	}
	return DefaultCommandRule
}

// NewCommandRule returns a new CommandRule.
func NewCommandRule(paramChecker func(k, v string) error, prs ...*ParamRule) *CommandRule {
	prMap := make(map[string]*ParamRule)
	for _, pr := range prs {
		prMap[pr.Key] = pr
	}
	return &CommandRule{
		ParamChecker:  paramChecker,
		ParamRules:    prs,
		ParamRulesMap: prMap,
	}
}

// CheckParam checks a parameter.
func (cr *CommandRule) CheckParam(k, v string) error {
	if cr, ok := cr.ParamRulesMap[k]; ok {
		if err := cr.CheckValue(v); err != nil {
			return EnhanceError(err, map[string]interface{}{
				"key": k,
			})
		}
		return nil
	}
	if cr.ParamChecker != nil {
		return cr.ParamChecker(k, v)
	}
	return checkParamDefault(k, v)
}

// commandRules is provided to hide CommandRules in doc.
var commandRules = map[string]*CommandRule{
	"cache_limit": NewCommandRule(
		nil,
		NewParamRule("max", nil, false),
	),
	"check": NewCommandRule(
		nil,
		NewParamRule("obj", nil, true),
	),
	"clearlock": NewCommandRule(
		nil,
		NewParamRule("objname", nil, true),
	),
	"column_copy": NewCommandRule(
		nil,
		NewParamRule("from_table", nil, true),
		NewParamRule("from_name", nil, true),
		NewParamRule("to_table", nil, true),
		NewParamRule("to_name", nil, true),
	),
	"column_create": NewCommandRule(
		nil,
		NewParamRule("table", nil, true),
		NewParamRule("name", nil, true),
		NewParamRule("flags", nil, true),
		NewParamRule("type", nil, true),
		NewParamRule("source", nil, false),
	),
	"column_list": NewCommandRule(
		nil,
		NewParamRule("table", nil, true),
	),
	"column_remove": NewCommandRule(
		nil,
		NewParamRule("table", nil, true),
		NewParamRule("name", nil, true),
	),
	"column_rename": NewCommandRule(
		nil,
		NewParamRule("table", nil, true),
		NewParamRule("name", nil, true),
		NewParamRule("new_name", nil, true),
	),
	"config_delete": NewCommandRule(
		nil,
		NewParamRule("key", nil, true),
	),
	"config_get": NewCommandRule(
		nil,
		NewParamRule("key", nil, true),
	),
	"config_set": NewCommandRule(
		nil,
		NewParamRule("key", nil, true),
		NewParamRule("value", nil, true),
	),
	"database_unmap": NewCommandRule(
		nil,
	),
	"define_selector": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
		NewParamRule("table", nil, true),
		NewParamRule("match_columns", nil, false),
		NewParamRule("query", nil, false),
		NewParamRule("filter", nil, false),
		NewParamRule("scorer", nil, false),
		NewParamRule("sortby", nil, false),
		NewParamRule("output_columns", nil, false),
		NewParamRule("offset", nil, false),
		NewParamRule("limit", nil, false),
		NewParamRule("drilldown", nil, false),
		NewParamRule("drilldown_sortby", nil, false),
		NewParamRule("drilldown_output_columns", nil, false),
		NewParamRule("drilldown_offset", nil, false),
		NewParamRule("drilldown_limit", nil, false),
	),
	"defrag": NewCommandRule(
		nil,
		NewParamRule("objname", nil, true),
		NewParamRule("threshold", nil, true),
	),
	"delete": NewCommandRule(
		nil,
		NewParamRule("table", nil, true),
		NewParamRule("key", nil, false),
		NewParamRule("id", nil, false),
		NewParamRule("filter", nil, false),
	),
	"dump": NewCommandRule(
		nil,
		NewParamRule("tables", nil, false),
		NewParamRule("dump_plugins", nil, false),
		NewParamRule("dump_schema", nil, false),
		NewParamRule("dump_records", nil, false),
		NewParamRule("dump_indexes", nil, false),
	),
	"io_flush": NewCommandRule(
		nil,
		NewParamRule("target_name", nil, false),
		NewParamRule("recursive", nil, false),
	),
	"load": NewCommandRule(
		nil,
		NewParamRule("values", nil, false), // values may be passes as a body.
		NewParamRule("table", nil, true),
		NewParamRule("columns", nil, false),
		NewParamRule("ifexists", nil, false),
		NewParamRule("input_type", nil, false),
	),
	"lock_acquire": NewCommandRule(
		nil,
		NewParamRule("target_name", nil, false),
	),
	"lock_clear": NewCommandRule(
		nil,
		NewParamRule("target_name", nil, false),
	),
	"lock_release": NewCommandRule(
		nil,
		NewParamRule("target_name", nil, false),
	),
	"log_level": NewCommandRule(
		nil,
		NewParamRule("level", nil, true),
	),
	"log_put": NewCommandRule(
		nil,
		NewParamRule("level", nil, true),
		NewParamRule("message", nil, true),
	),
	"log_reopen": NewCommandRule(
		nil,
	),
	"logical_count": NewCommandRule(
		nil,
		NewParamRule("logical_table", nil, true),
		NewParamRule("shard_key", nil, true),
		NewParamRule("min", nil, false),
		NewParamRule("min_border", nil, false),
		NewParamRule("max", nil, false),
		NewParamRule("max_border", nil, false),
		NewParamRule("filter", nil, false),
	),
	"logical_parameters": NewCommandRule(
		nil,
		NewParamRule("range_index", nil, false),
	),
	"logical_range_filter": NewCommandRule(
		nil,
		NewParamRule("logical_table", nil, true),
		NewParamRule("shard_key", nil, true),
		NewParamRule("min", nil, false),
		NewParamRule("min_border", nil, false),
		NewParamRule("max", nil, false),
		NewParamRule("max_border", nil, false),
		NewParamRule("order", nil, false),
		NewParamRule("filter", nil, false),
		NewParamRule("offset", nil, false),
		NewParamRule("limit", nil, false),
		NewParamRule("output_columns", nil, false),
		NewParamRule("use_range_index", nil, false),
	),
	"logical_select": NewCommandRule(
		nil,
		NewParamRule("logical_table", nil, true),
		NewParamRule("shard_key", nil, true),
		NewParamRule("min", nil, false),
		NewParamRule("min_border", nil, false),
		NewParamRule("max", nil, false),
		NewParamRule("max_border", nil, false),
		NewParamRule("filter", nil, false),
		NewParamRule("sortby", nil, false),
		NewParamRule("output_columns", nil, false),
		NewParamRule("offset", nil, false),
		NewParamRule("limit", nil, false),
		NewParamRule("drilldown", nil, false),
		NewParamRule("drilldown_sortby", nil, false),
		NewParamRule("drilldown_output_columns", nil, false),
		NewParamRule("drilldown_offset", nil, false),
		NewParamRule("drilldown_limit", nil, false),
		NewParamRule("drilldown_calc_types", nil, false),
		NewParamRule("drilldown_calc_target", nil, false),
		NewParamRule("sort_keys", nil, false),
		NewParamRule("drilldown_sort_keys", nil, false),
		NewParamRule("match_columns", nil, false),
		NewParamRule("query", nil, false),
		NewParamRule("drilldown_filter", nil, false),
	),
	"logical_shard_list": NewCommandRule(
		nil,
		NewParamRule("logical_table", nil, true),
	),
	"logical_table_remove": NewCommandRule(
		nil,
		NewParamRule("logical_table", nil, true),
		NewParamRule("shard_key", nil, true),
		NewParamRule("min", nil, false),
		NewParamRule("min_border", nil, false),
		NewParamRule("max", nil, false),
		NewParamRule("max_border", nil, false),
		NewParamRule("dependent", nil, false),
		NewParamRule("force", nil, false),
	),
	"normalize": NewCommandRule(
		nil,
		NewParamRule("normalizer", nil, true),
		NewParamRule("string", nil, true),
		NewParamRule("flags", nil, false),
	),
	"normalizer_list": NewCommandRule(
		nil,
	),
	"object_exist": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
	),
	"object_inspect": NewCommandRule(
		nil,
		NewParamRule("name", nil, false),
	),
	"object_list": NewCommandRule(
		nil,
	),
	"object_remove": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
		NewParamRule("force", nil, false),
	),
	"plugin_register": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
	),
	"plugin_unregister": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
	),
	"query_expand": NewCommandRule(
		nil,
	), // TODO
	"quit": NewCommandRule(
		nil,
	),
	"range_filter": NewCommandRule(
		nil,
	), // TODO
	"register": NewCommandRule(
		nil,
		NewParamRule("path", nil, true),
	),
	"reindex": NewCommandRule(
		nil,
		NewParamRule("target_name", nil, false),
	),
	"request_cancel": NewCommandRule(
		nil,
		NewParamRule("id", nil, true),
	),
	"ruby_eval": NewCommandRule(
		nil,
		NewParamRule("script", nil, true),
	),
	"ruby_load": NewCommandRule(
		nil,
		NewParamRule("path", nil, true),
	),
	"schema": NewCommandRule(
		nil,
	),
	"select": NewCommandRule(
		nil,
		NewParamRule("table", nil, true),
		NewParamRule("match_columns", nil, false),
		NewParamRule("query", nil, false),
		NewParamRule("filter", nil, false),
		NewParamRule("scorer", nil, false),
		NewParamRule("sortby", nil, false),
		NewParamRule("output_columns", nil, false),
		NewParamRule("offset", nil, false),
		NewParamRule("limit", nil, false),
		NewParamRule("drilldown", nil, false),
		NewParamRule("drilldown_sortby", nil, false),
		NewParamRule("drilldown_output_columns", nil, false),
		NewParamRule("drilldown_offset", nil, false),
		NewParamRule("drilldown_limit", nil, false),
		NewParamRule("cache", nil, false),
		NewParamRule("match_escalation_threshold", nil, false),
		NewParamRule("query_expansion", nil, false),
		NewParamRule("query_flags", nil, false),
		NewParamRule("query_expander", nil, false),
		NewParamRule("adjuster", nil, false),
		NewParamRule("drilldown_calc_types", nil, false),
		NewParamRule("drilldown_calc_target", nil, false),
		NewParamRule("drilldown_filter", nil, false),
		NewParamRule("sort_keys", nil, false),
		NewParamRule("drilldown_sort_keys", nil, false),
	),
	"shutdown": NewCommandRule(
		nil,
		NewParamRule("mode", nil, false),
	),
	"status": NewCommandRule(
		nil,
	),
	"suggest": NewCommandRule(
		nil,
		NewParamRule("types", nil, true),
		NewParamRule("table", nil, true),
		NewParamRule("column", nil, true),
		NewParamRule("query", nil, true),
		NewParamRule("sortby", nil, false),
		NewParamRule("output_columns", nil, false),
		NewParamRule("offset", nil, false),
		NewParamRule("limit", nil, false),
		NewParamRule("frequency_threshold", nil, false),
		NewParamRule("conditional_probability_threshold", nil, false),
		NewParamRule("prefix_search", nil, false),
	),
	"table_copy": NewCommandRule(
		nil,
		NewParamRule("from_name", nil, true),
		NewParamRule("to_name", nil, true),
	),
	"table_create": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
		NewParamRule("flags", nil, false),
		NewParamRule("key_type", nil, false),
		NewParamRule("value_type", nil, false),
		NewParamRule("default_tokenizer", nil, false),
		NewParamRule("normalizer", nil, false),
		NewParamRule("token_filters", nil, false),
	),
	"table_list": NewCommandRule(
		nil,
	),
	"table_remove": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
		NewParamRule("dependent", nil, false),
	),
	"table_rename": NewCommandRule(
		nil,
		NewParamRule("name", nil, true),
		NewParamRule("new_name", nil, true),
	),
	"table_tokenize": NewCommandRule(
		nil,
		NewParamRule("table", nil, true),
		NewParamRule("string", nil, true),
		NewParamRule("flags", nil, false),
		NewParamRule("mode", nil, false),
		NewParamRule("index_column", nil, false),
	),
	"thread_limit": NewCommandRule(
		nil,
		NewParamRule("max", nil, false),
	),
	"tokenize": NewCommandRule(
		nil,
		NewParamRule("tokenizer", nil, true),
		NewParamRule("string", nil, true),
		NewParamRule("normalizer", nil, false),
		NewParamRule("flags", nil, false),
		NewParamRule("mode", nil, false),
		NewParamRule("token_filters", nil, false),
	),
	"tokenizer_list": NewCommandRule(
		nil,
	),
	"truncate": NewCommandRule(
		nil,
		NewParamRule("target_name", nil, true),
	),
}

// CommandRules is a map of command rules.
var CommandRules = commandRules

// DefaultCommandRule is applied to commands not listed in CommandRules.
var DefaultCommandRule = NewCommandRule(nil)
