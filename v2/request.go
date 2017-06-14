package grnci

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

// Request is a request.
type Request struct {
	Command     string            // Command name
	CommandRule *CommandRule      // Command rule
	Params      map[string]string // Command parameters
	NAnonParams int               // Number of unnamed parameters
	Body        io.Reader         // Body (nil is allowed)
}

// newRequest returns a new Request with empty Params.
func newRequest(cmd string, body io.Reader) *Request {
	return &Request{
		Command:     cmd,
		CommandRule: GetCommandRule(cmd),
		Params:      make(map[string]string),
		Body:        body,
	}
}

// NewRequest returns a new Request.
func NewRequest(cmd string, params map[string]interface{}, body io.Reader) (*Request, error) {
	if err := checkCommand(cmd); err != nil {
		return nil, err
	}
	r := newRequest(cmd, body)
	for k, v := range params {
		if err := r.AddParam(k, v); err != nil {
			return nil, EnhanceError(err, map[string]interface{}{
				"command": cmd,
			})
		}
	}
	return r, nil
}

// unescapeCommandByte returns an unescaped byte.
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

// tokenizeCommand tokenizes s as a command.
func tokenizeCommand(s string) []string {
	var tokens []string
	var token []byte
	for {
		s = strings.TrimLeft(s, " \t\r\n")
		if len(s) == 0 {
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
					break
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
						break Loop
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
	return tokens
}

// ParseRequest parses a request.
func ParseRequest(cmd string, body io.Reader) (*Request, error) {
	tokens := tokenizeCommand(cmd)
	if len(tokens) == 0 {
		return nil, NewError(StatusInvalidCommand, map[string]interface{}{
			"tokens": tokens,
			"error":  "len(tokens) must not be 0.",
		})
	}
	if err := checkCommand(tokens[0]); err != nil {
		return nil, err
	}
	r := newRequest(tokens[0], body)
	for i := 1; i < len(tokens); i++ {
		var k, v string
		if strings.HasPrefix(tokens[i], "--") {
			k = tokens[i][2:]
			i++
			if i < len(tokens) {
				v = tokens[i]
			}
		} else {
			v = tokens[i]
		}
		if err := r.AddParam(k, v); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// convertParamValue converts a parameter value.
func (r *Request) convertParamValue(k string, v interface{}) (string, error) {
	if v == nil {
		return "null", nil
	}
	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Bool:
		return strconv.FormatBool(val.Bool()), nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(val.Int(), 10), nil
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(val.Uint(), 10), nil
	case reflect.String:
		return val.String(), nil
	default:
		return "", NewError(StatusInvalidCommand, map[string]interface{}{
			"key":   k,
			"value": v,
			"error": "The value type is not supported.",
		})
	}
}

// AddParam adds a parameter.
// AddParam assumes that Command is already set.
func (r *Request) AddParam(key string, value interface{}) error {
	if r.CommandRule == nil {
		r.CommandRule = GetCommandRule(r.Command)
	}
	if key == "" {
		if r.NAnonParams >= len(r.CommandRule.ParamRules) {
			return NewError(StatusInvalidCommand, map[string]interface{}{
				"command": r.Command,
				"error": fmt.Sprintf("The command accepts at most %d unnamed parameters.",
					len(r.CommandRule.ParamRules)),
			})
		}
		pr := r.CommandRule.ParamRules[r.NAnonParams]
		if err := pr.CheckValue(value); err != nil {
			return EnhanceError(err, map[string]interface{}{
				"command": r.Command,
				"key":     key,
			})
		}
		v, err := r.convertParamValue(pr.Key, value)
		if err != nil {
			return EnhanceError(err, map[string]interface{}{
				"command": r.Command,
			})
		}
		if r.Params == nil {
			r.Params = make(map[string]string)
		}
		r.Params[pr.Key] = v
		r.NAnonParams++
		return nil
	}
	if err := r.CommandRule.CheckParam(key, value); err != nil {
		return EnhanceError(err, map[string]interface{}{
			"command": r.Command,
		})
	}
	v, err := r.convertParamValue(key, value)
	if err != nil {
		return EnhanceError(err, map[string]interface{}{
			"command": r.Command,
		})
	}
	if r.Params == nil {
		r.Params = make(map[string]string)
	}
	r.Params[key] = v
	return nil
}

// RemoveParam removes a parameter.
func (r *Request) RemoveParam(key string) error {
	if _, ok := r.Params[key]; !ok {
		return NewError(StatusInvalidOperation, map[string]interface{}{
			"key":   key,
			"error": "The key does not exist.",
		})
	}
	delete(r.Params, key)
	return nil
}

// GQTPRequest returns components for a GQTP request.
// If the request is invalid, GQTPRequest returns an error.
//
// GQTPRequest assembles Command and Params into a string.
// Parameters in the string are sorted in key order.
func (r *Request) GQTPRequest() (cmd string, body io.Reader, err error) {
	if err = r.Check(); err != nil {
		return
	}
	size := len(r.Command)
	for k, v := range r.Params {
		size += len(k) + 3
		size += len(v)*2 + 3
	}
	buf := make([]byte, 0, size)
	buf = append(buf, r.Command...)
	keys := make([]string, 0, len(r.Params))
	for k := range r.Params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := r.Params[k]
		buf = append(buf, " --"...)
		buf = append(buf, k...)
		buf = append(buf, " '"...)
		for i := 0; i < len(v); i++ {
			switch v[i] {
			case '\'', '\\', '\b', '\t', '\r', '\n':
				buf = append(buf, '\\')
			}
			buf = append(buf, v[i])
		}
		buf = append(buf, '\'')
	}
	cmd = string(buf)
	body = r.Body
	return
}

// HTTPRequest returns components for an HTTP request.
// If the request is invalid, HTTPRequest returns an error.
func (r *Request) HTTPRequest() (cmd string, params map[string]string, body io.Reader, err error) {
	if err = r.Check(); err != nil {
		return
	}
	cmd = r.Command
	params = r.Params
	body = r.Body
	return
}

// NeedBody returns whether or not the request requires a body.
func (r *Request) NeedBody() bool {
	switch r.Command {
	case "load":
		_, ok := r.Params["values"]
		return !ok
	default:
		return false
	}
}

// Check checks whether or not the request is valid.
func (r *Request) Check() error {
	if err := checkCommand(r.Command); err != nil {
		return err
	}
	cr := r.CommandRule
	if cr == nil {
		cr = GetCommandRule(r.Command)
	}
	for k, v := range r.Params {
		if err := cr.CheckParam(k, v); err != nil {
			return EnhanceError(err, map[string]interface{}{
				"command": r.Command,
			})
		}
	}
	for _, pr := range cr.ParamRules {
		if pr.Required {
			if _, ok := r.Params[pr.Key]; !ok {
				return NewError(StatusInvalidCommand, map[string]interface{}{
					"command": r.Command,
					"key":     pr.Key,
					"error":   "The parameter is required.",
				})
			}
		}
	}
	switch r.Command {
	case "load":
		if _, ok := r.Params["values"]; ok {
			if r.Body != nil {
				return NewError(StatusInvalidCommand, map[string]interface{}{
					"command":   r.Command,
					"hasValues": true,
					"hasBody":   true,
					"error":     "The command does not accept a body.",
				})
			}
		} else if r.Body == nil {
			return NewError(StatusInvalidCommand, map[string]interface{}{
				"command":   r.Command,
				"hasValues": false,
				"hasBody":   false,
				"error":     "The command requires a body.",
			})
		}
	default:
		if r.Body != nil {
			return NewError(StatusInvalidCommand, map[string]interface{}{
				"command": r.Command,
				"hasBody": true,
				"error":   "The command does not accept a body.",
			})
		}
	}
	return nil
}
