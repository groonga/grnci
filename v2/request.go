package grnci

import (
	"errors"
	"fmt"
	"io"
)

// Request stores a Groonga command with arguments.
type Request struct {
	Command   string
	Arguments []Argument
	Body      io.Reader
}

// checkCommand checks if s is valid as a command name.
func checkCommand(s string) error {
	if s == "" {
		return errors.New("invalid name: s = ")
	}
	if s[0] == '_' {
		return fmt.Errorf("invalid name: s = %s", s)
	}
	for i := 0; i < len(s); i++ {
		if !(s[i] >= 'a' && s[i] <= 'z') && s[i] != '_' {
			return fmt.Errorf("invalid name: s = %s", s)
		}
	}
	return nil
}

// Check checks if req is valid.
func (r *Request) Check() error {
	if err := checkCommand(r.Command); err != nil {
		return fmt.Errorf("checkCommand failed: %v", err)
	}
	for _, arg := range r.Arguments {
		if err := arg.Check(); err != nil {
			return fmt.Errorf("arg.Check failed: %v", err)
		}
	}
	return nil
}

// Assemble assembles Command and Arguments into command bytes.
//
// The command format is
// Command --Arguments[i].Key 'Arguments[i].Value' ...
func (r *Request) Assemble() ([]byte, error) {
	if err := r.Check(); err != nil {
		return nil, err
	}
	size := len(r.Command)
	for _, arg := range r.Arguments {
		if len(arg.Key) != 0 {
			size += len(arg.Key) + 3
		}
		size += len(arg.Value)*2 + 3
	}
	buf := make([]byte, 0, size)
	buf = append(buf, r.Command...)
	for _, arg := range r.Arguments {
		buf = append(buf, ' ')
		if len(arg.Key) != 0 {
			buf = append(buf, "--"...)
			buf = append(buf, arg.Key...)
		}
		buf = append(buf, '\'')
		for i := 0; i < len(arg.Value); i++ {
			switch arg.Value[i] {
			case '\'':
				buf = append(buf, '\'')
			case '\\':
				buf = append(buf, '\'')
			}
			buf = append(buf, arg.Value[i])
		}
		buf = append(buf, '\'')

	}
	return buf, nil
}
