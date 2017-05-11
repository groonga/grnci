package grnci

import (
	"errors"
	"fmt"
	"io"
)

// Request stores a Groonga command with arguments.
type Request struct {
	Cmd  string
	Args []Argument
	Body io.Reader
}

// checkCmd checks if s is valid as a command name.
func checkCmd(s string) error {
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
func (req *Request) Check() error {
	if err := checkCmd(req.Cmd); err != nil {
		return fmt.Errorf("CheckCmd failed: %v", err)
	}
	for _, arg := range req.Args {
		if err := arg.Check(); err != nil {
			return fmt.Errorf("arg.Check failed: %v", err)
		}
	}
	return nil
}
