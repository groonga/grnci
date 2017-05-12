package grnci

import (
	"errors"
	"fmt"
)

// Argument stores a command argument.
//
// If Key != "", it is a named argument.
// Otherwise, it is an unnamed argument.
// Note that the order of unnamed arguments is important.
type Argument struct {
	Key   string
	Value string
}

// isDigit checks if c is a digit.
func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// isAlpha checks if c is an alphabet.
func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isAlnum checks if c is a digit or alphabet.
func isAlnum(c byte) bool {
	return isDigit(c) || isAlpha(c)
}

// checkArgumentKey checks if s is valid as an argument key.
func checkArgumentKey(s string) error {
	if s == "" {
		return errors.New("invalid format: s = ")
	}
	for i := 0; i < len(s); i++ {
		if isAlnum(s[i]) {
			continue
		}
		switch s[i] {
		case '#', '@', '-', '_', '.', '[', ']':
		default:
			return fmt.Errorf("invalid format: s = %s", s)
		}
	}
	return nil
}

// Check checks if arg is valid.
func (a *Argument) Check() error {
	if err := checkArgumentKey(a.Key); err != nil {
		return fmt.Errorf("checkArgumentKey failed: %v", err)
	}
	return nil
}
