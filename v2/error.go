package grnci

import "encoding/json"

// Code is an error code.
type Code int

// List of error codes.
const (
	CodeSuccess = Code(0)
)

// String returns a string which briefly describes an error.
func (c Code) String() string {
	switch c {
	case CodeSuccess:
		return "Success"
	default:
		return "Unknown error"
	}
}

// Error stores details of an error.
type Error struct {
	Code    Code
	Message string
	Data    interface{}
}

// NewError creates an error object.
func NewError(code Code, data interface{}) error {
	return &Error{
		Code: code,
		Data: data,
	}
}

// Error returns a string which describes an error.
func (e Error) Error() string {
	b, _ := json.Marshal(e)
	return string(b)
}
