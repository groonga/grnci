package grnci

import (
	"log"
	"testing"
)

func TestNewError(t *testing.T) {
	data := map[string]interface{}{
		"string": "value",
		"int":    100,
	}
	err := NewError(AddressError, data)
	if err.Code != AddressError {
		t.Fatalf("NewError failed: Code: actual = %d, want = %d", err.Code, AddressError)
	}
	for k, v := range data {
		if err.Data[k] != v {
			t.Fatalf("NewError failed: Data[\"%s\"]: actual = %s, want = %s", k, err.Data[k], v)
		}
	}
	log.Printf("err = %v", err)
}
