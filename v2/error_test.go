package grnci

import (
	"encoding/json"
	"testing"
)

func TestErrorCode(t *testing.T) {
	data, err := json.Marshal(AddressError)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	if want := `"AddressError"`; string(data) != want {
		t.Fatalf("json.Marshal failed: actual = %s, want = %s", data, want)
	}

	data, err = json.Marshal(ErrorCode(-22))
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	if want := `"GRN_INVALID_ARGUMENT"`; string(data) != want {
		t.Fatalf("json.Marshal failed: actual = %s, want = %s", data, want)
	}

	data, err = json.Marshal(ErrorCode(-12345))
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	if want := "-12345"; string(data) != want {
		t.Fatalf("json.Marshal failed: actual = %s, want = %s", data, want)
	}
}

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
}
