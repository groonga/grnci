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

func TestNewError2(t *testing.T) {
	msg := "This is a test of NewError2."
	data := map[string]interface{}{
		"string": "value",
		"int":    100,
	}
	err := NewError2(AddressError, msg, data)
	if err.Code != AddressError {
		t.Fatalf("NewError2 failed: Code = %d, want = %d", err.Code, AddressError)
	}
	if err.Message != msg {
		t.Fatalf("NewError2 failed: Message = %s, want = %s", err.Message, msg)
	}
	for k, v := range data {
		if err.Data[k] != v {
			t.Fatalf("NewError failed: Data[\"%s\"] = %s, want = %s", k, err.Data[k], v)
		}
	}
}
