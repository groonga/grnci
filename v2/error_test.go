package grnci

import "testing"

func TestNewError(t *testing.T) {
	data := map[string]interface{}{
		"string": "value",
		"int":    100,
	}
	err := NewError(AddressError, data)
	if err.Code != AddressError {
		t.Fatalf("NewError failed: Code: actual = %d, want = %d",
			err.Code, AddressError)
	}
	if err.Text != getCodeText(AddressError) {
		t.Fatalf("NewError failed: Text: actual = %s, want = %s",
			err.Text, getCodeText(AddressError))
	}
	for k, v := range data {
		if err.Data[k] != v {
			t.Fatalf("NewError failed: Data[\"key\"]: actual = %s, want = %s", err.Data[k], v)
		}
	}
}

func TestEnhanceError(t *testing.T) {
	data := map[string]interface{}{
		"string": "value",
		"int":    100,
	}
	newData := map[string]interface{}{
		"string": "value2",
		"int":    1000,
		"float":  1.0,
	}
	err := NewError(AddressError, data)
	err = EnhanceError(err, newData)
	if err.Code != AddressError {
		t.Fatalf("NewError failed: Code: actual = %d, want = %d",
			err.Code, AddressError)
	}
	if err.Text != getCodeText(AddressError) {
		t.Fatalf("NewError failed: Text: actual = %s, want = %s",
			err.Text, getCodeText(AddressError))
	}
	for k, v := range newData {
		if err.Data[k] != v {
			t.Fatalf("NewError failed: Data[\"key\"]: actual = %s, want = %s", err.Data[k], v)
		}
	}
}
