package grnci

import "testing"

func TestNewError(t *testing.T) {
	data := map[string]interface{}{
		"string": "value",
		"int":    100,
	}
	err := NewError(InvalidAddress, data).(*Error)
	if err.Code != InvalidAddress {
		t.Fatalf("NewError failed: Code: actual = %d, want = %d",
			err.Code, InvalidAddress)
	}
	if err.Text != getCodeText(InvalidAddress) {
		t.Fatalf("NewError failed: Text: actual = %s, want = %s",
			err.Text, getCodeText(InvalidAddress))
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
	err := NewError(InvalidAddress, data).(*Error)
	err = EnhanceError(err, newData).(*Error)
	if err.Code != InvalidAddress {
		t.Fatalf("NewError failed: Code: actual = %d, want = %d",
			err.Code, InvalidAddress)
	}
	if err.Text != getCodeText(InvalidAddress) {
		t.Fatalf("NewError failed: Text: actual = %s, want = %s",
			err.Text, getCodeText(InvalidAddress))
	}
	for k, v := range newData {
		if err.Data[k] != v {
			t.Fatalf("NewError failed: Data[\"key\"]: actual = %s, want = %s", err.Data[k], v)
		}
	}
}
