package grnci

import "testing"

func TestNewError(t *testing.T) {
	err := NewError(StatusInvalidAddress, map[string]interface{}{
		"key": "value",
	})
	if err.Code != StatusInvalidAddress {
		t.Fatalf("NewError failed: Code: actual = %d, want = %d",
			err.Code, StatusInvalidAddress)
	}
	if err.Text != StatusText(StatusInvalidAddress) {
		t.Fatalf("NewError failed: Text: actual = %s, want = %s",
			err.Text, StatusText(StatusInvalidAddress))
	}
	if err.Data["key"] != "value" {
		t.Fatalf("NewError failed: Data[\"key\"]: actual = %s, want = %s",
			err.Data["key"], "value")
	}
}

func TestEnhanceError(t *testing.T) {
	err := NewError(StatusInvalidAddress, map[string]interface{}{
		"key": "value",
	})
	err = EnhanceError(err, map[string]interface{}{
		"newKey": "newValue",
	})
	if err.Code != StatusInvalidAddress {
		t.Fatalf("NewError failed: Code: actual = %d, want = %d",
			err.Code, StatusInvalidAddress)
	}
	if err.Text != StatusText(StatusInvalidAddress) {
		t.Fatalf("NewError failed: Text: actual = %s, want = %s",
			err.Text, StatusText(StatusInvalidAddress))
	}
	if err.Data["key"] != "value" {
		t.Fatalf("NewError failed: Data[\"key\"]: actual = %s, want = %s",
			err.Data["key"], "value")
	}
	if err.Data["newKey"] != "newValue" {
		t.Fatalf("NewError failed: Data[\"newKey\"]: actual = %s, want = %s",
			err.Data["newKey"], "newValue")
	}
}
