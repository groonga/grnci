package grnci

import (
	"testing"
)

func TestNewCommand(t *testing.T) {
	params := map[string]interface{}{
		"table":     "Tbl",
		"filter":    "value < 100",
		"sort_keys": "value",
		"cache":     false,
		"offset":    0,
		"limit":     -1,
	}
	cmd, err := NewCommand("select", params)
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}
	if cmd.Name() != "select" {
		t.Fatalf("NewCommand failed: name = %s, want = %s", cmd.Name(), "select")
	}
	if key, want := "table", "Tbl"; cmd.Params()[key] != want {
		t.Fatalf("NewCommand failed: params[\"%s\"] = %s, want = %v", key, cmd.Params()[key], want)
	}
	if key, want := "cache", "no"; cmd.Params()[key] != want {
		t.Fatalf("NewCommand failed: params[\"%s\"] = %s, want = %v", key, cmd.Params()[key], want)
	}
	if key, want := "limit", "-1"; cmd.Params()[key] != want {
		t.Fatalf("NewCommand failed: params[\"%s\"] = %s, want = %v", key, cmd.Params()[key], want)
	}
}

func TestParseCommand(t *testing.T) {
	cmd, err := ParseCommand(`select Tbl --query '"apple juice"' --filter 'price < 100' --cache no`)
	if err != nil {
		t.Fatalf("ParseCommand failed: %v", err)
	}
	if want := "select"; cmd.Name() != want {
		t.Fatalf("ParseCommand failed: name = %s, want = %s", cmd.Name(), want)
	}
	if key, want := "table", "Tbl"; cmd.Params()[key] != want {
		t.Fatalf("NewCommand failed: params[\"%s\"] = %s, want = %v", key, cmd.Params()[key], want)
	}
	if key, want := "query", `"apple juice"`; cmd.Params()[key] != want {
		t.Fatalf("NewCommand failed: params[\"%s\"] = %s, want = %v", key, cmd.Params()[key], want)
	}
	if key, want := "cache", "no"; cmd.Params()[key] != want {
		t.Fatalf("NewCommand failed: params[\"%s\"] = %s, want = %v", key, cmd.Params()[key], want)
	}
}

func TestCommandSetParam(t *testing.T) {
	cmd, err := NewCommand("select", nil)
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}
	if err := cmd.SetParam("", "Tbl"); err != nil {
		t.Fatalf("cmd.SetParam failed: %v", err)
	}
	if err := cmd.SetParam("cache", false); err != nil {
		t.Fatalf("cmd.SetParam failed: %v", err)
	}
	if err := cmd.SetParam("cache", true); err != nil {
		t.Fatalf("cmd.SetParam failed: %v", err)
	}
	if err := cmd.SetParam("cache", nil); err != nil {
		t.Fatalf("cmd.SetParam failed: %v", err)
	}
}

func TestCommandString(t *testing.T) {
	params := map[string]interface{}{
		"table": "Tbl",
		"cache": "no",
		"limit": -1,
	}
	cmd, err := NewCommand("select", params)
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}
	actual := cmd.String()
	want := "select --cache 'no' --limit '-1' --table 'Tbl'"
	if actual != want {
		t.Fatalf("cmd.String failed: actual = %s, want = %s", actual, want)
	}
}

func TestCommandNeedsBody(t *testing.T) {
	data := map[string]bool{
		"status":                       false,
		"select Tbl":                   false,
		"load --table Tbl":             true,
		"load --table Tbl --values []": false,
	}
	for src, want := range data {
		cmd, err := ParseCommand(src)
		if err != nil {
			t.Fatalf("ParseCommand failed: %v", err)
		}
		actual := cmd.NeedsBody()
		if actual != want {
			t.Fatalf("cmd.NeedsBody failed: cmd = %s, needsBody = %v, want = %v", cmd, actual, want)
		}
	}
}
