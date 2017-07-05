package grnci

import (
	"testing"
)

func TestFormatParamValue(t *testing.T) {
	if actual, err := formatParamValue("", true); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "yes"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", false); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "no"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}

	if actual, err := formatParamValue("", int8(-128)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "-128"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", int16(-32768)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "-32768"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", int32(-2147483648)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "-2147483648"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", int64(-9223372036854775808)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "-9223372036854775808"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", int(-9223372036854775808)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "-9223372036854775808"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}

	if actual, err := formatParamValue("", uint8(255)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "255"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", uint16(65535)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "65535"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", uint32(4294967295)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "4294967295"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", uint64(18446744073709551615)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "18446744073709551615"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", uint(18446744073709551615)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "18446744073709551615"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}

	if actual, err := formatParamValue("", float32(1.234567890123456789)); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "1.2345679"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamValue("", 1.234567890123456789); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "1.2345678901234567"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}

	if actual, err := formatParamValue("", "String"); err != nil {
		t.Fatalf("formatParamValue failed: %v", err)
	} else if want := "String"; actual != want {
		t.Fatalf("formatParamValue failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamYesNo(t *testing.T) {
	if actual, err := formatParamYesNo("", true); err != nil {
		t.Fatalf("formatParamYesNo failed: %v", err)
	} else if want := "yes"; actual != want {
		t.Fatalf("formatParamYesNo failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamYesNo("", false); err != nil {
		t.Fatalf("formatParamYesNo failed: %v", err)
	} else if want := "no"; actual != want {
		t.Fatalf("formatParamYesNo failed: actual = %s, want = %s", actual, want)
	}

	if actual, err := formatParamYesNo("", "yes"); err != nil {
		t.Fatalf("formatParamYesNo failed: %v", err)
	} else if want := "yes"; actual != want {
		t.Fatalf("formatParamYesNo failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamYesNo("", "no"); err != nil {
		t.Fatalf("formatParamYesNo failed: %v", err)
	} else if want := "no"; actual != want {
		t.Fatalf("formatParamYesNo failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamBorder(t *testing.T) {
	if actual, err := formatParamBorder("", true); err != nil {
		t.Fatalf("formatParamBorder failed: %v", err)
	} else if want := "include"; actual != want {
		t.Fatalf("formatParamBorder failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamBorder("", false); err != nil {
		t.Fatalf("formatParamBorder failed: %v", err)
	} else if want := "exclude"; actual != want {
		t.Fatalf("formatParamBorder failed: actual = %s, want = %s", actual, want)
	}

	if actual, err := formatParamBorder("", "include"); err != nil {
		t.Fatalf("formatParamBorder failed: %v", err)
	} else if want := "include"; actual != want {
		t.Fatalf("formatParamBorder failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamBorder("", "exclude"); err != nil {
		t.Fatalf("formatParamBorder failed: %v", err)
	} else if want := "exclude"; actual != want {
		t.Fatalf("formatParamBorder failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamCSV(t *testing.T) {
	if actual, err := formatParamCSV("", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("formatParamCSV failed: %v", err)
	} else if want := "a,b,c"; actual != want {
		t.Fatalf("formatParamCSV failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamFlags(t *testing.T) {
	if actual, err := formatParamFlags("", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("formatParamFlags failed: %v", err)
	} else if want := "a|b|c"; actual != want {
		t.Fatalf("formatParamFlags failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamMatchColumns(t *testing.T) {
	if actual, err := formatParamMatchColumns("", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("formatParamMatchColumns failed: %v", err)
	} else if want := "a||b||c"; actual != want {
		t.Fatalf("formatParamMatchColumns failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamJSON(t *testing.T) {
	if actual, err := formatParamJSON("", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("formatParamJSON failed: %v", err)
	} else if want := `["a","b","c"]`; actual != want {
		t.Fatalf("formatParamJSON failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamDefault(t *testing.T) {
	if actual, err := formatParamDefault("", true); err == nil {
		t.Fatalf("formatParamDefault wrongly succeeded: actual = %s", actual)
	}
	if actual, err := formatParamDefault("output-columns", true); err == nil {
		t.Fatalf("formatParamDefault wrongly succeeded: actual = %s", actual)
	}

	if actual, err := formatParamDefault("cache", true); err != nil {
		t.Fatalf("formatParamDefault failed: %v", err)
	} else if want := "yes"; actual != want {
		t.Fatalf("formatParamDefault failed: actual = %s, want = %s", actual, want)
	}
}

func TestFormatParamSelect(t *testing.T) {
	if actual, err := formatParamSelect("", true); err == nil {
		t.Fatalf("formatParamSelect wrongly succeeded: actual = %s", actual)
	}
	if actual, err := formatParamSelect("output/columns", true); err == nil {
		t.Fatalf("formatParamSelect wrongly succeeded: actual = %s", actual)
	}

	if actual, err := formatParamSelect("cache", true); err != nil {
		t.Fatalf("formatParamSelect failed: %v", err)
	} else if want := "yes"; actual != want {
		t.Fatalf("formatParamSelect failed: actual = %s, want = %s", actual, want)
	}

	if actual, err := formatParamSelect("columns[NAME].flags", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("formatParamSelect failed: %v", err)
	} else if want := "a|b|c"; actual != want {
		t.Fatalf("formatParamSelect failed: actual = %s, want = %s", actual, want)
	}
	if actual, err := formatParamSelect("drilldown[LABEL].columns[NAME].flags", []string{"a", "b", "c"}); err != nil {
		t.Fatalf("formatParamSelect failed: %v", err)
	} else if want := "a|b|c"; actual != want {
		t.Fatalf("formatParamSelect failed: actual = %s, want = %s", actual, want)
	}
}

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
