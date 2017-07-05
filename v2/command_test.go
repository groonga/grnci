package grnci

import (
	"io"
	"io/ioutil"
	"strings"
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
		"table":                 "Tbl",
		"match_columns":         []string{"title", "body"},
		"query":                 "Japan",
		"filter":                "value < 100",
		"sort_keys":             []string{"value", "_key"},
		"output_columns":        []string{"_id", "_key", "value", "percent"},
		"cache":                 false,
		"offset":                0,
		"limit":                 -1,
		"column[percent].stage": "output",
		"column[percent].type":  "Float",
		"column[percent].value": "value / 100",
	}
	cmd, err := NewCommand("select", params)
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}
	if actual, want := cmd.Name(), "select"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["table"], "Tbl"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["match_columns"], "title||body"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["query"], "Japan"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["filter"], "value < 100"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["sort_keys"], "value,_key"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["output_columns"], "_id,_key,value,percent"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["cache"], "no"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["offset"], "0"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["limit"], "-1"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["column[percent].stage"], "output"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["column[percent].type"], "Float"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.params["column[percent].value"], "value / 100"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
}

func TestParseCommand(t *testing.T) {
	cmd, err := ParseCommand(`select Tbl --query '"apple juice"' --filter 'price < 100' --cache no`)
	if err != nil {
		t.Fatalf("ParseCommand failed: %v", err)
	}
	if actual, want := cmd.Name(), "select"; actual != want {
		t.Fatalf("ParseCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.Params()["table"], "Tbl"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.Params()["query"], "\"apple juice\""; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.Params()["filter"], "price < 100"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if actual, want := cmd.Params()["cache"], "no"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
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
	if actual, want := cmd.Params()["table"], "Tbl"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if err := cmd.SetParam("cache", false); err != nil {
		t.Fatalf("cmd.SetParam failed: %v", err)
	}
	if actual, want := cmd.Params()["cache"], "no"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if err := cmd.SetParam("cache", true); err != nil {
		t.Fatalf("cmd.SetParam failed: %v", err)
	}
	if actual, want := cmd.Params()["cache"], "yes"; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
	if err := cmd.SetParam("cache", nil); err != nil {
		t.Fatalf("cmd.SetParam failed: %v", err)
	}
	if actual, want := cmd.Params()["cache"], ""; actual != want {
		t.Fatalf("NewCommand failed: actual = %s, want = %s", actual, want)
	}
}

func TestCommandString(t *testing.T) {
	params := map[string]interface{}{
		"table":         "Tbl",
		"cache":         "no",
		"limit":         -1,
		"match_columns": []string{"title", "body"},
		"query":         `"de facto"`,
	}
	cmd, err := NewCommand("select", params)
	if err != nil {
		t.Fatalf("NewCommand failed: %v", err)
	}
	actual := cmd.String()
	want := `select --cache 'no' --limit '-1' --match_columns 'title||body' --query '"de facto"' --table 'Tbl'`
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

func TestCommandReader(t *testing.T) {
	dump := `table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR Text

table_create Idx TABLE_PAT_KEY ShortText \
  --default_tokenizer TokenBigram --normalizer NormalizerAuto
column_create Idx col COLUMN_INDEX|WITH_POSITION Tbl col

load --table Tbl
[
["col"],
["Hello, world!"],
["'{' is called a left brace."]
]
`
	cr := NewCommandReader(strings.NewReader(dump))
	if cmd, err := cr.Read(); err != nil {
		t.Fatalf("cr.Read failed: %v", err)
	} else if actual, want := cmd.Name(), "table_create"; actual != want {
		t.Fatalf("cr.Read failed: actual = %s, want = %s", actual, want)
	}
	if cmd, err := cr.Read(); err != nil {
		t.Fatalf("cr.Read failed: %v", err)
	} else if actual, want := cmd.Name(), "column_create"; actual != want {
		t.Fatalf("cr.Read failed: actual = %s, want = %s", actual, want)
	}
	if cmd, err := cr.Read(); err != nil {
		t.Fatalf("cr.Read failed: %v", err)
	} else if actual, want := cmd.Name(), "table_create"; actual != want {
		t.Fatalf("cr.Read failed: actual = %s, want = %s", actual, want)
	}
	if cmd, err := cr.Read(); err != nil {
		t.Fatalf("cr.Read failed: %v", err)
	} else if actual, want := cmd.Name(), "column_create"; actual != want {
		t.Fatalf("cr.Read failed: actual = %s, want = %s", actual, want)
	}
	if cmd, err := cr.Read(); err != nil {
		t.Fatalf("cr.Read failed: %v", err)
	} else if actual, want := cmd.Name(), "load"; actual != want {
		t.Fatalf("cr.Read failed: actual = %s, want = %s", actual, want)
	} else if body, err := ioutil.ReadAll(cmd.Body()); err != nil {
		t.Fatalf("io.ReadAll failed: %v", err)
	} else if actual, want := string(body), `[
["col"],
["Hello, world!"],
["'{' is called a left brace."]
]
`; actual != want {
		t.Fatalf("io.ReadAll failed: actual = %s, want = %s", actual, want)
	}
	if _, err := cr.Read(); err != io.EOF {
		t.Fatalf("cr.Read wongly succeeded")
	}
}
