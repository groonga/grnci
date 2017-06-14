package grnci

import (
	"fmt"
	"testing"
)

func TestNewRequest(t *testing.T) {
	params := map[string]interface{}{
		"table":     "Tbl",
		"filter":    "value < 100",
		"sort_keys": "value",
		"offset":    0,
		"limit":     -1,
	}
	req, err := NewRequest("select", params, nil)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	if req.Command != "select" {
		t.Fatalf("ParseRequest failed: cmd = %s, want = %s",
			req.Command, "select")
	}
	for key, value := range params {
		if req.Params[key] != fmt.Sprint(value) {
			t.Fatalf("ParseRequest failed: params[\"%s\"] = %s, want = %v",
				key, req.Params[key], value)
		}
	}
}

func TestParseRequest(t *testing.T) {
	req, err := ParseRequest(`select Tbl --query "\"apple juice\"" --filter 'price < 100'`, nil)
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	if req.Command != "select" {
		t.Fatalf("ParseRequest failed: command: actual = %s, want = %s",
			req.Command, "select")
	}
	if req.Params["table"] != "Tbl" {
		t.Fatalf("ParseRequest failed: params[\"table\"] = %s, want = %s",
			req.Params["table"], "Tbl")
	}
	if req.Params["query"] != "\"apple juice\"" {
		t.Fatalf("ParseRequest failed: params[\"query\"] = %s, want = %s",
			req.Params["query"], "apple juice")
	}
	if req.Params["filter"] != "price < 100" {
		t.Fatalf("ParseRequest failed: params[\"filter\"] = %s, want = %s",
			req.Params["filter"], "price < 100")
	}
}

func TestRequestAddParam(t *testing.T) {
	params := map[string]interface{}{
		"table":     "Tbl",
		"filter":    "value < 100",
		"sort_keys": "value",
		"offset":    0,
		"limit":     -1,
	}
	req, err := NewRequest("select", nil, nil)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	for key, value := range params {
		if err := req.AddParam(key, value); err != nil {
			t.Fatalf("req.AddParam failed: %v", err)
		}
	}
	if req.Command != "select" {
		t.Fatalf("req.AddParam failed: cmd = %s, want = %s",
			req.Command, "select")
	}
	for key, value := range params {
		if req.Params[key] != fmt.Sprint(value) {
			t.Fatalf("req.AddParam failed: params[\"%s\"] = %s, want = %v",
				key, req.Params[key], value)
		}
	}
}

func TestRequestRemoveParam(t *testing.T) {
	params := map[string]interface{}{
		"table":     "Tbl",
		"filter":    "value < 100",
		"sort_keys": "value",
		"offset":    0,
		"limit":     -1,
	}
	req, err := NewRequest("select", nil, nil)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	for key, value := range params {
		if err := req.AddParam(key, value); err != nil {
			t.Fatalf("req.AddParam failed: %v", err)
		}
	}
	for key := range params {
		if err := req.RemoveParam(key); err != nil {
			t.Fatalf("req.RemoveParam failed: %v", err)
		}
	}
	if req.Command != "select" {
		t.Fatalf("req.RemoveParam failed: cmd = %s, want = %s",
			req.Command, "select")
	}
	for key := range params {
		if _, ok := req.Params[key]; ok {
			t.Fatalf("req.RemoveParam failed: params[\"%s\"] = %s",
				key, req.Params[key])
		}
	}
}

func TestRequestCheck(t *testing.T) {
	data := map[string]bool{
		"status":                       true,
		"select Tbl":                   true,
		"select --123 xyz":             false,
		"_select --table Tbl":          true,
		"load --table Tbl":             false,
		"load --table Tbl --values []": true,
	}
	for cmd, want := range data {
		req, err := ParseRequest(cmd, nil)
		if err != nil {
			t.Fatalf("ParseRequest failed: %v", err)
		}
		err = req.Check()
		actual := err == nil
		if actual != want {
			t.Fatalf("req.Check failed: cmd = %s, actual = %v, want = %v, err = %v",
				cmd, actual, want, err)
		}
	}
}

func TestRequestGQTPRequest(t *testing.T) {
	params := map[string]interface{}{
		"table":     "Tbl",
		"filter":    "value < 100",
		"sort_keys": "value",
		"offset":    0,
		"limit":     -1,
	}
	req, err := NewRequest("select", params, nil)
	if err != nil {
		t.Fatalf("NewRequest failed: %v", err)
	}
	actual, _, err := req.GQTPRequest()
	if err != nil {
		t.Fatalf("req.GQTPRequest failed: %v", err)
	}
	want := "select --filter 'value < 100' --limit '-1' --offset '0' --sort_keys 'value' --table 'Tbl'"
	if actual != want {
		t.Fatalf("req.GQTPRequest failed: actual = %s, want = %s",
			actual, want)
	}
}

func TestRequestHTTPRequest(t *testing.T) {
	req, err := ParseRequest(`select Tbl --query "\"apple juice\"" --filter 'price < 100'`, nil)
	if err != nil {
		t.Fatalf("ParseRequest failed: %v", err)
	}
	cmd, params, _, err := req.HTTPRequest()
	if err != nil {
		t.Fatalf("req.HTTPRequest failed: %v", err)
	}
	if cmd != "select" {
		t.Fatalf("req.HTTPRequest failed: cmd = %s, want = %s", cmd, "select")
	}
	if params["table"] != "Tbl" {
		t.Fatalf("ParseRequest failed: params[\"table\"] = %s, want = %s",
			params["table"], "Tbl")
	}
	if params["query"] != "\"apple juice\"" {
		t.Fatalf("ParseRequest failed: params[\"query\"] = %s, want = %s",
			params["query"], "apple juice")
	}
	if params["filter"] != "price < 100" {
		t.Fatalf("ParseRequest failed: params[\"filter\"] = %s, want = %s",
			params["filter"], "price < 100")
	}
}

func TestRequestNeedBody(t *testing.T) {
	data := map[string]bool{
		"status":                       false,
		"select Tbl":                   false,
		"load --table Tbl":             true,
		"load --table Tbl --values []": false,
	}
	for cmd, want := range data {
		req, err := ParseRequest(cmd, nil)
		if err != nil {
			t.Fatalf("ParseRequest failed: %v", err)
		}
		actual := req.NeedBody()
		if actual != want {
			t.Fatalf("req.NeedBody failed: actual = %v, want = %v", actual, want)
		}
	}
}
