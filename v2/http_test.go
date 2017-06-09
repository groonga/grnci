package grnci

import (
	"io"
	"io/ioutil"
	"log"
	"strings"
	"testing"
)

func TestHTTPClient(t *testing.T) {
	type Pair struct {
		Command string
		Body    string
	}
	pairs := []Pair{
		Pair{"no_such_command", ""},
		Pair{"status", ""},
		Pair{`table_create Tbl TABLE_PAT_KEY ShortText`, ""},
		Pair{`column_create Tbl Col COLUMN_SCALAR Int32`, ""},
		Pair{`load --table Tbl --values '[["_key"],["test"]]'`, ""},
		Pair{`load --table Tbl --values '[["_key"],["test" invalid_format]]'`, ""},
		Pair{"load --table Tbl", `[["_key"],["test"]]`},
		Pair{"load --table Tbl", `[["_key"],["test" invalid_format]]`},
		Pair{"select --table Tbl", ""},
		Pair{"dump", ""},
	}

	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	defer client.Close()

	for _, pair := range pairs {
		var body io.Reader
		if pair.Body != "" {
			body = strings.NewReader(pair.Body)
		}
		req, err := ParseRequest(pair.Command, body)
		if err != nil {
			t.Fatalf("ParseRequest failed: %v", err)
		}
		log.Printf("command = %s", pair.Command)
		resp, err := client.Query(req)
		if err != nil {
			t.Fatalf("conn.Exec failed: %v", err)
		}
		result, err := ioutil.ReadAll(resp)
		if err != nil {
			t.Fatalf("ioutil.ReadAll failed: %v", err)
		}
		log.Printf("status = %d, err = %v", resp.Status(), resp.Err())
		log.Printf("start = %v, elapsed = %v", resp.Start(), resp.Elapsed())
		log.Printf("result = %s", result)
		if err := resp.Close(); err != nil {
			t.Fatalf("resp.Close failed: %v", err)
		}
	}
}
