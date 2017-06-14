package grnci

import (
	"io"
	"io/ioutil"
	"log"
	"strings"
	"testing"
)

func TestGQTPConn(t *testing.T) {
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

	conn, err := DialGQTP("")
	if err != nil {
		t.Skipf("DialGQTP failed: %v", err)
	}
	defer conn.Close()

	for _, pair := range pairs {
		var body io.Reader
		if pair.Body != "" {
			body = strings.NewReader(pair.Body)
		}
		log.Printf("command = %s", pair.Command)
		resp, err := conn.Exec(pair.Command, body)
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

func TestGQTPClient(t *testing.T) {
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

	client, err := NewGQTPClient("")
	if err != nil {
		t.Skipf("NewGQTPClient failed: %v", err)
	}
	defer client.Close()

	for _, pair := range pairs {
		var body io.Reader
		if pair.Body != "" {
			body = strings.NewReader(pair.Body)
		}
		log.Printf("command = %s", pair.Command)
		resp, err := client.Exec(pair.Command, body)
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

func TestGQTPConnHandler(t *testing.T) {
	var i interface{} = &GQTPConn{}
	if _, ok := i.(Handler); !ok {
		t.Fatalf("Failed to cast from *GQTPConn to Handler")
	}
}

func TestGQTPClientHandler(t *testing.T) {
	var i interface{} = &GQTPClient{}
	if _, ok := i.(Handler); !ok {
		t.Fatalf("Failed to cast from *GQTPClient to Handler")
	}
}
