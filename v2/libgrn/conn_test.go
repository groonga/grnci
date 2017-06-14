package libgrn

import (
	"io"
	"io/ioutil"
	"log"
	"strings"
	"testing"

	"github.com/groonga/grnci/v2"
)

func TestConnGQTP(t *testing.T) {
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

	conn, err := Dial("")
	if err != nil {
		t.Skipf("Dial failed: %v", err)
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

func TestConnDB(t *testing.T) {
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

	conn, err := Open("/tmp/db/db")
	if err != nil {
		t.Skipf("Open failed: %v", err)
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

func TestConnHandler(t *testing.T) {
	var i interface{} = &Conn{}
	if _, ok := i.(grnci.Handler); !ok {
		t.Fatalf("Failed to cast from *Conn to grnci.Handler")
	}
}
