package libgrn

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/groonga/grnci/v2"
)

type gqtpServer struct {
	dir    string
	path   string
	cmd    *exec.Cmd
	cancel context.CancelFunc
}

// newGQTPServer creates a new DB and starts a server.
func newGQTPServer(tb testing.TB) *gqtpServer {
	dir, err := ioutil.TempDir("", "grnci")
	if err != nil {
		tb.Fatalf("ioutil.TempDir failed: %v", err)
	}

	path := filepath.Join(dir, "db")
	cmd := exec.Command("groonga", "-n", path)
	stdin, _ := cmd.StdinPipe()
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		tb.Skipf("cmd.Start failed: %v", err)
	}
	stdin.Close()
	cmd.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	cmd = exec.CommandContext(ctx, "groonga", "-s", "--protocol", "gqtp", path)
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		tb.Skipf("cmd.Start failed: %v", err)
	}
	time.Sleep(time.Millisecond * 10)

	return &gqtpServer{
		dir:    dir,
		cmd:    cmd,
		cancel: cancel,
	}
}

// Close finishes the server and removes the DB.
func (s *gqtpServer) Close() {
	s.cancel()
	s.cmd.Wait()
	os.RemoveAll(s.dir)
}

func TestGQTPClient(t *testing.T) {
	server := newGQTPServer(t)
	defer server.Close()

	client, err := Dial("", nil)
	if err != nil {
		t.Skipf("Dial failed: %v", err)
	}
	defer client.Close()

	type Test struct {
		Command string
		Body    string
		Error   bool
		Success bool
	}
	tests := []Test{
		// Error: false, Success: true
		Test{"status", "", false, true},
		Test{"table_create Tbl TABLE_PAT_KEY ShortText", "", false, true},
		Test{"column_create Tbl Col COLUMN_SCALAR Int32", "", false, true},
		Test{`load --table Tbl --values '[["_key"],["test"]]'`, "", false, true},
		Test{"load --table Tbl", `[["_key"],["test"]]`, false, true},
		Test{"select --table Tbl", "", false, true},
		Test{"dump", "", false, true},
		// Error: true, Success: *
		Test{"no_such_command", "", true, false},
		Test{"status", "body is not acceptable", true, false},
		// Error: false, Success: false
		Test{"table_create Tbl2", "", false, false},
		Test{`load --table Tbl --values '[["_key"],["test" invalid_format]]'`, "", false, false},
		Test{"load --table Tbl", `[["_key"],["test" invalid_format]]`, false, false},
	}

	for _, test := range tests {
		var body io.Reader
		if test.Body != "" {
			body = strings.NewReader(test.Body)
		}
		resp, err := client.Exec(test.Command, body)
		if test.Error {
			if err != nil {
				continue
			}
			t.Fatalf("client.Exec wrongly succeeded: cmd = %s", test.Command)
		} else {
			if err != nil {
				t.Fatalf("conn.Exec failed: cmd = %s, err = %v", test.Command, err)
			}
		}
		respBody, err := ioutil.ReadAll(resp)
		if err != nil {
			t.Fatalf("ioutil.ReadAll failed: cmd = %s, err = %v", test.Command, err)
		}
		if test.Success {
			if err := resp.Err(); err != nil {
				t.Fatalf("client.Exec failed: cmd = %s, err = %v", test.Command, err)
			}
			if len(respBody) == 0 {
				t.Fatalf("ioutil.ReadAll failed: cmd = %s, len(respBody) = 0", test.Command)
			}
		} else {
			if err := resp.Err(); err == nil {
				t.Fatalf("client.Exec wrongly succeeded: cmd = %s", test.Command)
			}
		}
		if err := resp.Close(); err != nil {
			t.Fatalf("resp.Close failed: %v", err)
		}
	}
}

func TestDBClient(t *testing.T) {
	dir, err := ioutil.TempDir("", "grnci")
	if err != nil {
		t.Fatalf("ioutil.TempDir failed: %v", err)
	}
	defer os.RemoveAll(dir)

	client, err := Create(filepath.Join(dir, "db"), nil)
	if err != nil {
		t.Skipf("Dial failed: %v", err)
	}
	defer client.Close()

	type Test struct {
		Command string
		Body    string
		Error   bool
		Success bool
	}
	tests := []Test{
		// Error: false, Success: true
		Test{"status", "", false, true},
		Test{"table_create Tbl TABLE_PAT_KEY ShortText", "", false, true},
		Test{"column_create Tbl Col COLUMN_SCALAR Int32", "", false, true},
		Test{`load --table Tbl --values '[["_key"],["test"]]'`, "", false, true},
		Test{"load --table Tbl", `[["_key"],["test"]]`, false, true},
		Test{"select --table Tbl", "", false, true},
		Test{"dump", "", false, true},
		// Error: true, Success: *
		Test{"no_such_command", "", true, false},
		Test{"status", "body is not acceptable", true, false},
		// Error: false, Success: false
		Test{"table_create Tbl2", "", false, false},
		Test{`load --table Tbl --values '[["_key"],["test" invalid_format]]'`, "", false, false},
		Test{"load --table Tbl", `[["_key"],["test" invalid_format]]`, false, false},
	}

	for _, test := range tests {
		var body io.Reader
		if test.Body != "" {
			body = strings.NewReader(test.Body)
		}
		resp, err := client.Exec(test.Command, body)
		if test.Error {
			if err != nil {
				continue
			}
			t.Fatalf("client.Exec wrongly succeeded: cmd = %s", test.Command)
		} else {
			if err != nil {
				t.Fatalf("conn.Exec failed: cmd = %s, err = %v", test.Command, err)
			}
		}
		respBody, err := ioutil.ReadAll(resp)
		if err != nil {
			t.Fatalf("ioutil.ReadAll failed: cmd = %s, err = %v", test.Command, err)
		}
		if test.Success {
			if err := resp.Err(); err != nil {
				t.Fatalf("client.Exec failed: cmd = %s, err = %v", test.Command, err)
			}
			if len(respBody) == 0 {
				t.Fatalf("ioutil.ReadAll failed: cmd = %s, len(respBody) = 0", test.Command)
			}
		} else {
			if err := resp.Err(); err == nil {
				t.Fatalf("client.Exec wrongly succeeded: cmd = %s", test.Command)
			}
		}
		if err := resp.Close(); err != nil {
			t.Fatalf("resp.Close failed: %v", err)
		}
	}
}

func BenchmarkGQTPClient(b *testing.B) {
	server := newGQTPServer(b)
	defer server.Close()

	client, err := Dial("", nil)
	if err != nil {
		b.Skipf("Dial failed: %v", err)
	}
	defer client.Close()

	for i := 0; i < b.N; i++ {
		resp, err := client.Exec("status", nil)
		if err != nil {
			b.Fatalf("conn.Exec failed: err = %v", err)
		}
		respBody, err := ioutil.ReadAll(resp)
		if err != nil {
			b.Fatalf("ioutil.ReadAll failed: err = %v", err)
		}
		if err := resp.Err(); err != nil {
			b.Fatalf("client.Exec failed: err = %v", err)
		}
		if len(respBody) == 0 {
			b.Fatalf("ioutil.ReadAll failed: len(respBody) = 0")
		}
		if err := resp.Close(); err != nil {
			b.Fatalf("resp.Close failed: %v", err)
		}
	}
}

func BenchmarkDBClient(b *testing.B) {
	dir, err := ioutil.TempDir("", "grnci")
	if err != nil {
		b.Fatalf("ioutil.TempDir failed: %v", err)
	}
	defer os.RemoveAll(dir)

	client, err := Create(filepath.Join(dir, "db"), nil)
	if err != nil {
		b.Skipf("Dial failed: %v", err)
	}
	defer client.Close()
	for i := 0; i < b.N; i++ {
		resp, err := client.Exec("status", nil)
		if err != nil {
			b.Fatalf("conn.Exec failed: err = %v", err)
		}
		respBody, err := ioutil.ReadAll(resp)
		if err != nil {
			b.Fatalf("ioutil.ReadAll failed: err = %v", err)
		}
		if err := resp.Err(); err != nil {
			b.Fatalf("client.Exec failed: err = %v", err)
		}
		if len(respBody) == 0 {
			b.Fatalf("ioutil.ReadAll failed: len(respBody) = 0")
		}
		if err := resp.Close(); err != nil {
			b.Fatalf("resp.Close failed: %v", err)
		}
	}
}

func TestClientHandler(t *testing.T) {
	var i interface{} = &Client{}
	if _, ok := i.(grnci.Handler); !ok {
		t.Fatalf("Failed to cast from *Client to grnci.Handler")
	}
}
