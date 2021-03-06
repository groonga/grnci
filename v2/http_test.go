package grnci

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
)

type httpServer struct {
	dir    string
	path   string
	cmd    *exec.Cmd
	cancel context.CancelFunc
}

// newHTTPServer creates a new DB and starts a server.
func newHTTPServer(tb testing.TB) *httpServer {
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
	cmd = exec.CommandContext(ctx, "groonga", "-s", "--protocol", "http", path)
	if err := cmd.Start(); err != nil {
		os.RemoveAll(dir)
		tb.Skipf("cmd.Start failed: %v", err)
	}
	time.Sleep(time.Millisecond * 50)

	return &httpServer{
		dir:    dir,
		cmd:    cmd,
		cancel: cancel,
	}
}

// Close finishes the server and removes the DB.
func (s *httpServer) Close() {
	s.cancel()
	s.cmd.Wait()
	os.RemoveAll(s.dir)
}

func TestHTTPClient(t *testing.T) {
	server := newHTTPServer(t)
	defer server.Close()
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
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

func BenchmarkHTTPClient(b *testing.B) {
	b.StopTimer()
	server := newHTTPServer(b)
	defer server.Close()
	client, err := NewHTTPClient("", nil)
	if err != nil {
		b.Skipf("NewHTTPClient failed: %v", err)
	}
	defer client.Close()
	b.StartTimer()

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
func TestHTTPClientHandler(t *testing.T) {
	var i interface{} = &HTTPClient{}
	if _, ok := i.(Handler); !ok {
		t.Fatalf("Failed to cast from *HTTPClient to Handler")
	}
}
