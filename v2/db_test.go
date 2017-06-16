package grnci

import (
	"io/ioutil"
	"log"
	"strings"
	"testing"
)

func TestDBColumnList(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.ColumnList("Tbl")
	if err != nil {
		t.Fatalf("db.ColumnList failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBColumnCopy(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.ColumnCopy("Tbl.col", "Tbl.col2")
	if err != nil {
		t.Fatalf("db.ColumnCopy failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBColumnCreate(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.ColumnCreate("Tbl.col", "ShortText", nil)
	if err != nil {
		t.Fatalf("db.ColumnCreate failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBColumnRemove(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.ColumnRemove("no_such_table.no_such_column")
	if err != nil {
		t.Fatalf("db.ColumnRemove failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBDump(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	resp, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	result, err := ioutil.ReadAll(resp)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	log.Printf("result = %s", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBLoad(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.Load("Tbl", strings.NewReader("[]"), nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	log.Printf("result = %d", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBNormalizerList(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.NormalizerList()
	if err != nil {
		t.Fatalf("db.NormalizerList failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBSchema(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.Schema()
	if err != nil {
		t.Fatalf("db.Schema failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBSelect(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	resp, err := db.Select("Tbl", nil)
	if err != nil {
		t.Fatalf("db.Select failed: %v", err)
	}
	result, err := ioutil.ReadAll(resp)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	log.Printf("result = %s", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBStatus(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.Status()
	if err != nil {
		t.Fatalf("db.Status failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBTableList(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.TableList()
	if err != nil {
		t.Fatalf("db.TableList failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBTruncate(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.Truncate("no_such_target")
	if err != nil {
		t.Fatalf("db.Truncate failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBTableRemove(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.TableRemove("no_such_table", false)
	if err != nil {
		t.Fatalf("db.TableRemove failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBTokenizerList(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.TokenizerList()
	if err != nil {
		t.Fatalf("db.TokenizerList failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}
