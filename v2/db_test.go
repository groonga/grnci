package grnci

import (
	"io/ioutil"
	"log"
	"strings"
	"testing"
)

func TestDBColumnRemove(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.ColumnRemove("no_such_table", "no_such_column")
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
