package grnci

import (
	"log"
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
