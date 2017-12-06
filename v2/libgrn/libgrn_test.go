package libgrn

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLog(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	path := filepath.Join(dir, "groonga.log")
	Log(path, nil)
	if err := db.TableCreate("Tbl", nil); err != nil {
		t.Fatalf("db.Status failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
}

func TestQueryLog(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	path := filepath.Join(dir, "query.log")
	QueryLog(path, nil)
	if _, err := db.Status(); err != nil {
		t.Fatalf("db.Status failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("os.Stat failed: %v", err)
	}
}
