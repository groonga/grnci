package grnci

import (
	"io/ioutil"
	"os"
	"testing"
)

//
// Utility
//

// createTempDB() creates a database for tests.
// The database must be removed with removeTempDB().
func createTempDB(tb testing.TB) (string, string, *DB) {
	dirPath, err := ioutil.TempDir("", "grnci_test")
	if err != nil {
		tb.Fatalf("ioutil.TempDir() failed: %v", err)
	}
	dbPath := dirPath + "/db"
	db, err := Create(dbPath)
	if err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("Create() failed: %v", err)
	}
	return dirPath, dbPath, db
}

// removeTempDB() removes a database created with createTempDB().
func removeTempDB(tb testing.TB, dirPath string, db *DB) {
	if err := db.Close(); err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("DB.Close() failed: %v", err)
	}
	if err := os.RemoveAll(dirPath); err != nil {
		tb.Fatalf("os.RemoveAll() failed: %v", err)
	}
}

//
// Tests
//

// TestCreate() tests Create().
func TestCreate(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
}

// TestCreate() tests Open().
func TestOpen(t *testing.T) {
	dirPath, dbPath, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("DB.Close() failed: %v", err)
	}
}

// TestDup() tests DB.Dup().
func TestDup(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	db2, err := db.Dup()
	if err != nil {
		t.Fatalf("DB.Dup() failed: %v", err)
	}
	if err := db2.Close(); err != nil {
		t.Fatalf("DB.Close() failed: %v", err)
	}
}

// TestTableCreate() tests DB.TableCreate().
func TestTableCreate(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	if err := db.TableCreate("a", nil); err != nil {
		t.Fatalf("DB.TableCreate() failed: %v", err)
	}
	if err := db.TableCreate("a", nil); err == nil {
		t.Fatalf("DB.TableCreate() succeeded")
	}

	options := NewTableCreateOptions()
	options.KeyType = "ShortText"
	options.Flags = "TABLE_PAT_KEY"
	if err := db.TableCreate("b", options); err != nil {
		t.Fatalf("DB.TableCreate() failed: %v", err)
	}

	options = NewTableCreateOptions()
	options.ValueType = "Int32"
	if err := db.TableCreate("c", options); err != nil {
		t.Fatalf("DB.TableCreate() failed: %v", err)
	}

	options = NewTableCreateOptions()
	options.KeyType = "ShortText"
	options.Flags = "TABLE_PAT_KEY"
	options.Normalizer = "TokenBigram"
	options.DefaultTokenizer = "NormalizerAuto"
	if err := db.TableCreate("tbl2", options); err != nil {
		t.Fatalf("DB.TableCreate() failed: %v", err)
	}
}

// TestColumnCreate() tests DB.ColumnCreate().
func TestColumnCreate(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate() failed: %v", err)
	}
	tblOptions := NewTableCreateOptions()
	tblOptions.KeyType = "ShortText"
	tblOptions.Normalizer = "TokenBigram"
	tblOptions.DefaultTokenizer = "NormalizerAuto"
	if err := db.TableCreate("tbl2", tblOptions); err != nil {
		t.Fatalf("DB.TableCreate() failed: %v", err)
	}

	if err := db.ColumnCreate("tbl", "a", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "b", "[]Int32", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}

	colOptions := NewColumnCreateOptions()
	colOptions.Flags = "WITH_SECTION|WITH_POSITION"
	if err := db.ColumnCreate("tbl2", "a", "*tbl.a", colOptions); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl2", "b", "*tbl.b", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
}
