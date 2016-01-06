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

	if !db.IsHandle() {
		t.Fatalf("DB.IsHandle() failed")
	}
	if db.IsConnection() {
		t.Fatalf("DB.IsConnection() failed")
	}
	if len(db.Path()) == 0 {
		t.Fatalf("DB.Path() failed")
	}
	if len(db.Host()) != 0 {
		t.Fatalf("DB.Host() failed")
	}
	if db.Port() != 0 {
		t.Fatalf("DB.Port() failed")
	}
}

// TestCreate() tests Open().
func TestOpen(t *testing.T) {
	dirPath, dbPath, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open() failed: %v", err)
	}
	defer db2.Close()

	if !db2.IsHandle() {
		t.Fatalf("DB.IsHandle() failed")
	}
	if db2.IsConnection() {
		t.Fatalf("DB.IsConnection() failed")
	}
	if len(db2.Path()) == 0 {
		t.Fatalf("DB.Path() failed")
	}
	if len(db2.Host()) != 0 {
		t.Fatalf("DB.Host() failed")
	}
	if db2.Port() != 0 {
		t.Fatalf("DB.Port() failed")
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
	defer db2.Close()

	if !db2.IsHandle() {
		t.Fatalf("DB.IsHandle() failed")
	}
	if db2.IsConnection() {
		t.Fatalf("DB.IsConnection() failed")
	}
	if len(db2.Path()) == 0 {
		t.Fatalf("DB.Path() failed")
	}
	if len(db2.Host()) != 0 {
		t.Fatalf("DB.Host() failed")
	}
	if db2.Port() != 0 {
		t.Fatalf("DB.Port() failed")
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

// TestLoad() tests DB.Load().
func TestLoad(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	options := NewTableCreateOptions()
	options.KeyType = "ShortText"
	if err := db.TableCreate("tbl", options); err != nil {
		t.Fatalf("DB.TableCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "bool", "Bool", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "int", "Int32", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "float", "Float", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "time", "Time", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "text", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "geo", "WGS84GeoPoint", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vbool", "[]Bool", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vint", "[]Int32", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vfloat", "[]Float", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vtime", "[]Time", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vtext", "[]Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vgeo", "[]WGS84GeoPoint", nil); err != nil {
		t.Fatalf("DB.ColumnCreate() failed: %v", err)
	}

	type tblRec struct {
		Key    Text    `groonga:"_key"`
		Bool   Bool    `groonga:"bool"`
		Int    Int     `groonga:"int"`
		Float  Float   `groonga:"float"`
		Time   Time    `groonga:"time"`
		Text   Text    `groonga:"text"`
		Geo    Geo     `groonga:"geo"`
		VBool  []Bool  `groonga:"vbool"`
		VInt   []Int   `groonga:"vint"`
		VFloat []Float `groonga:"vfloat"`
		VTime  []Time  `groonga:"vtime"`
		VText  []Text  `groonga:"vtext"`
		VGeo   []Geo   `groonga:"vgeo"`
	}
	recs := []tblRec{
		{Key: "Apple", Bool: false, Int: 123, Float: 1.23,
			Time: Now(), Text: "Hello, world!", Geo: Geo{123, 456}},
		{Key: "Banana", Bool: true, Int: 456, Float: 4.56,
			Time: Now(), Text: "Foo, var!", Geo: Geo{456, 789},
			VBool: []Bool{false, true}, VInt: []Int{100, 200},
			VFloat: []Float{-1.25, 1.25}, VTime: []Time{Now(), Now() + 1000000},
			VText: []Text{"one", "two"}, VGeo: []Geo{{100, 200}, {300, 400}}}}

	cnt, err := db.Load("tbl", recs[0], nil)
	if err != nil {
		t.Fatalf("DB.Load() failed: %v", err)
	} else if cnt != 1 {
		t.Fatalf("DB.Load() failed: cnt = %d", cnt)
	}

	cnt, err = db.Load("tbl", &recs[0], nil)
	if err != nil {
		t.Fatalf("DB.Load() failed: %v", err)
	} else if cnt != 1 {
		t.Fatalf("DB.Load() failed: cnt = %d", cnt)
	}

	cnt, err = db.Load("tbl", recs, nil)
	if err != nil {
		t.Fatalf("DB.Load() failed: %v", err)
	} else if cnt != 2 {
		t.Fatalf("DB.Load() failed: cnt = %d", cnt)
	}
}
