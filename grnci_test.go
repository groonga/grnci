package grnci

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"testing"
)

//
// Utility
//

// createTempDB creates a database for tests.
// The database must be removed with removeTempDB.
func createTempDB(tb testing.TB) (string, string, *DB) {
	dirPath, err := ioutil.TempDir("", "grnci_test")
	if err != nil {
		tb.Fatalf("ioutil.TempDir failed: %v", err)
	}
	dbPath := dirPath + "/db"
	db, err := Create(dbPath)
	if err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("Create failed: %v", err)
	}
	return dirPath, dbPath, db
}

// removeTempDB removes a database created with createTempDB.
func removeTempDB(tb testing.TB, dirPath string, db *DB) {
	if err := db.Close(); err != nil {
		os.RemoveAll(dirPath)
		tb.Fatalf("DB.Close failed: %v", err)
	}
	if err := os.RemoveAll(dirPath); err != nil {
		tb.Fatalf("os.RemoveAll failed: %v", err)
	}
}

//
// Tests
//

// TestGrnInit tests GrnInit and GrnFin
func TestGrnInit(t *testing.T) {
	if err := GrnFin(); err == nil {
		t.Fatalf("GrnFin succeeded")
	}
	if err := GrnInit(); err != nil {
		t.Fatalf("GrnInit failed: %v", err)
	}
	if err := GrnInit(); err == nil {
		t.Fatalf("GrnInit succeeded")
	}
	if err := GrnFin(); err != nil {
		t.Fatalf("GrnFin failed: %v", err)
	}
	if err := GrnFin(); err == nil {
		t.Fatalf("GrnFin succeeded")
	}
}

// TestCreate tests Create.
func TestCreate(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	if db.Mode() != LocalDB {
		t.Fatalf("DB.Mode failed")
	}
	if len(db.Path()) == 0 {
		t.Fatalf("DB.Path failed")
	}
	if len(db.Host()) != 0 {
		t.Fatalf("DB.Host failed")
	}
	if db.Port() != 0 {
		t.Fatalf("DB.Port failed")
	}
}

// TestCreate tests Open.
func TestOpen(t *testing.T) {
	dirPath, dbPath, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	db2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer db2.Close()

	if db2.Mode() != LocalDB {
		t.Fatalf("DB.Mode failed")
	}
	if len(db2.Path()) == 0 {
		t.Fatalf("DB.Path failed")
	}
	if len(db2.Host()) != 0 {
		t.Fatalf("DB.Host failed")
	}
	if db2.Port() != 0 {
		t.Fatalf("DB.Port failed")
	}
}

// TestDup tests DB.Dup.
func TestDup(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	db2, err := db.Dup()
	if err != nil {
		t.Fatalf("DB.Dup failed: %v", err)
	}
	defer db2.Close()

	if db2.Mode() != LocalDB {
		t.Fatalf("DB.Mode failed")
	}
	if len(db2.Path()) == 0 {
		t.Fatalf("DB.Path failed")
	}
	if len(db2.Host()) != 0 {
		t.Fatalf("DB.Host failed")
	}
	if db2.Port() != 0 {
		t.Fatalf("DB.Port failed")
	}
}

// TestTableCreate tests DB.TableCreate.
func TestTableCreate(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	if err := db.TableCreate("a", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
	if err := db.TableCreate("a", nil); err == nil {
		t.Fatalf("DB.TableCreate() succeeded")
	}

	options := NewTableCreateOptions()
	options.KeyType = "ShortText"
	options.Flags = "TABLE_PAT_KEY"
	if err := db.TableCreate("b", options); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}

	options = NewTableCreateOptions()
	options.ValueType = "Int32"
	if err := db.TableCreate("c", options); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}

	options = NewTableCreateOptions()
	options.KeyType = "ShortText"
	options.Flags = "TABLE_PAT_KEY"
	options.Normalizer = "TokenBigram"
	options.DefaultTokenizer = "NormalizerAuto"
	if err := db.TableCreate("tbl2", options); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
}

// TestColumnCreate tests DB.ColumnCreate.
func TestColumnCreate(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
	tblOptions := NewTableCreateOptions()
	tblOptions.KeyType = "ShortText"
	tblOptions.Normalizer = "TokenBigram"
	tblOptions.DefaultTokenizer = "NormalizerAuto"
	if err := db.TableCreate("tbl2", tblOptions); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}

	if err := db.ColumnCreate("tbl", "a", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "b", "[]Int32", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}

	colOptions := NewColumnCreateOptions()
	colOptions.Flags = "WITH_SECTION|WITH_POSITION"
	if err := db.ColumnCreate("tbl2", "a", "tbl.a", colOptions); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl2", "b", "tbl.b", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
}

// TestLoad tests DB.Load.
func TestLoad(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	options := NewTableCreateOptions()
	options.KeyType = "ShortText"
	if err := db.TableCreate("tbl", options); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "bool", "Bool", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "int", "Int32", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "float", "Float", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "time", "Time", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "text", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "geo", "WGS84GeoPoint", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vbool", "[]Bool", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vint", "[]Int32", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vfloat", "[]Float", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vtime", "[]Time", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vtext", "[]Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "vgeo", "[]WGS84GeoPoint", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}

	type tblRec struct {
		Key    Text    `grnci:"_key"`
		Bool   Bool    `grnci:"bool"`
		Int    Int     `grnci:"int"`
		Float  Float   `grnci:"float"`
		Time   Time    `grnci:"time"`
		Text   Text    `grnci:"text"`
		Geo    Geo     `grnci:"geo"`
		VBool  []Bool  `grnci:"vbool"`
		VInt   []Int   `grnci:"vint"`
		VFloat []Float `grnci:"vfloat"`
		VTime  []Time  `grnci:"vtime"`
		VText  []Text  `grnci:"vtext"`
		VGeo   []Geo   `grnci:"vgeo"`
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
		t.Fatalf("DB.Load failed: %v", err)
	} else if cnt != 1 {
		t.Fatalf("DB.Load failed: cnt = %d", cnt)
	}

	cnt, err = db.Load("tbl", &recs[0], nil)
	if err != nil {
		t.Fatalf("DB.Load failed: %v", err)
	} else if cnt != 1 {
		t.Fatalf("DB.Load failed: cnt = %d", cnt)
	}

	cnt, err = db.Load("tbl", (*tblRec)(nil), nil)
	if err == nil {
		t.Fatalf("DB.Load() succeeded")
	}

	cnt, err = db.Load("tbl", recs, nil)
	if err != nil {
		t.Fatalf("DB.Load failed: %v", err)
	} else if cnt != 2 {
		t.Fatalf("DB.Load failed: cnt = %d", cnt)
	}
}

// TestLoadEx tests DB.LoadEx.
func TestLoadEx(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	type tblRec struct {
		Key    Text    `grnci:"_key;;TABLE_PAT_KEY"`
		Bool   Bool    `grnci:"bool"`
		Int    Int     `grnci:"int;Int32"`
		Float  Float   `grnci:"float"`
		Time   Time    `grnci:"time"`
		Text   Text    `grnci:"text"`
		Geo    Geo     `grnci:"geo;TokyoGeoPoint"`
		VBool  []Bool  `grnci:"vbool"`
		VInt   []Int   `grnci:"vint"`
		VFloat []Float `grnci:"vfloat"`
		VTime  []Time  `grnci:"vtime"`
		VText  []Text  `grnci:"vtext;[]ShortText"`
		VGeo   []Geo   `grnci:"vgeo"`
	}
	recs := []tblRec{
		{Key: "Apple", Bool: false, Int: 123, Float: 1.23,
			Time: Now(), Text: "Hello, world!", Geo: Geo{123, 456}},
		{Key: "Banana", Bool: true, Int: 456, Float: 4.56,
			Time: Now(), Text: "Foo, var!", Geo: Geo{456, 789},
			VBool: []Bool{false, true}, VInt: []Int{100, 200},
			VFloat: []Float{-1.25, 1.25}, VTime: []Time{Now(), Now() + 1000000},
			VText: []Text{"one", "two"}, VGeo: []Geo{{100, 200}, {300, 400}}}}

	cnt, err := db.LoadEx("tbl", []tblRec(nil), nil)
	if err != nil {
		t.Fatalf("DB.LoadEx failed: %v", err)
	} else if cnt != 0 {
		t.Fatalf("DB.LoadEx failed: cnt = %d", cnt)
	}

	cnt, err = db.LoadEx("tbl2", recs, nil)
	if err != nil {
		t.Fatalf("DB.LoadEx failed: %v", err)
	} else if cnt != 2 {
		t.Fatalf("DB.LoadEx failed: cnt = %d", cnt)
	}

	options := NewLoadOptions()
	options.Columns = "_key,int,time,geo"
	cnt, err = db.LoadEx("tbl3", recs, options)
	if err != nil {
		t.Fatalf("DB.LoadEx failed: %v", err)
	} else if cnt != 2 {
		t.Fatalf("DB.LoadEx failed: cnt = %d", cnt)
	}
}

// TestSelect tests DB.Select.
func TestSelect(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	type tblRec struct {
		Key    Text    `grnci:"_key;;TABLE_PAT_KEY"`
		Bool   Bool    `grnci:"bool"`
		Int    Int     `grnci:"int;Int32"`
		Float  Float   `grnci:"float"`
		Time   Time    `grnci:"time"`
		Text   Text    `grnci:"text"`
		Geo    Geo     `grnci:"geo;TokyoGeoPoint"`
		VBool  []Bool  `grnci:"vbool"`
		VInt   []Int   `grnci:"vint"`
		VFloat []Float `grnci:"vfloat"`
		VTime  []Time  `grnci:"vtime"`
		VText  []Text  `grnci:"vtext;[]ShortText"`
		VGeo   []Geo   `grnci:"vgeo"`
	}
	rec := tblRec{
		Key: "Banana", Bool: true, Int: 456, Float: 4.56,
		Time: Now(), Text: "Foo, var!", Geo: Geo{456, 789},
		VBool: []Bool{false, true}, VInt: []Int{100, 200},
		VFloat: []Float{-1.25, 1.25}, VTime: []Time{Now(), Now() + 1000000},
		VText: []Text{"one", "two"}, VGeo: []Geo{{100, 200}, {300, 400}},
	}
	if _, err := db.LoadEx("tbl", rec, nil); err != nil {
		t.Fatalf("DB.LoadEx failed: %v", err)
	}

	var recs []tblRec
	n, err := db.Select("tbl", &recs, nil)
	if err != nil {
		t.Fatalf("DB.Select failed: %v", err)
	} else if n != 1 {
		t.Fatalf("DB.Select failed: n = %d", n)
	}

	type tblRec2 struct {
		Key   Text  `grnci:"_key;;TABLE_PAT_KEY"`
		Bool  Bool  `grnci:"!bool"`
		Int   Int   `grnci:"int+2;Int32"`
		Float Float `grnci:"float*2.0"`
		Score Float `grnci:"_score"`
	}

	options := NewSelectOptions()
	options.Filter = "?"
	var recs2 []tblRec2
	n, err = db.Select("tbl", &recs2, options)
	if err != nil {
		t.Fatalf("DB.Select failed: %v", err)
	} else if n != 1 {
		t.Fatalf("DB.Select failed: n = %d", n)
	}
}

// TestColumnRemove tests DB.ColumnRemove.
func TestColumnRemove(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "val", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}

	if err := db.ColumnRemove("tbl", "val", nil); err != nil {
		t.Fatalf("DB.ColumnRemove failed: %v", err)
	}
	if err := db.ColumnRemove("tbl", "val", nil); err == nil {
		t.Fatalf("DB.ColumnRemove() succeeded")
	}
}

// TestColumnRename tests DB.ColumnRename.
func TestColumnRename(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "val", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}

	if err := db.ColumnRename("tbl", "val", "val2", nil); err != nil {
		t.Fatalf("DB.ColumnRename failed: %v", err)
	}
	if err := db.ColumnRename("tbl", "val", "val2", nil); err == nil {
		t.Fatalf("DB.ColumnRename() succeeded")
	}
	if err := db.ColumnRename("tbl", "val2", "val3", nil); err != nil {
		t.Fatalf("DB.ColumnRename failed: %v", err)
	}
}

// TestTableRemove tests DB.TableRemove.
func TestTableRemove(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}

	if err := db.TableRemove("tbl", nil); err != nil {
		t.Fatalf("DB.TableRemove failed: %v", err)
	}
	if err := db.TableRemove("tbl", nil); err == nil {
		t.Fatalf("DB.TableRemove() succeeded")
	}
}

// TestTableRename tests DB.TableRename.
func TestTableRename(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}

	if err := db.TableRename("tbl", "tbl2", nil); err != nil {
		t.Fatalf("DB.TableRename failed: %v", err)
	}
	if err := db.TableRename("tbl", "tbl2", nil); err == nil {
		t.Fatalf("DB.TableRename() succeeded")
	}
	if err := db.TableRename("tbl2", "tbl3", nil); err != nil {
		t.Fatalf("DB.TableRename failed: %v", err)
	}
}

// TestObjectExist tests DB.ObjectExist.
func TestObjectExist(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	if err := db.ObjectExist("tbl", nil); err == nil {
		t.Fatalf("DB.ObjectExist() succeeded")
	}
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
	if err := db.ObjectExist("tbl", nil); err != nil {
		t.Fatalf("DB.ObjectExist failed: %v", err)
	}

	if err := db.ObjectExist("tbl.val", nil); err == nil {
		t.Fatalf("DB.ObjectExist() succeeded")
	}
	if err := db.ColumnCreate("tbl", "val", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}
	if err := db.ObjectExist("tbl.val", nil); err != nil {
		t.Fatalf("DB.ObjectExist failed: %v", err)
	}
}

// TestTruncate tests DB.Truncate.
func TestTruncate(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)
	if err := db.TableCreate("tbl", nil); err != nil {
		t.Fatalf("DB.TableCreate failed: %v", err)
	}
	if err := db.ColumnCreate("tbl", "val", "Text", nil); err != nil {
		t.Fatalf("DB.ColumnCreate failed: %v", err)
	}

	if err := db.Truncate("tbl.val", nil); err != nil {
		t.Fatalf("DB.Truncate failed: %v", err)
	}
	if err := db.Truncate("tbl", nil); err != nil {
		t.Fatalf("DB.Truncate failed: %v", err)
	}
}

// TestThreadLimit tests DB.ThreadLimit.
func TestThreadLimit(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	n, err := db.ThreadLimit(nil)
	if err != nil {
		t.Fatalf("DB.ThreadLimit failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("failed: %d", n)
	}
}

// TestDatabaseUnmap tests DB.DatabaseUnmap.
func TestDatabaseUnmap(t *testing.T) {
	dirPath, _, db := createTempDB(t)
	defer removeTempDB(t, dirPath, db)

	if err := db.DatabaseUnmap(nil); err != nil {
		t.Fatalf("DB.DatabaseUnmap failed: %v", err)
	}
}

// TestMarshalJSON tests MarshalJSON.
func TestMarshalJSON(t *testing.T) {
	type tblRec struct {
		Key    Text    `grnci:"_key;;TABLE_PAT_KEY"`
		Bool   Bool    `grnci:"bool"`
		Int    Int     `grnci:"int;Int32"`
		Float  Float   `grnci:"float"`
		Time   Time    `grnci:"time"`
		Text   Text    `grnci:"text"`
		Geo    Geo     `grnci:"geo;TokyoGeoPoint"`
		VBool  []Bool  `grnci:"vbool"`
		VInt   []Int   `grnci:"vint"`
		VFloat []Float `grnci:"vfloat"`
		VTime  []Time  `grnci:"vtime"`
		VText  []Text  `grnci:"vtext;[]ShortText"`
		VGeo   []Geo   `grnci:"vgeo"`
	}
	rec := tblRec{Key: "Banana", Bool: true, Int: 456, Float: 4.56,
		Time: Now(), Text: "Foo, var!", Geo: Geo{456, 789},
		VBool: []Bool{false, true}, VInt: []Int{100, 200},
		VFloat: []Float{-1.25, 1.25}, VTime: []Time{Now(), Now() + 1000000},
		VText: []Text{"one", "two"}, VGeo: []Geo{{100, 200}, {300, 400}}}
	bytes, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}

	var rec2 tblRec
	if err := json.Unmarshal(bytes, &rec2); err != nil {
		t.Fatal(err)
	}
}

// TestGetStructInfo tests GetStructInfo
func TestGetStructInfo(t *testing.T) {
	info := GetStructInfo(nil)
	if err := info.Error(); err == nil {
		t.Fatal("GetStructInfo() succeeded")
	}
	info = GetStructInfo(0)
	if err := info.Error(); err == nil {
		t.Fatal("GetStructInfo() succeeded")
	}

	type tblRec struct {
		Key    Text    `grnci:"_key;;TABLE_PAT_KEY"`
		Bool   Bool    `grnci:"bool"`
		Int    Int     `grnci:"int;Int32"`
		Float  Float   `grnci:"float"`
		Time   Time    `grnci:"time"`
		Text   Text    `grnci:"text"`
		Geo    Geo     `grnci:"geo;TokyoGeoPoint"`
		VBool  []Bool  `grnci:"vbool"`
		VInt   []Int   `grnci:"vint"`
		VFloat []Float `grnci:"vfloat"`
		VTime  []Time  `grnci:"vtime"`
		VText  []Text  `grnci:"vtext;[]ShortText"`
		VGeo   []Geo   `grnci:"vgeo"`
	}
	info = GetStructInfo((*tblRec)(nil))
	if err := info.Error(); err != nil {
		t.Fatalf("GetStructInfo failed: %v", err)
	}
	if info.Type() != reflect.TypeOf(tblRec{}) {
		t.Fatalf("GetStructInfo failed: Type() = %v", info.Type())
	}
	if info.NumField() != 13 {
		t.Fatalf("GetStructInfo failed: NumField() = %d", info.NumField())
	}
	if field := info.FieldByColumnName("_key"); field == nil {
		t.Fatalf("GetStructInfo failed")
	} else if field.ColumnName() != "_key" {
		t.Fatalf("GetStructInfo failed: field = %v", field)
	}
	if field := info.FieldByColumnName("vgeo"); field == nil {
		t.Fatalf("GetStructInfo failed")
	} else if field.TerminalType() != reflect.TypeOf(Geo{}) {
		t.Fatalf("GetStructInfo failed: field = %v", field)
	} else if field.Dimension() != 1 {
		t.Fatalf("GetStructInfo failed: field = %v", field)
	}
}
