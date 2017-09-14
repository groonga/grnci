package libgrn

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"reflect"

	"github.com/groonga/grnci/v2"
)

// makeDB creates a temporary DB.
func makeDB(t *testing.T) (db *grnci.DB, dir string) {
	dir, err := ioutil.TempDir("", "libgrn")
	if err != nil {
		t.Fatalf("ioutil.TempDir failed: %v", err)
	}
	client, err := Create(filepath.Join(dir, "db"), nil)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Open failed: %v", err)
	}
	return grnci.NewDB(client), dir
}

// removeDB removes a temporary DB.
func removeDB(db *grnci.DB, dir string) {
	db.Close()
	os.RemoveAll(dir)
}

func TestDBCacheLimit(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	n, err := db.CacheLimit(50)
	if err != nil {
		t.Fatalf("db.CacheLimit failed: %v", err)
	}
	if want := 100; n != want {
		t.Fatalf("db.CacheLimit failed: n = %d, want = %d", n, want)
	}
	n, err = db.CacheLimit(-1)
	if err != nil {
		t.Fatalf("db.CacheLimit failed: %v", err)
	}
	if want := 50; n != want {
		t.Fatalf("db.CacheLimit failed: n = %d, want = %d", n, want)
	}
}

// func TestDBColumnCopy(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.ColumnCopy("Tbl.col", "Tbl.col2")
// 	if err != nil {
// 		t.Fatalf("db.ColumnCopy failed: %v", err)
// 	}
// 	log.Printf("result = %#v", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

func TestDBColumnCreate(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.ColumnCreate("Tbl.col", "Text", nil); err != nil {
		t.Fatalf("db.ColumnCreate failed: %v", err)
	}
	if ok, err := db.ObjectExist("Tbl.col"); !ok {
		t.Fatalf("db.ObjectExist failed: %v", err)
	}
}

func TestDBColumnCreateInvalidTable(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	err := db.ColumnCreate("no_such_table.col", "Text", nil)
	if err == nil {
		t.Fatalf("db.ColumnCreate wrongly succeeded")
	}
}

func TestDBColumnList(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Users TABLE_PAT_KEY ShortText
column_create Users name COLUMN_SCALAR ShortText`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.ColumnList("Users")
	if err != nil {
		t.Fatalf("db.ColumnList failed: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("db.ColumnList failed: result = %#v", result)
	}
}

func TestDBColumnListInvalidTable(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if _, err := db.ColumnList("no_such_table"); err == nil {
		t.Fatalf("db.ColumnList wrongly succeeded")
	}
}

func TestDBColumnRemove(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR ShortText`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.ColumnRemove("Tbl.col"); err != nil {
		t.Fatalf("db.ColumnRemove failed: %v", err)
	}
	if ok, _ := db.ObjectExist("Tbl.col"); ok {
		t.Fatalf("db.ObjectExist wrongly succeeded")
	}
}

func TestDBColumnRemoveInvalidTable(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.ColumnRemove("no_such_table.no_such_column"); err == nil {
		t.Fatalf("db.ColumnRemove wrongly succeeded")
	}
}

func TestDBColumnRemoveInvalidColumn(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.ColumnRemove("Tbl.no_such_column"); err == nil {
		t.Fatalf("db.ColumnRemove wrongly succeeded")
	}
}

func TestDBColumnRename(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR ShortText`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.ColumnRename("Tbl.col", "col2"); err != nil {
		t.Fatalf("db.ColumnRename failed: %v", err)
	}
	if ok, _ := db.ObjectExist("Tbl.col"); ok {
		t.Fatalf("db.ObjectExist wrongly succeeded")
	}
	if ok, err := db.ObjectExist("Tbl.col2"); !ok {
		t.Fatalf("db.ObjectExist failed: %v", err)
	}
}

func TestDBColumnRenameInvalidTable(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.ColumnRename("no_such_table.col", "col2"); err == nil {
		t.Fatalf("db.ColumnRename wrongly succeeded.")
	}
}

func TestDBColumnRenameInvalidName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.ColumnRename("Tbl.no_such_column", "col2"); err == nil {
		t.Fatalf("db.ColumnRename wrongly succeeded.")
	}
}

func TestDBConfigDelete(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.ConfigSet("config_key", "config_value"); err != nil {
		t.Fatalf("db.ConfigSet failed: %v", err)
	}
	if err := db.ConfigDelete("config_key"); err != nil {
		t.Fatalf("db.ConfigDelete failed: %v", err)
	}
	value, err := db.ConfigGet("config_key")
	if err != nil {
		t.Fatalf("db.ConfigGet failed: %v", err)
	}
	if value != "" {
		t.Fatalf("db.ConfigGet wrongly succeeded")
	}
}

func TestDBConfigDeleteInvalidKey(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.ConfigDelete("no_such_key"); err == nil {
		t.Fatalf("db.ConfigDelete wrongly succeeded")
	}
}

func TestDBConfigGetInvalidKey(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	value, err := db.ConfigGet("no_such_key")
	if err != nil {
		t.Fatalf("db.ConfigGet failed: %v", err)
	}
	if value != "" {
		t.Fatalf("db.ConfigGet wrongly succeeded")
	}
}

func TestDBConfigSet(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	want := "config_value"
	if err := db.ConfigSet("config_key", want); err != nil {
		t.Fatalf("db.ConfigSet failed: %v", err)
	}
	value, err := db.ConfigGet("config_key")
	if err != nil {
		t.Fatalf("db.ConfigGet failed: %v", err)
	}
	if value != want {
		t.Fatalf("db.ConfigGet failed: actual = %s, want = %s", value, want)
	}
}

func TestDBConfigSetOverwrite(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	want := "config_value"
	if err := db.ConfigSet("config_key", "pre_config_value"); err != nil {
		t.Fatalf("db.ConfigSet failed: %v", err)
	}
	if err := db.ConfigSet("config_key", want); err != nil {
		t.Fatalf("db.ConfigSet failed: %v", err)
	}
	value, err := db.ConfigGet("config_key")
	if err != nil {
		t.Fatalf("db.ConfigGet failed: %v", err)
	}
	if value != want {
		t.Fatalf("db.ConfigGet failed: actual = %s, want = %s", value, want)
	}
}

func TestDBDatabaseUnmap(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	n, err := db.ThreadLimit(1)
	if err != nil {
		t.Fatalf("db.ThreadLimit failed: %v", err)
	}
	if n == 0 {
		t.Skipf("This client does not support thread_limit")
	}
	if err := db.DatabaseUnmap(); err != nil {
		t.Fatalf("db.DatabaseUnmap failed: %v", err)
	}
}

func TestDBDeleteByFilter(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_PAT_KEY ShortText
	load --table Tbl --values '[["_key"],["foo"]]'`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.DeleteByFilter("Tbl", "_key == \"foo\""); err != nil {
		t.Fatalf("db.DeleteByFilter failed: %v", err)
	}
}

func TestDBDeleteByFilterInvalidFilter(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.DeleteByFilter("Tbl", "no_such_filter"); err == nil {
		t.Fatalf("db.DeleteByFilter wrongly succeeded")
	}
}

func TestDBDeleteByID(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY
load --table Tbl --values '[[],[]]'`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.DeleteByID("Tbl", 1); err != nil {
		t.Fatalf("db.DeleteByID failed: %v", err)
	}
}

func TestDBDeleteByIDInvalidID(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.DeleteByID("Tbl", 1); err == nil {
		t.Fatalf("db.DeleteByID wrongly succeeded")
	}
}

func TestDBDeleteByKey(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_PAT_KEY ShortText
load --table Tbl --values '[["_key"],["foo"]]'`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.DeleteByKey("Tbl", "foo"); err != nil {
		t.Fatalf("db.DeleteByKey failed: %v", err)
	}
}

func TestDBDeleteByKeyInvalidKey(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.DeleteByKey("Tbl", "no_such_key"); err == nil {
		t.Fatalf("db.DeleteByKey wrongly succeeded")
	}
}

func TestDBDump(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := ""; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpPlugins(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `plugin_register functions/math`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := dump; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpPluginsNo(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `plugin_register functions/math`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	options := grnci.NewDBDumpOptions()
	options.DumpPlugins = false
	result, err := db.Dump(options)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := ""; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpSchema(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := dump; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpSchemaNo(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	options := grnci.NewDBDumpOptions()
	options.DumpSchema = false
	result, err := db.Dump(options)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := ""; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpRecords(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_PAT_KEY ShortText

load --table Tbl
[
["_key"],
["Hello"]
]`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := dump; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpRecordsNo(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_PAT_KEY ShortText

load --table Tbl
[
["_key"],
["Hello"]
]`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	options := grnci.NewDBDumpOptions()
	options.DumpRecords = false
	result, err := db.Dump(options)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	want := "table_create Tbl TABLE_PAT_KEY ShortText"
	if string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpIndexes(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Idx TABLE_PAT_KEY ShortText

table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR ShortText

column_create Idx col COLUMN_INDEX Tbl col`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := dump; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpIndexesNo(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Idx TABLE_PAT_KEY ShortText

table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR ShortText

column_create Idx col COLUMN_INDEX Tbl col`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	options := grnci.NewDBDumpOptions()
	options.DumpIndexes = false
	result, err := db.Dump(options)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	want := `table_create Idx TABLE_PAT_KEY ShortText

table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR ShortText`
	if string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpConfigs(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `config_set config_key config_value`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := dump; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpConfigsNo(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `config_set config_key config_value`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	options := grnci.NewDBDumpOptions()
	options.DumpConfigs = false
	result, err := db.Dump(options)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	if want := ""; string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBDumpSortHashTable(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_HASH_KEY ShortText

load --table Tbl
[
["_key"],
["Orange"],
["Banana"],
["Apple"]
]`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	options := grnci.NewDBDumpOptions()
	options.SortHashTable = true
	result, err := db.Dump(options)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	want := `table_create Tbl TABLE_HASH_KEY ShortText

load --table Tbl
[
["_key"],
["Apple"],
["Banana"],
["Orange"]
]`
	if string(body) != want {
		t.Fatalf("db.Dump failed: actual = %s, want = %s", body, want)
	}
}

func TestDBIOFlush(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.IOFlush(nil); err != nil {
		t.Fatalf("db.IOFlush failed: %v", err)
	}
}

func TestDBIOFlushInvalidTargetName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	options := grnci.NewDBIOFlushOptions()
	options.TargetName = "no_such_target"
	if err := db.IOFlush(options); err == nil {
		t.Fatalf("db.IOFlush wrongly succeeded")
	}
}

// func TestDBLoad(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.Load("Tbl", strings.NewReader("[]"), nil)
// 	if err != nil {
// 		t.Fatalf("db.Load failed: %v", err)
// 	}
// 	log.Printf("result = %d", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

// func TestDBLoadRows(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	type Row struct {
// 		Key         string      `grnci:"_key"`
// 		Bool        bool        `grnci:"bool"`
// 		Int         int         `grnci:"int"`
// 		Int8        int8        `grnci:"int8"`
// 		Int16       int16       `grnci:"int16"`
// 		Int32       int32       `grnci:"int32"`
// 		Int64       int64       `grnci:"int64"`
// 		UInt        uint        `grnci:"uint"`
// 		UInt8       uint8       `grnci:"uint8"`
// 		UInt16      uint16      `grnci:"uint16"`
// 		UInt32      uint32      `grnci:"uint32"`
// 		UInt64      uint64      `grnci:"uint64"`
// 		Float       float64     `grnci:"float64"`
// 		String      string      `grnci:"string"`
// 		Time        time.Time   `grnci:"time"`
// 		BoolSlice   []bool      `grnci:"bool_slice"`
// 		IntSlice    []int       `grnci:"int_slice"`
// 		Int8Slice   []int8      `grnci:"int8_slice"`
// 		Int16Slice  []int16     `grnci:"int16_slice"`
// 		Int32Slice  []int32     `grnci:"int32_slice"`
// 		Int64Slice  []int64     `grnci:"int64_slice"`
// 		UIntSlice   []uint      `grnci:"uint_slice"`
// 		UInt8Slice  []uint8     `grnci:"uint8_slice"`
// 		UInt16Slice []uint16    `grnci:"uint16_slice"`
// 		UInt32Slice []uint32    `grnci:"uint32_slice"`
// 		UInt64Slice []uint64    `grnci:"uint64_slice"`
// 		FloatSlice  []float64   `grnci:"float64_slice"`
// 		StringSlice []string    `grnci:"string_slice"`
// 		TimeSlice   []time.Time `grnci:"time_slice"`
// 	}
// 	rows := []Row{
// 		Row{
// 			Key:         "Apple",
// 			Time:        time.Now(),
// 			Float:       1.23,
// 			StringSlice: []string{"iOS", "Safari"},
// 		},
// 		Row{
// 			Key:         "Microsoft",
// 			Time:        time.Now(),
// 			Float:       4.56,
// 			StringSlice: []string{"Windows", "Edge"},
// 		},
// 	}
// 	result, resp, err := db.LoadRows("Tbl", rows, nil)
// 	if err != nil {
// 		t.Fatalf("db.LoadRows failed: %v", err)
// 	}
// 	log.Printf("result = %d", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

func TestDBLockAcquire(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.LockAcquire(""); err != nil {
		t.Fatalf("db.LockAcquire failed: %v", err)
	}
}

func TestDBLockAcquireInvalidTargetName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.LockAcquire("no_such_target"); err == nil {
		t.Fatalf("db.LockAcquire wrongly succeeded")
	}
}

func TestDBLockClear(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.LockClear(""); err != nil {
		t.Fatalf("db.LockClear failed: %v", err)
	}
}

func TestDBLockClearInvalidTargetName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.LockClear("no_such_target"); err == nil {
		t.Fatalf("db.LockClear wrongly succeeded")
	}
}

func TestDBLockRelease(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.LockAcquire(""); err != nil {
		t.Fatalf("db.LockAcquire failed: %v", err)
	}
	if err := db.LockRelease(""); err != nil {
		t.Fatalf("db.LockRelease failed: %v", err)
	}
}

func TestDBLockReleaseInvalidTargetName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.LockRelease("no_such_target"); err == nil {
		t.Fatalf("db.LockRelease wrongly succeeded")
	}
}

func TestDBNormalize(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, err := db.Normalize("NormalizerAuto", "ｳｫｰﾀｰ Two \t\r\n㍑", nil)
	if err != nil {
		t.Fatalf("db.Normalize failed: %v", err)
	}
	if result.Normalized != "ウォーター two リットル" {
		t.Fatalf("Normalized is wrong: result = %#v", result)
	}
	if len(result.Types) != 0 {
		t.Fatalf("Types is wrong: result = %#v", result)
	}
	if len(result.Checks) != 0 {
		t.Fatalf("Checks is wrong: result = %#v", result)
	}
}

func TestDBNormalizeInvalidNormalizer(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if _, err := db.Normalize("", "ｳｫｰﾀｰ Two \t\r\n㍑", nil); err == nil {
		t.Fatalf("db.Normalize wrongly succeeded")
	}
}

func TestDBNormalizeWithFlags(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	flags := []string{"WITH_TYPES", "WITH_CHECKS"}
	result, err := db.Normalize("NormalizerAuto", "ｳｫｰﾀｰ Two \t\r\n㍑", flags)
	if err != nil {
		t.Fatalf("db.Normalize failed: %v", err)
	}
	if result.Normalized != "ウォーター two リットル" {
		t.Fatalf("Normalized is wrong: result = %#v", result)
	}
	types := []string{
		"katakana", "katakana", "katakana", "katakana", "katakana", "others",
		"alpha", "alpha", "alpha", "others|blank", "katakana", "katakana",
		"katakana", "katakana",
	}
	if !reflect.DeepEqual(result.Types, types) {
		t.Fatalf("Types is wrong: result = %#v", result)
	}
	checks := []int{
		3, 0, 0, 3, 0, 0, 3, 0, 0, 3, 0, 0, 3, 0, 0, 1, 1, 1, 1, 1, 6, 0, 0,
		-1, 0, 0, -1, 0, 0, -1, 0, 0,
	}
	if !reflect.DeepEqual(result.Checks, checks) {
		t.Fatalf("Checks is wrong: result = %#v", result)
	}
}

func TestDBNormalizerList(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, err := db.NormalizerList()
	if err != nil {
		t.Fatalf("db.NormalizerList failed: %v", err)
	}
	if len(result) == 0 {
		t.Fatalf("Normalizers not found")
	}
	for i, normalizer := range result {
		if normalizer.Name == "" {
			t.Fatalf("Name is wrong: i = %d, normalizer = %#v", i, normalizer)
		}
	}
}

func TestDBObjectExist(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if ok, err := db.ObjectExist("Bool"); !ok {
		t.Fatalf("db.ObjectExist failed: %v", err)
	}
}

func TestDBObjectExistInvalidName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if ok, _ := db.ObjectExist("no_such_object"); ok {
		t.Fatalf("db.ObjectExist wrongly succeeded")
	}
}

func TestDBObjectList(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.ObjectList()
	if err != nil {
		t.Fatalf("db.ObjectList failed: %v", err)
	}
	if len(result) == 0 || result["Bool"] == nil || result["Tbl"] == nil {
		t.Fatalf("db.ObjectList failed: result = %#v", result)
	}
}

func TestDBObjectRemove(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.ObjectRemove("Tbl", false); err != nil {
		t.Fatalf("db.ObjectRemove failed: %v", err)
	}
	if ok, _ := db.ObjectExist("Tbl"); ok {
		t.Fatalf("db.ObjectExist wrongly succeeded")
	}
}

func TestDBObjectRemoveInvalidName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.ObjectRemove("no_such_object", false); err == nil {
		t.Fatalf("db.ObjectRemove wrongly succeeded")
	}
}

func TestPluginRegister(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.PluginRegister("functions/math"); err != nil {
		t.Fatalf("db.PluginRegister failed: %v", err)
	}
}

func TestPluginRegisterInvalid(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.PluginRegister(""); err == nil {
		t.Fatalf("db.PluginRegister wrongly succeeded")
	}
}

func TestPluginUnregister(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.PluginRegister("functions/math"); err != nil {
		t.Fatalf("db.PluginRegister failed: %v", err)
	}
	if err := db.PluginUnregister("functions/math"); err != nil {
		t.Fatalf("db.PluginUnregister failed: %v", err)
	}
}

func TestPluginUnregisterInvalid(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.PluginUnregister(""); err == nil {
		t.Fatalf("db.PluginUnregister wrongly succeeded")
	}
}

func TestDBQuit(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.Quit(); err != nil {
		t.Fatalf("db.Quit failed: %v", err)
	}
}

func TestRestore(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR ShortText

load --table Tbl
[
["col"],
["Hello, world!"]
]
`
	buf := new(bytes.Buffer)
	n, err := db.Restore(strings.NewReader(dump), buf, true)
	if err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if n != 3 {
		t.Fatalf("N is wrong: n = %d", n)
	}
	actual := buf.String()
	want := `true
true
1
`
	if actual != want {
		t.Fatalf("db.Restore failed: actual = %s, want = %s", actual, want)
	}
}

func TestRubyEval(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.PluginRegister("ruby/eval"); err != nil {
		t.Skipf("db.PluginRegister failed: %v", err)
	}
	result, err := db.RubyEval("1 + 2")
	if err != nil {
		t.Fatalf("db.RubyEval failed: %v", err)
	}
	value := reflect.ValueOf(result)
	if kind, want := value.Kind(), reflect.Float64; kind != want {
		t.Fatalf("db.RubyEval failed: kind = %v, want = %v", kind, want)
	}
	if float, want := value.Float(), 3.0; float != want {
		t.Fatalf("db.RubyEval failed: value = %f, want = %f", float, want)
	}
}

func TestRubyEvalInvalidScript(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.PluginRegister("ruby/eval"); err != nil {
		t.Skipf("db.PluginRegister failed: %v", err)
	}
	if _, err := db.RubyEval(""); err == nil {
		t.Fatalf("db.RubyEval wrongly succeeded")
	}
}

func TestRubyLoad(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	f, err := ioutil.TempFile("", "libgrn")
	if err != nil {
		t.Fatalf("ioutil.TempFile failed: %v", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err := db.PluginRegister("ruby/load"); err != nil {
		t.Skipf("db.PluginRegister failed: %v", err)
	}
	result, err := db.RubyLoad(f.Name())
	if err != nil {
		t.Fatalf("db.RubyLoad failed: %v", err)
	}
	if result != nil {
		t.Fatalf("db.RubyLoad failed: result = %v, want = %v", result, nil)
	}
}

func TestRubyLoadInvalidScript(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.PluginRegister("ruby/load"); err != nil {
		t.Skipf("db.PluginRegister failed: %v", err)
	}
	if _, err := db.RubyLoad(""); err == nil {
		t.Fatalf("db.RubyLoad wrongly succeeded")
	}
}

func TestDBSchema(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	db.PluginRegister("token_filters/stem")
	db.TableCreate("Tbl", nil)
	result, err := db.Schema()
	if err != nil {
		t.Fatalf("db.Schema failed: %v", err)
	}
	if len(result.Plugins) != 1 {
		t.Fatalf("Plugins is wrong: result = %#v", result)
	}
	if _, ok := result.Plugins["token_filters/stem"]; !ok {
		t.Fatalf("Plugins is wrong: result = %#v", result)
	}
	if len(result.Types) == 0 {
		t.Fatalf("Types is wrong: result = %#v", result)
	}
	if len(result.Tokenizers) == 0 {
		t.Fatalf("Tokenizers is wrong: result = %#v", result)
	}
	if len(result.Normalizers) == 0 {
		t.Fatalf("Normalizers is wrong: result = %#v", result)
	}
	if len(result.TokenFilters) == 0 {
		t.Fatalf("TokenFilters is wrong: result = %#v", result)
	}
	if len(result.Tables) != 1 {
		t.Fatalf("Tables is wrong: result = %#v", result)
	}
	if _, ok := result.Tables["Tbl"]; !ok {
		t.Fatalf("Tables is wrong: result = %#v", result)
	}
}

// func TestDBSelect(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.Select("Tbl", nil)
// 	if err != nil {
// 		t.Fatalf("db.Select failed: %v", err)
// 	}
// 	body, err := ioutil.ReadAll(result)
// 	if err != nil {
// 		t.Fatalf("ioutil.ReadAll failed: %v", err)
// 	}
// 	result.Close()
// 	log.Printf("body = %s", body)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

// func TestDBSelectRows(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	type Row struct {
// 		Key       string      `grnci:"_key"`
// 		Bool      bool        `grnci:"bool"`
// 		Int8      int8        `grnci:"int8"`
// 		Int16     int16       `grnci:"int16"`
// 		Int32     int32       `grnci:"int32"`
// 		Int64     int64       `grnci:"int64"`
// 		UInt8     int8        `grnci:"uint8"`
// 		UInt16    int16       `grnci:"uint16"`
// 		UInt32    int32       `grnci:"uint32"`
// 		UInt64    int64       `grnci:"uint64"`
// 		Float     float64     `grnci:"float"`
// 		String    string      `grnci:"string"`
// 		Time      time.Time   `grnci:"time"`
// 		TimeSlice []time.Time `grnci:"time_slice"`
// 	}
// 	var rows []Row
// 	n, resp, err := db.SelectRows("Tbl", &rows, nil)
// 	if err != nil {
// 		t.Fatalf("db.SelectRows failed: %v", err)
// 	}
// 	log.Printf("n = %d", n)
// 	log.Printf("rows = %#v", rows)
// 	if len(rows) != 0 {
// 		log.Printf("time = %s", rows[0].Time)
// 	}
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

func TestDBStatus(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, err := db.Status()
	if err != nil {
		t.Fatalf("db.Status failed: %v", err)
	}
	if result.AllocCount == 0 {
		t.Fatalf("AllocCount is wrong: result = %#v", result)
	}
	if result.CacheHitRate != 0.0 {
		t.Fatalf("CacheHitRate is wrong: result = %#v", result)
	}
	if result.CommandVersion != 1 {
		t.Fatalf("CommandVersion is wrong: result = %#v", result)
	}
	if result.DefaultCommandVersion != 1 {
		t.Fatalf("DefaultCommandVersion is wrong: result = %#v", result)
	}
	if result.MaxCommandVersion != 3 {
		t.Fatalf("MaxCommandVersion is wrong: result = %#v", result)
	}
	if result.NQueries != 0 {
		t.Fatalf("NQueries is wrong: result = %#v", result)
	}
	if result.StartTime.IsZero() {
		t.Fatalf("StartTime is wrong: result = %#v", result)
	}
	if result.Uptime < 0 || result.Uptime > time.Minute {
		t.Fatalf("Uptime is wrong: result = %#v", result)
	}
	if result.Version == "" {
		t.Fatalf("Version is wrong: result = %#v", result)
	}
}

func TestDBTableList(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Users TABLE_PAT_KEY ShortText`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.TableList()
	if err != nil {
		t.Fatalf("db.TableList failed: %v", err)
	}
	if len(result) != 1 || result[0].Name != "Users" {
		t.Fatalf("db.TableList failed: result = %#v", result)
	}
}

func TestDBTableRemove(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.TableRemove("Tbl", false); err != nil {
		t.Fatalf("db.TableRemove failed: %v", err)
	}
	if ok, _ := db.ObjectExist("Tbl"); ok {
		t.Fatalf("db.ObjectExist wrongly succeeded")
	}
}

func TestDBTableRemoveInvalidName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.TableRemove("no_such_table", false); err == nil {
		t.Fatalf("db.TableRemove wrongly succeeded")
	}
}

func TestDBTableRemoveDependent(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Referred TABLE_HASH_KEY ShortText
table_create Referrer TABLE_HASH_KEY Referred`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.TableRemove("Referred", true); err != nil {
		t.Fatalf("db.TableRemove failed: %v", err)
	}
}

func TestDBTableRename(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.TableRename("Tbl", "Tbl2"); err != nil {
		t.Fatalf("db.TableRename failed: %v", err)
	}
	if ok, _ := db.ObjectExist("Tbl"); ok {
		t.Fatalf("db.ObjectExist wrongly succeeded")
	}
	if ok, err := db.ObjectExist("Tbl2"); !ok {
		t.Fatalf("db.ObjectExist failed: %v", err)
	}
}

func TestDBTableRenameInvalidName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.TableRename("no_such_table", "col2"); err == nil {
		t.Fatalf("db.TableRename wrongly succeeded.")
	}
}

func TestDBTableTokenize(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_HASH_KEY ShortText --default_tokenizer TokenBigram`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, err := db.TableTokenize("Tbl", "あいうえお", nil)
	if err != nil {
		t.Fatalf("db.TableTokenize failed: %v", err)
	}
	values := []string{"あい", "いう", "うえ", "えお", "お"}
	for i, token := range result {
		if token.Position != i {
			t.Fatalf("Position is wrong: i = %d, token = %#v", i, token)
		}
		if token.ForcePrefix {
			t.Fatalf("ForcePrefix is wrong: i = %d, token = %#v", i, token)
		}
		if i >= len(values) || token.Value != values[i] {
			t.Fatalf("Value is wrong: i = %d, token = %#v", i, token)
		}
	}
}

func TestDBTableTokenizeInvalidTable(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if _, err := db.TableTokenize("no_such_table", "あいうえお", nil); err == nil {
		t.Fatalf("db.TableTokenize wrongly succeeded")
	}
}

func TestDBTableTokenizeWithOptions(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_HASH_KEY ShortText --default_tokenizer TokenBigram`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	options := grnci.NewDBTableTokenizeOptions()
	options.Flags = []string{"NONE"}
	options.Mode = "ADD"
	result, err := db.TableTokenize("Tbl", "あいうえお", options)
	if err != nil {
		t.Fatalf("db.TableTokenize failed: %v", err)
	}
	values := []string{"あい", "いう", "うえ", "えお", "お"}
	for i, token := range result {
		if token.Position != i {
			t.Fatalf("Position is wrong: i = %d, token = %#v", i, token)
		}
		if token.ForcePrefix {
			t.Fatalf("ForcePrefix is wrong: i = %d, token = %#v", i, token)
		}
		if i >= len(values) || token.Value != values[i] {
			t.Fatalf("Value is wrong: i = %d, token = %#v", i, token)
		}
	}
}

func TestDBThreadLimit(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	n, err := db.ThreadLimit(2)
	if err != nil {
		t.Fatalf("db.ThreadLimit failed: %v", err)
	}
	if n == 0 {
		t.Skipf("This client does not support thread_limit")
	}
	if want := 1; n != want {
		t.Fatalf("db.ThreadLimit failed: n = %d, want = %d", n, want)
	}
	n, err = db.ThreadLimit(-1)
	if err != nil {
		t.Fatalf("db.ThreadLimit failed: %v", err)
	}
	if want := 2; n != want {
		t.Fatalf("db.ThreadLimit failed: n = %d, want = %d", n, want)
	}
}

func TestDBTokenize(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, err := db.Tokenize("TokenBigram", "あいうえお", nil)
	if err != nil {
		t.Fatalf("db.Tokenize failed: %v", err)
	}
	values := []string{"あい", "いう", "うえ", "えお", "お"}
	for i, token := range result {
		if token.Position != i {
			t.Fatalf("Position is wrong: i = %d, token = %#v", i, token)
		}
		if token.ForcePrefix {
			t.Fatalf("ForcePrefix is wrong: i = %d, token = %#v", i, token)
		}
		if i >= len(values) || token.Value != values[i] {
			t.Fatalf("Value is wrong: i = %d, token = %#v", i, token)
		}
	}
}

func TestDBTokenizeInvalidTokenizer(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if _, err := db.Tokenize("", "あいうえお", nil); err == nil {
		t.Fatalf("db.Tokenize wrongly succeeded")
	}
}

func TestDBTokenizeWithOptions(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	db.PluginRegister("token_filters/stem")
	options := grnci.NewDBTokenizeOptions()
	options.Normalizer = "NormalizerAuto"
	options.Flags = []string{"NONE"}
	options.Mode = "ADD"
	options.TokenFilters = []string{"TokenFilterStem"}
	result, err := db.Tokenize("TokenBigram", "It works well.", options)
	if err != nil {
		t.Fatalf("db.Tokenize failed: %v", err)
	}
	values := []string{"it", "work", "well", "."}
	for i, token := range result {
		if token.Position != i {
			t.Fatalf("Position is wrong: i = %d, token = %#v", i, token)
		}
		if token.ForcePrefix {
			t.Fatalf("ForcePrefix is wrong: i = %d, token = %#v", i, token)
		}
		if token.Value != values[i] {
			t.Fatalf("Value is wrong: i = %d, token = %#v", i, token)
		}
	}
}

func TestDBTokenizerList(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, err := db.TokenizerList()
	if err != nil {
		t.Fatalf("db.TokenizerList failed: %v", err)
	}
	if len(result) == 0 {
		t.Fatalf("Tokenizers not found")
	}
	for i, tokenizer := range result {
		if tokenizer.Name == "" {
			t.Fatalf("Name is wrong: i = %d, tokenizer = %#v", i, tokenizer)
		}
	}
}

func TestDBTruncate(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_HASH_KEY ShortText
load --table Tbl --columns _key --values '[["Key"]]'`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	if err := db.Truncate("Tbl"); err != nil {
		t.Fatalf("db.Truncate failed: %v", err)
	}
	obj, err := db.ObjectInspect("Tbl")
	if err != nil {
		t.Fatalf("db.ObjectInspect failed: %v", err)
	}
	tbl, ok := obj.(*grnci.DBObjectTable)
	if !ok {
		t.Fatalf("db.ObjectInspect failed: obj = %#v", obj)
	}
	if tbl.NRecords != 0 {
		t.Fatalf("db.Truncate failed: nRecords = %d", tbl.NRecords)
	}
}

func TestDBTruncateInvalidTarget(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	if err := db.Truncate("no_such_target"); err == nil {
		t.Fatalf("db.Truncate wrongly succeeded")
	}
}
