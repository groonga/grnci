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
	conn, err := Create(filepath.Join(dir, "db"))
	if err != nil {
		os.RemoveAll(dir)
		t.Fatalf("Open failed: %v", err)
	}
	return grnci.NewDB(conn), dir
}

// removeDB removes a temporary DB.
func removeDB(db *grnci.DB, dir string) {
	db.Close()
	os.RemoveAll(dir)
}

func TestDBColumnList(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Users TABLE_PAT_KEY ShortText
column_create Users name COLUMN_SCALAR ShortText`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	result, resp, err := db.ColumnList("Users")
	if err == nil {
		err = resp.Err()
	}
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

	_, resp, err := db.ColumnList("no_such_table")
	if err != nil {
		t.Fatalf("db.ColumnList failed: %v", err)
	}
	if resp.Err() == nil {
		t.Fatalf("db.ColumnList wrongly succeeded")
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

// func TestDBColumnCreate(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.ColumnCreate("Tbl.col", "ShortText", nil)
// 	if err != nil {
// 		t.Fatalf("db.ColumnCreate failed: %v", err)
// 	}
// 	log.Printf("result = %#v", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

func TestDBColumnRemove(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY
column_create Tbl col COLUMN_SCALAR ShortText`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	_, resp, err := db.ColumnRemove("Tbl.col")
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		t.Fatalf("db.ColumnRemove failed: %v", err)
	}
}

func TestDBColumnRemoveInvalidTable(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	_, resp, err := db.ColumnRemove("no_such_table.no_such_column")
	if err != nil {
		t.Fatalf("db.ColumnRemove failed: %v", err)
	}
	if resp.Err() == nil {
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
	_, resp, err := db.ColumnRemove("Tbl.no_such_column")
	if err != nil {
		t.Fatalf("db.ColumnRemove failed: %v", err)
	}
	if resp.Err() == nil {
		t.Fatalf("db.ColumnRemove wrongly succeeded")
	}
}

// func TestDBDump(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.Dump(nil)
// 	if err != nil {
// 		t.Fatalf("db.Dump failed: %v", err)
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

func TestDBNormalize(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, resp, err := db.Normalize("NormalizerAuto", "ｳｫｰﾀｰ Two \t\r\n㍑", nil)
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		t.Fatalf("db.Tokenize failed: %v", err)
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

	_, resp, err := db.Normalize("", "ｳｫｰﾀｰ Two \t\r\n㍑", nil)
	if err != nil {
		t.Fatalf("db.Normalize failed: %v", err)
	}
	if resp.Err() == nil {
		t.Fatalf("db.Normalize wrongly succeeded")
	}
}

func TestDBNormalizeWithFlags(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	flags := []string{"WITH_TYPES", "WITH_CHECKS"}
	result, resp, err := db.Normalize("NormalizerAuto", "ｳｫｰﾀｰ Two \t\r\n㍑", flags)
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		t.Fatalf("db.Tokenize failed: %v", err)
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

	result, resp, err := db.NormalizerList()
	if err == nil {
		err = resp.Err()
	}
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

// func TestDBObjectList(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.ObjectList()
// 	if err != nil {
// 		t.Fatalf("db.ObjectList failed: %v", err)
// 	}
// 	log.Printf("result = %#v", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

func TestDBQuit(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, resp, err := db.Quit()
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		t.Fatalf("db.Quit failed: %v", err)
	}
	if !result {
		t.Fatalf("db.Quit failed: result = %v", result)
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

func TestDBSchema(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	db.PluginRegister("token_filters/stem")
	db.TableCreate("Tbl", nil)
	result, resp, err := db.Schema()
	if err == nil {
		err = resp.Err()
	}
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

	result, resp, err := db.Status()
	if err == nil {
		err = resp.Err()
	}
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

// func TestDBTableList(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.TableList()
// 	if err != nil {
// 		t.Fatalf("db.TableList failed: %v", err)
// 	}
// 	log.Printf("result = %#v", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

// func TestDBTableTokenize(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	options := NewDBTableTokenizeOptions()
// 	options.Mode = "ADD"
// 	result, resp, err := db.TableTokenize("Tbl", "あいうえお", options)
// 	if err != nil {
// 		t.Fatalf("db.TableTokenize failed: %v", err)
// 	}
// 	log.Printf("result = %#v", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

func TestDBTokenize(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, resp, err := db.Tokenize("TokenBigram", "あいうえお", nil)
	if err == nil {
		err = resp.Err()
	}
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

	_, resp, err := db.Tokenize("", "あいうえお", nil)
	if err != nil {
		t.Fatalf("db.Tokenize failed: %v", err)
	}
	if resp.Err() == nil {
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
	result, resp, err := db.Tokenize("TokenBigram", "It works well.", options)
	if err == nil {
		err = resp.Err()
	}
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

// func TestDBTruncate(t *testing.T) {
// 	client, err := NewHTTPClient("", nil)
// 	if err != nil {
// 		t.Skipf("NewHTTPClient failed: %v", err)
// 	}
// 	db := NewDB(client)
// 	defer db.Close()

// 	result, resp, err := db.Truncate("no_such_target")
// 	if err != nil {
// 		t.Fatalf("db.Truncate failed: %v", err)
// 	}
// 	log.Printf("result = %#v", result)
// 	log.Printf("resp = %#v", resp)
// 	if err := resp.Err(); err != nil {
// 		log.Printf("error = %#v", err)
// 	}
// }

func TestDBTruncate(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_HASH_KEY ShortText
load --table Tbl --columns _key --values '[["Key"]]'`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	_, resp, err := db.Truncate("Tbl")
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		t.Fatalf("db.Truncate failed: %v", err)
	}
	obj, resp, err := db.ObjectInspect("Tbl")
	if err == nil {
		err = resp.Err()
	}
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

	_, resp, err := db.Truncate("no_such_target")
	if err != nil {
		t.Fatalf("db.Truncate failed: %v", err)
	}
	if resp.Err() == nil {
		t.Fatalf("db.Truncate wrongly succeeded")
	}
}

func TestDBTableRemove(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	dump := `table_create Tbl TABLE_NO_KEY`
	if _, err := db.Restore(strings.NewReader(dump), nil, true); err != nil {
		t.Fatalf("db.Restore failed: %v", err)
	}
	_, resp, err := db.TableRemove("Tbl", false)
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		t.Fatalf("db.TableRemove failed: %v", err)
	}
}

func TestDBTableRemoveInvalidName(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	_, resp, err := db.TableRemove("no_such_table", false)
	if err != nil {
		t.Fatalf("db.TableRemove failed: %v", err)
	}
	if resp.Err() == nil {
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
	_, resp, err := db.TableRemove("Referred", true)
	if err == nil {
		err = resp.Err()
	}
	if err != nil {
		t.Fatalf("db.TableRemove failed: %v", err)
	}
}

func TestDBTokenizerList(t *testing.T) {
	db, dir := makeDB(t)
	defer removeDB(db, dir)

	result, resp, err := db.TokenizerList()
	if err == nil {
		err = resp.Err()
	}
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
