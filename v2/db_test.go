package grnci

import (
	"io/ioutil"
	"log"
	"strings"
	"testing"
	"time"
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

	result, resp, err := db.Dump(nil)
	if err != nil {
		t.Fatalf("db.Dump failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	log.Printf("body = %s", body)
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
		t.Fatalf("db.Load failed: %v", err)
	}
	log.Printf("result = %d", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBLoadRows(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	type Row struct {
		Key         string      `grnci:"_key"`
		Bool        bool        `grnci:"bool"`
		Int         int         `grnci:"int"`
		Int8        int8        `grnci:"int8"`
		Int16       int16       `grnci:"int16"`
		Int32       int32       `grnci:"int32"`
		Int64       int64       `grnci:"int64"`
		UInt        uint        `grnci:"uint"`
		UInt8       uint8       `grnci:"uint8"`
		UInt16      uint16      `grnci:"uint16"`
		UInt32      uint32      `grnci:"uint32"`
		UInt64      uint64      `grnci:"uint64"`
		Float       float64     `grnci:"float64"`
		String      string      `grnci:"string"`
		Time        time.Time   `grnci:"time"`
		BoolSlice   []bool      `grnci:"bool_slice"`
		IntSlice    []int       `grnci:"int_slice"`
		Int8Slice   []int8      `grnci:"int8_slice"`
		Int16Slice  []int16     `grnci:"int16_slice"`
		Int32Slice  []int32     `grnci:"int32_slice"`
		Int64Slice  []int64     `grnci:"int64_slice"`
		UIntSlice   []uint      `grnci:"uint_slice"`
		UInt8Slice  []uint8     `grnci:"uint8_slice"`
		UInt16Slice []uint16    `grnci:"uint16_slice"`
		UInt32Slice []uint32    `grnci:"uint32_slice"`
		UInt64Slice []uint64    `grnci:"uint64_slice"`
		FloatSlice  []float64   `grnci:"float64_slice"`
		StringSlice []string    `grnci:"string_slice"`
		TimeSlice   []time.Time `grnci:"time_slice"`
	}
	rows := []Row{
		Row{
			Key:         "Apple",
			Time:        time.Now(),
			Float:       1.23,
			StringSlice: []string{"iOS", "Safari"},
		},
		Row{
			Key:         "Microsoft",
			Time:        time.Now(),
			Float:       4.56,
			StringSlice: []string{"Windows", "Edge"},
		},
	}
	result, resp, err := db.LoadRows("Tbl", rows, nil)
	if err != nil {
		t.Fatalf("db.LoadRows failed: %v", err)
	}
	log.Printf("result = %d", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBNormalize(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.Normalize("NormalizerAuto", "LaTeX", []string{
		"WITH_TYPES", "WITH_CHECKS",
	})
	if err != nil {
		t.Fatalf("db.Normalize failed: %v", err)
	}
	log.Printf("result = %#v", result)
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

func TestDBObjectList(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.ObjectList()
	if err != nil {
		t.Fatalf("db.ObjectList failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBQuit(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.Quit()
	if err != nil {
		t.Fatalf("db.Quit failed: %v", err)
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

	result, resp, err := db.Select("Tbl", nil)
	if err != nil {
		t.Fatalf("db.Select failed: %v", err)
	}
	body, err := ioutil.ReadAll(result)
	if err != nil {
		t.Fatalf("ioutil.ReadAll failed: %v", err)
	}
	result.Close()
	log.Printf("body = %s", body)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBSelectRows(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	type Row struct {
		Key       string      `grnci:"_key"`
		Bool      bool        `grnci:"bool"`
		Int8      int8        `grnci:"int8"`
		Int16     int16       `grnci:"int16"`
		Int32     int32       `grnci:"int32"`
		Int64     int64       `grnci:"int64"`
		UInt8     int8        `grnci:"uint8"`
		UInt16    int16       `grnci:"uint16"`
		UInt32    int32       `grnci:"uint32"`
		UInt64    int64       `grnci:"uint64"`
		Float     float64     `grnci:"float"`
		String    string      `grnci:"string"`
		Time      time.Time   `grnci:"time"`
		TimeSlice []time.Time `grnci:"time_slice"`
	}
	var rows []Row
	n, resp, err := db.SelectRows("Tbl", &rows, nil)
	if err != nil {
		t.Fatalf("db.SelectRows failed: %v", err)
	}
	log.Printf("n = %d", n)
	log.Printf("rows = %#v", rows)
	if len(rows) != 0 {
		log.Printf("time = %s", rows[0].Time)
	}
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

func TestDBTableTokenize(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	options := NewDBTableTokenizeOptions()
	options.Mode = "ADD"
	result, resp, err := db.TableTokenize("Tbl", "あいうえお", options)
	if err != nil {
		t.Fatalf("db.TableTokenize failed: %v", err)
	}
	log.Printf("result = %#v", result)
	log.Printf("resp = %#v", resp)
	if err := resp.Err(); err != nil {
		log.Printf("error = %#v", err)
	}
}

func TestDBTokenize(t *testing.T) {
	client, err := NewHTTPClient("", nil)
	if err != nil {
		t.Skipf("NewHTTPClient failed: %v", err)
	}
	db := NewDB(client)
	defer db.Close()

	result, resp, err := db.Tokenize("TokenBigram", "あいうえお", nil)
	if err != nil {
		t.Fatalf("db.Tokenize failed: %v", err)
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
