package grnci

import (
	"math"
	"testing"
	"time"
)

func TestAppendJSONBool(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONBool(buf, true)
	if want += "true"; string(buf) != want {
		t.Fatalf("AppendJSONBool failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONBool(buf, false)
	if want += "false"; string(buf) != want {
		t.Fatalf("AppendJSONBool failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONInt(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONInt(buf, 0)
	if want += "0"; string(buf) != want {
		t.Fatalf("AppendJSONInt failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONInt(buf, 9223372036854775807)
	if want += "9223372036854775807"; string(buf) != want {
		t.Fatalf("AppendJSONInt failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONInt(buf, -9223372036854775808)
	if want += "-9223372036854775808"; string(buf) != want {
		t.Fatalf("AppendJSONInt failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONUint(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONUint(buf, 0)
	if want += "0"; string(buf) != want {
		t.Fatalf("AppendJSONUint failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONUint(buf, 18446744073709551615)
	if want += "18446744073709551615"; string(buf) != want {
		t.Fatalf("AppendJSONUint failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONFloat(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONFloat(buf, 0.0, 64)
	if want += "0"; string(buf) != want {
		t.Fatalf("AppendJSONFloat failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONFloat(buf, 1.25, 64)
	if want += "1.25"; string(buf) != want {
		t.Fatalf("AppendJSONFloat failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONFloat(buf, -1.25, 64)
	if want += "-1.25"; string(buf) != want {
		t.Fatalf("AppendJSONFloat failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONFloat(buf, math.Pow(2, -16), 64)
	if want += "1.52587890625e-05"; string(buf) != want {
		t.Fatalf("AppendJSONFloat failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONFloat32(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONFloat(buf, 1.234567890123456789, 32)
	if want += "1.2345679"; string(buf) != want {
		t.Fatalf("AppendJSONFloat failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONFloat64(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONFloat(buf, 1.234567890123456789, 64)
	if want += "1.2345678901234567"; string(buf) != want {
		t.Fatalf("AppendJSONFloat failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONString(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONString(buf, "Hello")
	if want += "\"Hello\""; string(buf) != want {
		t.Fatalf("AppendJSONString failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONString(buf, "World")
	if want += "\"World\""; string(buf) != want {
		t.Fatalf("AppendJSONString failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONString(buf, " \t\n\"")
	if want += "\" \\t\\n\\\"\""; string(buf) != want {
		t.Fatalf("AppendJSONString failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONTime(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONTime(buf, time.Unix(1234567890, 0))
	if want += "1234567890"; string(buf) != want {
		t.Fatalf("AppendJSONTime failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONTime(buf, time.Unix(1123456789, 987123654))
	if want += "1123456789.987123"; string(buf) != want {
		t.Fatalf("AppendJSONTime failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONGeo(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSONGeo(buf, Geo{Lat: 123456, Long: 234567})
	if want += "\"123456,234567\""; string(buf) != want {
		t.Fatalf("AppendJSONGeo failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSONGeo(buf, Geo{Lat: -123456, Long: -234567})
	if want += "\"-123456,-234567\""; string(buf) != want {
		t.Fatalf("AppendJSONTime failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONScalar(t *testing.T) {
	var buf []byte
	var want string
	buf = AppendJSON(buf, true)
	if want += "true"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, int8(-128))
	if want += "-128"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, int16(-32768))
	if want += "-32768"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, int32(-2147483648))
	if want += "-2147483648"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, int64(-9223372036854775808))
	if want += "-9223372036854775808"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, int(-9223372036854775808))
	if want += "-9223372036854775808"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, uint8(255))
	if want += "255"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, uint16(65535))
	if want += "65535"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, uint32(4294967295))
	if want += "4294967295"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, uint64(18446744073709551615))
	if want += "18446744073709551615"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, uint(18446744073709551615))
	if want += "18446744073709551615"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, float32(1.234567890123456789))
	if want += "1.2345679"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, float64(1.234567890123456789))
	if want += "1.2345678901234567"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, "String")
	if want += "\"String\""; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, time.Unix(1234567890, 123456789))
	if want += "1234567890.123456"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
	buf = AppendJSON(buf, Geo{Lat: 123456, Long: 234567})
	if want += "\"123456,234567\""; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONPtr(t *testing.T) {
	var buf []byte
	var want string
	v := 123456
	buf = AppendJSON(buf, &v)
	if want += "123456"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONArray(t *testing.T) {
	var buf []byte
	var want string
	v := [3]int{123, 456, 789}
	buf = AppendJSON(buf, v)
	if want += "[123,456,789]"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
}

func TestAppendJSONSlice(t *testing.T) {
	var buf []byte
	var want string
	v := []int{987, 654, 321}
	buf = AppendJSON(buf, v)
	if want += "[987,654,321]"; string(buf) != want {
		t.Fatalf("AppendJSON failed: actual = %s, want = %s", buf, want)
	}
}

func TestEncodeJSONBool(t *testing.T) {
	if want, actual := "true", EncodeJSONBool(true); actual != want {
		t.Fatalf("EncodeJSONBool failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "false", EncodeJSONBool(false); actual != want {
		t.Fatalf("EncodeJSONBool failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONInt(t *testing.T) {
	if want, actual := "0", EncodeJSONInt(0); actual != want {
		t.Fatalf("EncodeJSONInt failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "9223372036854775807", EncodeJSONInt(9223372036854775807); actual != want {
		t.Fatalf("EncodeJSONInt failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-9223372036854775808", EncodeJSONInt(-9223372036854775808); actual != want {
		t.Fatalf("EncodeJSONInt failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONUint(t *testing.T) {
	if want, actual := "0", EncodeJSONUint(0); actual != want {
		t.Fatalf("EncodeJSONUint failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "18446744073709551615", EncodeJSONUint(18446744073709551615); actual != want {
		t.Fatalf("EncodeJSONUint failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONFloat(t *testing.T) {
	if want, actual := "0", EncodeJSONFloat(0.0, 64); actual != want {
		t.Fatalf("EncodeJSONFloat failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.25", EncodeJSONFloat(1.25, 64); actual != want {
		t.Fatalf("EncodeJSONFloat failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-1.25", EncodeJSONFloat(-1.25, 64); actual != want {
		t.Fatalf("EncodeJSONFloat failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.52587890625e-05", EncodeJSONFloat(math.Pow(2, -16), 64); actual != want {
		t.Fatalf("EncodeJSONFloat failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONFloat32(t *testing.T) {
	if want, actual := "1.2345679", EncodeJSONFloat(1.234567890123456789, 32); actual != want {
		t.Fatalf("EncodeJSONFloat failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONFloat64(t *testing.T) {
	if want, actual := "1.2345678901234567", EncodeJSONFloat(1.234567890123456789, 64); actual != want {
		t.Fatalf("EncodeJSONFloat failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONString(t *testing.T) {
	if want, actual := "\"Hello\"", EncodeJSONString("Hello"); actual != want {
		t.Fatalf("EncodeJSONString failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"World\"", EncodeJSONString("World"); actual != want {
		t.Fatalf("EncodeJSONString failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\" \\t\\n\\\"\"", EncodeJSONString(" \t\n\""); actual != want {
		t.Fatalf("EncodeJSONString failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONTime(t *testing.T) {
	if want, actual := "1234567890", EncodeJSONTime(time.Unix(1234567890, 0)); actual != want {
		t.Fatalf("EncodeJSONTime failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1123456789.987123", EncodeJSONTime(time.Unix(1123456789, 987123654)); actual != want {
		t.Fatalf("EncodeJSONTime failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONGeo(t *testing.T) {
	if want, actual := "\"123456,234567\"", EncodeJSONGeo(Geo{Lat: 123456, Long: 234567}); actual != want {
		t.Fatalf("EncodeJSONGeo failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"-123456,-234567\"", EncodeJSONGeo(Geo{Lat: -123456, Long: -234567}); actual != want {
		t.Fatalf("EncodeJSONGeo failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONScalar(t *testing.T) {
	if want, actual := "true", EncodeJSON(true); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-128", EncodeJSON(int8(-128)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-32768", EncodeJSON(int16(-32768)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-2147483648", EncodeJSON(int32(-2147483648)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-9223372036854775808", EncodeJSON(int64(-9223372036854775808)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-9223372036854775808", EncodeJSON(int(-9223372036854775808)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "255", EncodeJSON(uint8(255)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "65535", EncodeJSON(uint16(65535)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "4294967295", EncodeJSON(uint32(4294967295)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "18446744073709551615", EncodeJSON(uint64(18446744073709551615)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "18446744073709551615", EncodeJSON(uint(18446744073709551615)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.2345679", EncodeJSON(float32(1.234567890123456789)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.2345678901234567", EncodeJSON(1.234567890123456789); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"String\"", EncodeJSON("String"); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1234567890.123456", EncodeJSONTime(time.Unix(1234567890, 123456789)); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"123456,234567\"", EncodeJSON(Geo{Lat: 123456, Long: 234567}); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONPtr(t *testing.T) {
	v := 123456
	if want, actual := "123456", EncodeJSON(&v); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONArray(t *testing.T) {
	v := [3]int{123, 456, 789}
	if want, actual := "[123,456,789]", EncodeJSON(v); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
}

func TestEncodeJSONSlice(t *testing.T) {
	v := []int{987, 654, 321}
	if want, actual := "[987,654,321]", EncodeJSON(v); actual != want {
		t.Fatalf("EncodeJSON failed: actual = %s, want = %s", actual, want)
	}
}
