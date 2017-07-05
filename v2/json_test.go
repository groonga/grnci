package grnci

import (
	"math"
	"testing"
	"time"
)

func TestJSONAppendBool(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendBool(buf, true)
	if want += "true"; string(buf) != want {
		t.Fatalf("jsonAppendBool failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendBool(buf, false)
	if want += "false"; string(buf) != want {
		t.Fatalf("jsonAppendBool failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendInt(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendInt(buf, 0)
	if want += "0"; string(buf) != want {
		t.Fatalf("jsonAppendInt failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendInt(buf, 9223372036854775807)
	if want += "9223372036854775807"; string(buf) != want {
		t.Fatalf("jsonAppendInt failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendInt(buf, -9223372036854775808)
	if want += "-9223372036854775808"; string(buf) != want {
		t.Fatalf("jsonAppendInt failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendUint(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendUint(buf, 0)
	if want += "0"; string(buf) != want {
		t.Fatalf("jsonAppendUint failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendUint(buf, 18446744073709551615)
	if want += "18446744073709551615"; string(buf) != want {
		t.Fatalf("jsonAppendUint failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendFloat(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendFloat(buf, 0.0, 64)
	if want += "0"; string(buf) != want {
		t.Fatalf("jsonAppendFloat failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendFloat(buf, 1.25, 64)
	if want += "1.25"; string(buf) != want {
		t.Fatalf("jsonAppendFloat failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendFloat(buf, -1.25, 64)
	if want += "-1.25"; string(buf) != want {
		t.Fatalf("jsonAppendFloat failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendFloat(buf, math.Pow(2, -16), 64)
	if want += "1.52587890625e-05"; string(buf) != want {
		t.Fatalf("jsonAppendFloat failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendFloat32(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendFloat(buf, 1.234567890123456789, 32)
	if want += "1.2345679"; string(buf) != want {
		t.Fatalf("jsonAppendFloat failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendFloat64(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendFloat(buf, 1.234567890123456789, 64)
	if want += "1.2345678901234567"; string(buf) != want {
		t.Fatalf("jsonAppendFloat failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendString(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendString(buf, "Hello")
	if want += "\"Hello\""; string(buf) != want {
		t.Fatalf("jsonAppendString failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendString(buf, "World")
	if want += "\"World\""; string(buf) != want {
		t.Fatalf("jsonAppendString failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendString(buf, " \t\n\"")
	if want += "\" \\t\\n\\\"\""; string(buf) != want {
		t.Fatalf("jsonAppendString failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendTime(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendTime(buf, time.Unix(1234567890, 0))
	if want += "1234567890"; string(buf) != want {
		t.Fatalf("jsonAppendTime failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendTime(buf, time.Unix(1123456789, 987123654))
	if want += "1123456789.987123"; string(buf) != want {
		t.Fatalf("jsonAppendTime failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendGeo(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppendGeo(buf, Geo{Lat: 123456, Long: 234567})
	if want += "\"123456,234567\""; string(buf) != want {
		t.Fatalf("jsonAppendGeo failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppendGeo(buf, Geo{Lat: -123456, Long: -234567})
	if want += "\"-123456,-234567\""; string(buf) != want {
		t.Fatalf("jsonAppendTime failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendScalar(t *testing.T) {
	var buf []byte
	var want string
	buf = jsonAppend(buf, true)
	if want += "true"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, int8(-128))
	if want += "-128"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, int16(-32768))
	if want += "-32768"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, int32(-2147483648))
	if want += "-2147483648"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, int64(-9223372036854775808))
	if want += "-9223372036854775808"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, int(-9223372036854775808))
	if want += "-9223372036854775808"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, uint8(255))
	if want += "255"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, uint16(65535))
	if want += "65535"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, uint32(4294967295))
	if want += "4294967295"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, uint64(18446744073709551615))
	if want += "18446744073709551615"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, uint(18446744073709551615))
	if want += "18446744073709551615"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, float32(1.234567890123456789))
	if want += "1.2345679"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, float64(1.234567890123456789))
	if want += "1.2345678901234567"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, "String")
	if want += "\"String\""; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, time.Unix(1234567890, 123456789))
	if want += "1234567890.123456"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
	buf = jsonAppend(buf, Geo{Lat: 123456, Long: 234567})
	if want += "\"123456,234567\""; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendPtr(t *testing.T) {
	var buf []byte
	var want string
	v := 123456
	buf = jsonAppend(buf, &v)
	if want += "123456"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendArray(t *testing.T) {
	var buf []byte
	var want string
	v := [3]int{123, 456, 789}
	buf = jsonAppend(buf, v)
	if want += "[123,456,789]"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONAppendSlice(t *testing.T) {
	var buf []byte
	var want string
	v := []int{987, 654, 321}
	buf = jsonAppend(buf, v)
	if want += "[987,654,321]"; string(buf) != want {
		t.Fatalf("jsonAppend failed: actual = %s, want = %s", buf, want)
	}
}

func TestJSONEncodeBool(t *testing.T) {
	if want, actual := "true", jsonEncodeBool(true); actual != want {
		t.Fatalf("jsonEncodeBool failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "false", jsonEncodeBool(false); actual != want {
		t.Fatalf("jsonEncodeBool failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeInt(t *testing.T) {
	if want, actual := "0", jsonEncodeInt(0); actual != want {
		t.Fatalf("jsonEncodeInt failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "9223372036854775807", jsonEncodeInt(9223372036854775807); actual != want {
		t.Fatalf("jsonEncodeInt failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-9223372036854775808", jsonEncodeInt(-9223372036854775808); actual != want {
		t.Fatalf("jsonEncodeInt failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeUint(t *testing.T) {
	if want, actual := "0", jsonEncodeUint(0); actual != want {
		t.Fatalf("jsonEncodeUint failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "18446744073709551615", jsonEncodeUint(18446744073709551615); actual != want {
		t.Fatalf("jsonEncodeUint failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeFloat(t *testing.T) {
	if want, actual := "0", jsonEncodeFloat(0.0, 64); actual != want {
		t.Fatalf("jsonEncodeFloat failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.25", jsonEncodeFloat(1.25, 64); actual != want {
		t.Fatalf("jsonEncodeFloat failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-1.25", jsonEncodeFloat(-1.25, 64); actual != want {
		t.Fatalf("jsonEncodeFloat failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.52587890625e-05", jsonEncodeFloat(math.Pow(2, -16), 64); actual != want {
		t.Fatalf("jsonEncodeFloat failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeFloat32(t *testing.T) {
	if want, actual := "1.2345679", jsonEncodeFloat(1.234567890123456789, 32); actual != want {
		t.Fatalf("jsonEncodeFloat failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeFloat64(t *testing.T) {
	if want, actual := "1.2345678901234567", jsonEncodeFloat(1.234567890123456789, 64); actual != want {
		t.Fatalf("jsonEncodeFloat failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeString(t *testing.T) {
	if want, actual := "\"Hello\"", jsonEncodeString("Hello"); actual != want {
		t.Fatalf("jsonEncodeString failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"World\"", jsonEncodeString("World"); actual != want {
		t.Fatalf("jsonEncodeString failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\" \\t\\n\\\"\"", jsonEncodeString(" \t\n\""); actual != want {
		t.Fatalf("jsonEncodeString failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeTime(t *testing.T) {
	if want, actual := "1234567890", jsonEncodeTime(time.Unix(1234567890, 0)); actual != want {
		t.Fatalf("jsonEncodeTime failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1123456789.987123", jsonEncodeTime(time.Unix(1123456789, 987123654)); actual != want {
		t.Fatalf("jsonEncodeTime failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeGeo(t *testing.T) {
	if want, actual := "\"123456,234567\"", jsonEncodeGeo(Geo{Lat: 123456, Long: 234567}); actual != want {
		t.Fatalf("jsonEncodeGeo failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"-123456,-234567\"", jsonEncodeGeo(Geo{Lat: -123456, Long: -234567}); actual != want {
		t.Fatalf("jsonEncodeGeo failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeScalar(t *testing.T) {
	if want, actual := "true", jsonEncode(true); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-128", jsonEncode(int8(-128)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-32768", jsonEncode(int16(-32768)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-2147483648", jsonEncode(int32(-2147483648)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-9223372036854775808", jsonEncode(int64(-9223372036854775808)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "-9223372036854775808", jsonEncode(int(-9223372036854775808)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "255", jsonEncode(uint8(255)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "65535", jsonEncode(uint16(65535)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "4294967295", jsonEncode(uint32(4294967295)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "18446744073709551615", jsonEncode(uint64(18446744073709551615)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "18446744073709551615", jsonEncode(uint(18446744073709551615)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.2345679", jsonEncode(float32(1.234567890123456789)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1.2345678901234567", jsonEncode(1.234567890123456789); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"String\"", jsonEncode("String"); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "1234567890.123456", jsonEncodeTime(time.Unix(1234567890, 123456789)); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
	if want, actual := "\"123456,234567\"", jsonEncode(Geo{Lat: 123456, Long: 234567}); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodePtr(t *testing.T) {
	v := 123456
	if want, actual := "123456", jsonEncode(&v); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeArray(t *testing.T) {
	v := [3]int{123, 456, 789}
	if want, actual := "[123,456,789]", jsonEncode(v); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
}

func TestJSONEncodeSlice(t *testing.T) {
	v := []int{987, 654, 321}
	if want, actual := "[987,654,321]", jsonEncode(v); actual != want {
		t.Fatalf("jsonEncode failed: actual = %s, want = %s", actual, want)
	}
}
