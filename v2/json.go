package grnci

import (
	"reflect"
	"strconv"
	"time"
)

// jsonAppendBool appends the JSON-encoded v to buf and returns the extended buffer.
func jsonAppendBool(buf []byte, v bool) []byte {
	return strconv.AppendBool(buf, v)
}

// jsonAppendInt appends the JSON-encoded v to buf and returns the extended buffer.
func jsonAppendInt(buf []byte, v int64) []byte {
	return strconv.AppendInt(buf, v, 10)
}

// jsonAppendUint appends the JSON-encoded v to buf and returns the extended buffer.
func jsonAppendUint(buf []byte, v uint64) []byte {
	return strconv.AppendUint(buf, v, 10)
}

// jsonAppendFloat appands the JSON-encoded v to buf and returns the extended buffer.
func jsonAppendFloat(buf []byte, v float64, bitSize int) []byte {
	return strconv.AppendFloat(buf, v, 'g', -1, bitSize)
}

// jsonAppendString appends the JSON-encoded v to buf and returns the extended buffer.
func jsonAppendString(buf []byte, v string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '\b':
			buf = append(buf, `\b`...)
		case '\t':
			buf = append(buf, `\t`...)
		case '\n':
			buf = append(buf, `\n`...)
		case '\f':
			buf = append(buf, `\f`...)
		case '\r':
			buf = append(buf, `\r`...)
		case '"':
			buf = append(buf, `\"`...)
		case '\\':
			buf = append(buf, `\\`...)
		default:
			buf = append(buf, v[i])
		}
	}
	return append(buf, '"')
}

// jsonAppendTime appends the JSON-encoded v to buf and returns the extended buffer.
func jsonAppendTime(buf []byte, v time.Time) []byte {
	buf = strconv.AppendInt(buf, v.Unix(), 10)
	usec := v.Nanosecond() / 1000
	if usec != 0 {
		buf = append(buf, '.')
		n := len(buf)
		if cap(buf) < n+6 {
			newBuf := make([]byte, n+6, cap(buf)*2)
			copy(newBuf, buf)
			buf = newBuf
		} else {
			buf = buf[:n+6]
		}
		for i := 0; i < 6; i++ {
			buf[n+5-i] = byte('0' + usec%10)
			usec /= 10
		}
	}
	return buf
}

// jsonAppendGeo appends the JSON-encoded v to buf and returns the extended buffer.
func jsonAppendGeo(buf []byte, v Geo) []byte {
	buf = append(buf, '"')
	buf = strconv.AppendInt(buf, int64(v.Lat), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(v.Long), 10)
	return append(buf, '"')
}

// jsonAppendValue appends the JSON-encoded v to buf and returns the extended buffer.
// If the type of v is unsupported, it appends "null".
func jsonAppendValue(buf []byte, v reflect.Value) []byte {
	switch v.Kind() {
	case reflect.Bool:
		return jsonAppendBool(buf, v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return jsonAppendInt(buf, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return jsonAppendUint(buf, v.Uint())
	case reflect.Float32:
		return jsonAppendFloat(buf, v.Float(), 32)
	case reflect.Float64:
		return jsonAppendFloat(buf, v.Float(), 64)
	case reflect.String:
		return jsonAppendString(buf, v.String())
	case reflect.Struct:
		switch v := v.Interface().(type) {
		case time.Time:
			return jsonAppendTime(buf, v)
		case Geo:
			return jsonAppendGeo(buf, v)
		default:
			return append(buf, "null"...)
		}
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return append(buf, "null"...)
		}
		return jsonAppendValue(buf, v.Elem())
	case reflect.Array:
		buf = append(buf, '[')
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, ',')
			}
			buf = jsonAppendValue(buf, v.Index(i))
		}
		return append(buf, ']')
	case reflect.Slice:
		if v.IsNil() {
			return append(buf, "null"...)
		}
		buf = append(buf, '[')
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, ',')
			}
			buf = jsonAppendValue(buf, v.Index(i))
		}
		return append(buf, ']')
	default:
		return append(buf, "null"...)
	}
}

// jsonAppend appends the JSON-encoded v to buf and returns the extended buffer.
// If the type of v is unsupported, it appends "null".
func jsonAppend(buf []byte, v interface{}) []byte {
	return jsonAppendValue(buf, reflect.ValueOf(v))
}

// jsonEncodeBool returns the JSON-encoded v.
func jsonEncodeBool(v bool) string {
	return strconv.FormatBool(v)
}

// jsonEncodeInt returns the JSON-encoded v.
func jsonEncodeInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

// jsonEncodeUint returns the JSON-encoded v.
func jsonEncodeUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// jsonEncodeFloat returns the JSON-encoded v.
func jsonEncodeFloat(v float64, bitSize int) string {
	return strconv.FormatFloat(v, 'g', -1, bitSize)
}

// jsonEncodeString returns the JSON-encoded v.
func jsonEncodeString(v string) string {
	return string(jsonAppendString(nil, v))
}

// jsonEncodeTime returns the JSON-encoded v.
func jsonEncodeTime(v time.Time) string {
	return string(jsonAppendTime(nil, v))
}

// jsonEncodeGeo returns the JSON-encoded v.
func jsonEncodeGeo(v Geo) string {
	return string(jsonAppendGeo(nil, v))
}

// jsonEncodeValue returns the JSON-encoded v.
// If the type of v is unsupported, it returns "null".
func jsonEncodeValue(v reflect.Value) string {
	return string(jsonAppendValue(nil, v))
}

// jsonEncode returns the JSON-encoded v.
// If the type of v is unsupported, it returns "null".
func jsonEncode(v interface{}) string {
	return jsonEncodeValue(reflect.ValueOf(v))
}
