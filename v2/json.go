package grnci

import (
	"reflect"
	"strconv"
	"time"
)

// AppendJSONBool appends the JSON-encoded v to buf and returns the extended buffer.
func AppendJSONBool(buf []byte, v bool) []byte {
	return strconv.AppendBool(buf, v)
}

// AppendJSONInt appends the JSON-encoded v to buf and returns the extended buffer.
func AppendJSONInt(buf []byte, v int64) []byte {
	return strconv.AppendInt(buf, v, 10)
}

// AppendJSONUint appends the JSON-encoded v to buf and returns the extended buffer.
func AppendJSONUint(buf []byte, v uint64) []byte {
	return strconv.AppendUint(buf, v, 10)
}

// AppendJSONFloat appands the JSON-encoded v to buf and returns the extended buffer.
func AppendJSONFloat(buf []byte, v float64, bitSize int) []byte {
	return strconv.AppendFloat(buf, v, 'g', -1, bitSize)
}

// AppendJSONString appends the JSON-encoded v to buf and returns the extended buffer.
func AppendJSONString(buf []byte, v string) []byte {
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

// AppendJSONTime appends the JSON-encoded v to buf and returns the extended buffer.
func AppendJSONTime(buf []byte, v time.Time) []byte {
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

// AppendJSONGeo appends the JSON-encoded v to buf and returns the extended buffer.
func AppendJSONGeo(buf []byte, v Geo) []byte {
	buf = append(buf, '"')
	buf = strconv.AppendInt(buf, int64(v.Lat), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(v.Long), 10)
	return append(buf, '"')
}

// AppendJSONValue appends the JSON-encoded v to buf and returns the extended buffer.
// If the type of v is unsupported, AppendJSONValue appends "null".
func AppendJSONValue(buf []byte, v reflect.Value) []byte {
	switch v.Kind() {
	case reflect.Bool:
		return AppendJSONBool(buf, v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return AppendJSONInt(buf, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return AppendJSONUint(buf, v.Uint())
	case reflect.Float32:
		return AppendJSONFloat(buf, v.Float(), 32)
	case reflect.Float64:
		return AppendJSONFloat(buf, v.Float(), 64)
	case reflect.String:
		return AppendJSONString(buf, v.String())
	case reflect.Struct:
		switch v := v.Interface().(type) {
		case time.Time:
			return AppendJSONTime(buf, v)
		case Geo:
			return AppendJSONGeo(buf, v)
		default:
			return append(buf, "null"...)
		}
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return append(buf, "null"...)
		}
		return AppendJSONValue(buf, v.Elem())
	case reflect.Array:
		buf = append(buf, '[')
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, ',')
			}
			buf = AppendJSONValue(buf, v.Index(i))
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
			buf = AppendJSONValue(buf, v.Index(i))
		}
		return append(buf, ']')
	default:
		return append(buf, "null"...)
	}
}

// AppendJSON appends the JSON-encoded v to buf and returns the extended buffer.
// If the type of v is unsupported, AppendJSON appends "null".
func AppendJSON(buf []byte, v interface{}) []byte {
	if v == nil {
		return append(buf, "null"...)
	}
	return AppendJSONValue(buf, reflect.ValueOf(v))
}

// EncodeJSONBool returns the JSON-encoded v.
func EncodeJSONBool(v bool) string {
	return strconv.FormatBool(v)
}

// EncodeJSONInt returns the JSON-encoded v.
func EncodeJSONInt(v int64) string {
	return strconv.FormatInt(v, 10)
}

// EncodeJSONUint returns the JSON-encoded v.
func EncodeJSONUint(v uint64) string {
	return strconv.FormatUint(v, 10)
}

// EncodeJSONFloat returns the JSON-encoded v.
func EncodeJSONFloat(v float64, bitSize int) string {
	return strconv.FormatFloat(v, 'g', -1, bitSize)
}

// EncodeJSONString returns the JSON-encoded v.
func EncodeJSONString(v string) string {
	return string(AppendJSONString(nil, v))
}

// EncodeJSONTime returns the JSON-encoded v.
func EncodeJSONTime(v time.Time) string {
	return string(AppendJSONTime(nil, v))
}

// EncodeJSONGeo returns the JSON-encoded v.
func EncodeJSONGeo(v Geo) string {
	return string(AppendJSONGeo(nil, v))
}

// EncodeJSONValue returns the JSON-encoded v.
// If the type of v is unsupported, EncodeJSONValue returns "null".
func EncodeJSONValue(v reflect.Value) string {
	return string(AppendJSONValue(nil, v))
}

// EncodeJSON returns the JSON-encoded v.
// If the type of v is unsupported, EncodeJSON returns "null".
func EncodeJSON(v interface{}) string {
	return string(AppendJSON(nil, v))
}
