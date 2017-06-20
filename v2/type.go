package grnci

import (
	"reflect"
	"strconv"
	"time"
)

// Geo represents a TokyoGeoPoint or WGS84GeoPoint.
type Geo struct {
	Lat  int32 // Latitude in milliseconds.
	Long int32 // Longitude in milliseconds.
}

// encodeBool encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeBool(buf []byte, v bool) []byte {
	return strconv.AppendBool(buf, v)
}

// encodeInt encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeInt(buf []byte, v int64) []byte {
	return strconv.AppendInt(buf, v, 10)
}

// encodeUint encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeUint(buf []byte, v uint64) []byte {
	return strconv.AppendUint(buf, v, 10)
}

// encodeFloat encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeFloat(buf []byte, v float64) []byte {
	return strconv.AppendFloat(buf, v, 'g', -1, 64)
}

// encodeString encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeString(buf []byte, v string) []byte {
	buf = append(buf, '"')
	for i := 0; i < len(v); i++ {
		switch v[i] {
		case '\b', '\t', '\n', '\f', '\r', '"', '\\':
			buf = append(buf, '\\')
		}
		buf = append(buf, v[i])
	}
	return append(buf, '"')
}

// encodeTime encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeTime(buf []byte, v time.Time) []byte {
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

// encodeGeo encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeGeo(buf []byte, v Geo) []byte {
	buf = append(buf, '"')
	buf = strconv.AppendInt(buf, int64(v.Lat), 10)
	buf = append(buf, ',')
	buf = strconv.AppendInt(buf, int64(v.Long), 10)
	return append(buf, '"')
}

// encodeValue encodes the JSON-encoded v to buf and returns the extended buffer.
func encodeValue(buf []byte, v reflect.Value) []byte {
	switch v.Kind() {
	case reflect.Bool:
		return encodeBool(buf, v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return encodeInt(buf, v.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return encodeUint(buf, v.Uint())
	case reflect.Float64:
		return encodeFloat(buf, v.Float())
	case reflect.String:
		return encodeString(buf, v.String())
	case reflect.Struct:
		switch v := v.Interface().(type) {
		case time.Time:
			return encodeTime(buf, v)
		case Geo:
			return encodeGeo(buf, v)
		default:
			return append(buf, "null"...)
		}
	case reflect.Ptr:
		if v.IsNil() {
			return append(buf, "null"...)
		}
		return encodeValue(buf, v.Elem())
	case reflect.Array:
		buf = append(buf, '[')
		n := v.Len()
		for i := 0; i < n; i++ {
			if i != 0 {
				buf = append(buf, ',')
			}
			buf = encodeValue(buf, v.Index(i))
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
			buf = encodeValue(buf, v.Index(i))
		}
		return append(buf, ']')
	default:
		return append(buf, "null"...)
	}
}

// // encodeBoolPtr encodes the JSON-encoded v to buf and returns the extended buffer.
// func encodeBoolPtr(buf []byte, v *bool) []byte {
// 	if v == nil {
// 		return append(buf, "null"...)
// 	}
// 	return encodeBool(buf, *v)
// }

// // encodeIntPtr encodes the JSON-encoded v to buf and returns the extended buffer.
// func encodeIntPtr(buf []byte, v *int64) []byte {
// 	if v == nil {
// 		return append(buf, "null"...)
// 	}
// 	return encodeInt(buf, *v)
// }

// // encodeUintPtr encodes the JSON-encoded v to buf and returns the extended buffer.
// func encodeUintPtr(buf []byte, v *uint64) []byte {
// 	if v == nil {
// 		return append(buf, "null"...)
// 	}
// 	return encodeUint(buf, *v)
// }

// // encodeFloatPtr encodes the JSON-encoded v to buf and returns the extended buffer.
// func encodeFloatPtr(buf []byte, v *float64) []byte {
// 	if v == nil {
// 		return append(buf, "null"...)
// 	}
// 	return encodeFloat(buf, *v)
// }

// // encodeStringPtr encodes the JSON-encoded v to buf and returns the extended buffer.
// func encodeStringPtr(buf []byte, v *string) []byte {
// 	if v == nil {
// 		return append(buf, "null"...)
// 	}
// 	return encodeString(buf, *v)
// }

// // encodeTimePtr encodes the JSON-encoded v to buf and returns the extended buffer.
// func encodeTimePtr(buf []byte, v *time.Time) []byte {
// 	if v == nil {
// 		return append(buf, "null"...)
// 	}
// 	return encodeTime(buf, *v)
// }

// // encodeGeoPtr encodes the JSON-encoded v to buf and returns the extended buffer.
// func encodeGeoPtr(buf []byte, v *Geo) []byte {
// 	if v == nil {
// 		return append(buf, "null"...)
// 	}
// 	return encodeGeo(buf, *v)
// }
