package grnci

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// This source file provides built-in data types and utility functions.
//
// Time and Geo require MarshalJSON and UnmarshalJSON because Groonga uses the
// special format for these types.

// Bool represents Bool.
type Bool bool

// Int represents Int8, Int16, Int32, Int64, UInt8, UInt16, UInt32 and UInt64.
type Int int64

// Float represents Float.
type Float float64

// Time represents Time.
type Time int64 // The number of microseconds elapsed since the Unix epoch.

// Text represents ShortText, Text and LongText.
type Text string

// Geo represents TokyoGeoPoint and WGS84GeoPoint.
type Geo struct {
	Lat  int32 // Latitude in milliseconds.
	Long int32 // Longitude in milliseconds.
}

var (
	boolType  = reflect.TypeOf(Bool(false))
	intType   = reflect.TypeOf(Int(0))
	floatType = reflect.TypeOf(Float(0.0))
	timeType  = reflect.TypeOf(Time(0))
	textType  = reflect.TypeOf(Text(""))
	geoType   = reflect.TypeOf(Geo{0, 0})

	vBoolType  = reflect.TypeOf([]Bool(nil))
	vIntType   = reflect.TypeOf([]Int(nil))
	vFloatType = reflect.TypeOf([]Float(nil))
	vTimeType  = reflect.TypeOf([]Time(nil))
	vTextType  = reflect.TypeOf([]Text(nil))
	vGeoType   = reflect.TypeOf([]Geo(nil))
)

// writeTo writes val to buf.
func (val *Bool) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, bool(*val))
	return err
}

// writeTo writes val to buf.
func (val *Int) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, int64(*val))
	return err
}

// writeTo writes val to buf.
func (val *Float) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprint(buf, float64(*val))
	return err
}

// writeTo writes val to buf.
func (val *Time) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	sec := int64(*val) / 1000000
	usec := int64(*val) % 1000000
	_, err := fmt.Fprintf(buf, "%d.%06d", sec, usec)
	return err
}

// writeTo writes val to buf.
func (val *Text) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	str := strings.Replace(string(*val), "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	_, err := fmt.Fprintf(buf, "\"%s\"", str)
	return err
}

// writeTo writes val to buf.
func (val *Geo) writeTo(buf *bytes.Buffer) error {
	if val == nil {
		_, err := buf.WriteString("null")
		return err
	}
	_, err := fmt.Fprintf(buf, "\"%d,%d\"", val.Lat, val.Long)
	return err
}

// MarshalJSON encodes Time to JSON bytes.
//
// Time is represented by the number of seconds elapsed since the Unix Epoch in
// JSON.
//
// http://groonga.org/docs/tutorial/data.html#date-and-time-type
func (val Time) MarshalJSON() ([]byte, error) {
	sec := int64(val) / 1000000
	usec := int64(val) % 1000000
	return []byte(fmt.Sprintf("%d.%06d", sec, usec)), nil
}

// MarshalJSON encodes Geo to JSON bytes.
//
// Geo is represented by a string with the format "Lat,Long" in JSON.
//
// http://groonga.org/docs/tutorial/data.html#longitude-and-latitude-types
func (val Geo) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%d,%d\"", val.Lat, val.Long)), nil
}

// UnmarshalJSON decodes JSON bytes to Time.
//
// http://groonga.org/docs/tutorial/data.html#date-and-time-type
func (val *Time) UnmarshalJSON(data []byte) error {
	str := string(data)
	idx := strings.IndexByte(str, '.')
	if idx == -1 {
		sec, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return err
		}
		*val = Time(sec * 1000000)
		return nil
	}
	sec, err := strconv.ParseInt(str[:idx], 10, 64)
	if err != nil {
		return err
	}
	usec, err := strconv.ParseInt(str[idx+1:], 10, 64)
	if err != nil {
		return err
	}
	*val = Time(sec*1000000 + usec)
	return nil
}

// UnmarshalJSON decodes JSON bytes to Geo.
//
// http://groonga.org/docs/tutorial/data.html#longitude-and-latitude-types
func (val *Geo) UnmarshalJSON(data []byte) error {
	str := string(data)
	if (len(str) < 2) || (str[0] != '"') || (str[len(str)-1] != '"') {
		return fmt.Errorf("Geo must be a string in JSON")
	}
	str = str[1 : len(str)-1]
	idx := strings.IndexAny(str, "x,")
	if idx == -1 {
		return fmt.Errorf("Geo needs a separator 'x' or ',' in JSON")
	}
	if strings.Contains(str, ".") {
		lat, err := strconv.ParseFloat(str[:idx], 64)
		if err != nil {
			return err
		}
		long, err := strconv.ParseFloat(str[idx+1:], 64)
		if err != nil {
			return err
		}
		val.Lat = int32(lat * 60 * 60 * 1000)
		val.Long = int32(long * 60 * 60 * 1000)
	} else {
		lat, err := strconv.ParseInt(str[:idx], 10, 32)
		if err != nil {
			return err
		}
		long, err := strconv.ParseInt(str[idx+1:], 10, 32)
		if err != nil {
			return err
		}
		val.Lat = int32(lat)
		val.Long = int32(long)
	}
	return nil
}

// Now returns the current time.
func Now() Time {
	now := time.Now()
	return Time((now.Unix() * 1000000) + (now.UnixNano() / 1000))
}

// Unix returns sec and nsec for time.Unix.
func (val Time) Unix() (sec, nsec int64) {
	sec = int64(val) / 1000000
	nsec = (int64(val) % 1000000) * 1000
	return
}
