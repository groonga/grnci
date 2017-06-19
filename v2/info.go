package grnci

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	// tagKey is the tag key for struct fields associated with Groonga columns.
	tagKey = "grnci"
	// tagSep is the separator in a struct field tag value.
	tagSep = ';'
)

var (
	structInfos      = make(map[reflect.Type]*StructInfo)
	structInfosMutex sync.RWMutex
)

type StructFieldInfo struct {
	Index      int                  // Field position
	Field      *reflect.StructField // Field
	Type       reflect.Type         // Field's underlying type
	Tags       []string             // Field tag semicolon-separated values
	ColumnName string               // Column name
	Dimension  int                  // Vector dimension
}

// newStructFieldInfo returns a StructFieldInfo.
func newStructFieldInfo(index int, field *reflect.StructField) (*StructFieldInfo, error) {
	tagValue := field.Tag.Get(tagKey)
	tags := strings.Split(tagValue, ";")
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
	}
	if strings.HasSuffix(tags[0], "*") {
		return nil, NewError(InvalidType, map[string]interface{}{
			"tag":   tagValue,
			"error": "The first tag must not end with '*'.",
		})
	}
	typ := field.Type
	dim := 0
	for {
		if typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		} else if typ.Kind() == reflect.Slice {
			typ = typ.Elem()
			dim++
		} else {
			break
		}
	}
	switch reflect.Zero(typ).Interface().(type) {
	case bool:
	case int, int8, int16, int32, int64:
	case uint, uint8, uint16, uint32, uint64:
	case float32, float64:
	case string:
	case time.Time:
	default:
		return nil, NewError(InvalidType, map[string]interface{}{
			"type":  typ.Name(),
			"error": "The type is not supported.",
		})
	}
	return &StructFieldInfo{
		Index:      index,
		Field:      field,
		Tags:       tags,
		Type:       field.Type,
		ColumnName: tags[0],
		Dimension:  dim,
	}, nil
}

type StructInfo struct {
	Type               reflect.Type
	Fields             []*StructFieldInfo
	FieldsByName       map[string]*StructFieldInfo
	FieldsByColumnName map[string]*StructFieldInfo
}

// getStructInfo returns the StructInfo that represents typ.
func getStructInfo(typ reflect.Type) (*StructInfo, error) {
	structInfosMutex.Lock()
	defer structInfosMutex.Unlock()
	if si, ok := structInfos[typ]; ok {
		return si, nil
	}
	fis := make([]*StructFieldInfo, 0)
	fisByName := make(map[string]*StructFieldInfo)
	fisByColumnName := make(map[string]*StructFieldInfo)
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		if f.PkgPath != "" { // Skip unexported fields.
			continue
		}
		tag := f.Tag.Get(tagKey)
		if tag == "" || tag == "-" { // Skip untagged fields.
			continue
		}
		fi, err := newStructFieldInfo(i, &f)
		if err != nil {
			return nil, err
		}
		fis = append(fis, fi)
		fisByName[f.Name] = fi
		if _, ok := fisByColumnName[fi.ColumnName]; ok {
			return nil, NewError(InvalidType, map[string]interface{}{
				"columnName": fi.ColumnName,
				"error":      "The column name appears more than once.",
			})
		}
		fisByColumnName[fi.ColumnName] = fi
	}
	si := &StructInfo{
		Type:               typ,
		Fields:             fis,
		FieldsByName:       fisByName,
		FieldsByColumnName: fisByColumnName,
	}
	structInfos[typ] = si
	return si, nil
}

// GetStructInfo returns the StructInfo that represents the underlying struct of i.
// If i is nil or the underlying type is not a struct, GetStructInfo returns an error.
func GetStructInfo(v interface{}) (*StructInfo, error) {
	if v == nil {
		return nil, NewError(InvalidType, map[string]interface{}{
			"value": nil,
			"error": "The value must not be nil.",
		})
	}
	typ := reflect.TypeOf(v)
	for {
		switch typ.Kind() {
		case reflect.Ptr, reflect.Slice, reflect.Array:
			typ = typ.Elem()
		default:
			if kind := typ.Kind(); kind != reflect.Struct {
				return nil, NewError(InvalidType, map[string]interface{}{
					"kind":  kind.String(),
					"error": fmt.Sprintf("The kind must be %s.", reflect.Struct),
				})
			}
			return getStructInfo(typ)
		}
	}
}
