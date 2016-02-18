package grnci

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// This file provides an interface to access struct details.

// tagKey is the key of a struct field tag that specifies details of the
// associated Groonga column.
const tagKey = "grnci"
const oldTagKey = "groonga"

// tagSep is the separator of a struct field tag value.
const tagSep = ';'

// FieldInfo stores information of a target field.
type FieldInfo struct {
	id    int                  // Field ID
	field *reflect.StructField // Field
	tags  []string             // Field tag semicolon-separated values
	typ   reflect.Type         // Terminal type
	dim   int                  // Vector dimension
}

// parseFieldTag parses a struct field tag value.
func parseFieldTag(s string) ([]string, error) {
	s = strings.TrimSpace(s)
	var vals []string
	for len(s) != 0 {
		i := 0
		for i < len(s) {
			if s[i] == '"' {
				for i++; i < len(s); i++ {
					if s[i] == '"' {
						break
					} else if s[i] == '\\' {
						if i == (len(s) - 1) {
							return nil, fmt.Errorf("invalid '\\' in field tag")
						}
						i++
					}
				}
				if i == len(s) {
					return nil, fmt.Errorf("invalid '\"' in field tag")
				}
			} else if s[i] == '\\' {
				if i == (len(s) - 1) {
					return nil, fmt.Errorf("invalid '\\' in field tag")
				}
				i++
			} else if s[i] == ';' {
				break
			}
			i++
		}
		vals = append(vals, s[:i])
		if i < len(s) {
			i++
		}
		s = s[i:]
	}
	for i, _ := range vals {
		vals[i] = strings.TrimSpace(vals[i])
		if strings.HasSuffix(vals[i], "*") {
			return nil, fmt.Errorf("invalid '*' in field tag")
		}
	}
	return vals, nil
}

// newFieldInfo returns a FieldInfo.
// If field is non-target, newFieldInfo returns nil.
func newFieldInfo(id int, field *reflect.StructField) (*FieldInfo, error) {
	info := FieldInfo{id: id, field: field}
	tag := field.Tag.Get(tagKey)
	if len(tag) == 0 {
		tag = field.Tag.Get(oldTagKey)
	}
	tags, err := parseFieldTag(tag)
	if err != nil {
		return nil, err
	}
	info.tags = tags
	info.typ = field.Type
	for {
		if info.typ.Kind() == reflect.Ptr {
			info.typ = info.typ.Elem()
		} else if info.typ.Kind() == reflect.Slice {
			info.typ = info.typ.Elem()
			info.dim++
		} else {
			break
		}
	}
	switch info.typ {
	case boolType, intType, floatType, timeType, textType, geoType:
	default:
		return nil, nil
	}
	return &info, nil
}

// ID returns the field ID.
func (info *FieldInfo) ID() int {
	return info.id
}

// Name returns the field name.
func (info *FieldInfo) Name() string {
	return info.field.Name
}

// Type returns the field type.
func (info *FieldInfo) Type() reflect.Type {
	return info.field.Type
}

// Tag returns the i-th tag value.
func (info *FieldInfo) Tag(i int) string {
	if i >= len(info.tags) {
		return ""
	}
	return info.tags[i]
}

// TerminalType returns the terminal type.
func (info *FieldInfo) TerminalType() reflect.Type {
	return info.typ
}

// Dimension returns the vector dimension.
func (info *FieldInfo) Dimension() int {
	return info.dim
}

// ColumnName returns the name of the associated column.
func (info *FieldInfo) ColumnName() string {
	if (len(info.tags) == 0) || (len(info.tags[0]) == 0) {
		return info.Name()
	}
	return info.tags[0]
}

// StructInfo stores information of a struct.
type StructInfo struct {
	typ             reflect.Type          // Struct type
	fields          []*FieldInfo          // Struct fields
	fieldsByColName map[string]*FieldInfo // Struct fields by column name
	err             error                 // Error
}

// Type returns the source type.
func (info *StructInfo) Type() reflect.Type {
	return info.typ
}

// NumField returns the number of target fields.
func (info *StructInfo) NumField() int {
	return len(info.fields)
}

// Field returns the i-th target field.
func (info *StructInfo) Field(i int) *FieldInfo {
	return info.fields[i]
}

// FieldByColumnName returns the target field with the given column name.
func (info *StructInfo) FieldByColumnName(name string) *FieldInfo {
	return info.fieldsByColName[name]
}

// Error returns the error.
func (info *StructInfo) Error() error {
	return info.err
}

// Registered struct information.
var (
	structInfoNil    = StructInfo{err: fmt.Errorf("not a struct type")}
	structInfos      = make(map[reflect.Type]*StructInfo)
	structInfosMutex sync.Mutex
)

// getStructInfoFromType returns information of a struct.
func getStructInfoFromType(typ reflect.Type) *StructInfo {
	structInfosMutex.Lock()
	defer structInfosMutex.Unlock()
	if info, ok := structInfos[typ]; ok {
		return info
	}
	if typ.Kind() != reflect.Struct {
		return &structInfoNil
	}
	fieldInfos := make([]*FieldInfo, 0)
	fieldInfosByColName := make(map[string]*FieldInfo)
	var err error
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if len(field.PkgPath) != 0 {
			continue
		}
		var fieldInfo *FieldInfo
		fieldInfo, err = newFieldInfo(i, &field)
		if err != nil {
			break
		}
		if fieldInfo == nil {
			continue
		}
		fieldInfos = append(fieldInfos, fieldInfo)
		if _, ok := fieldInfosByColName[fieldInfo.ColumnName()]; ok {
			err = fmt.Errorf("duplicate column name %#v", fieldInfo.ColumnName())
			break
		} else {
			fieldInfosByColName[fieldInfo.ColumnName()] = fieldInfo
		}
	}
	info := &StructInfo{typ, fieldInfos, fieldInfosByColName, err}
	structInfos[typ] = info
	return info
}

// GetStructInfo returns information of a struct.
func GetStructInfo(v interface{}) *StructInfo {
	if v == nil {
		return &structInfoNil
	}
	typ := reflect.TypeOf(v)
	for {
		switch typ.Kind() {
		case reflect.Ptr, reflect.Slice, reflect.Array:
			typ = typ.Elem()
		default:
			return getStructInfoFromType(typ)
		}
	}
}
