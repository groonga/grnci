package grnci

import (
	"reflect"
	"strings"
	"sync"
	"time"
)

// Geo is a geographical (latitude-longitude) location and
// associated with WGS84GeoPoint and TokyoGeoPoint in Groonga.
type Geo struct {
	Lat  int32 // Latitude in milliseconds.
	Long int32 // Longitude in milliseconds.
}

const (
	// columnFieldTagKey is the tag key for a struct field associated with a column.
	columnFieldTagKey = "grnci"
	// columnFieldTagDelim is the delimiter in a struct field tag value.
	columnFieldTagDelim = ";"
)

// ColumnField stores the details of a struct field associated with a column.
// The tag format is as follows:
//
//  grnci:"_key;key_type;flags;default_tokenizer;normalizer;token_filters"
//  grnci:"_value;value_type"
//  grnci:"name;type;flags"
//
// TODO: support dynamic columns (--columns[NAME]).
type ColumnField struct {
	Field            *reflect.StructField // Struct field
	Index            int                  // Index of the struct field
	Name             string               // Column name
	Type             string               // --key_type for _key, --value_type for _value or --type for columns
	Flags            []string             // --flags for _key and columns
	DefaultTokenizer string               // --default_tokenizer for _key
	Normalizer       string               // --normalizer for _key
	TokenFilters     []string             // --token_filters for _key
	Loadable         bool                 // Whether or not the column is loadable
}

// checkTableName checks if s is valid as a table name.
func checkTableName(s string) error {
	switch s {
	case "":
		return NewError2(TypeError, "A table name must not be empty.", map[string]interface{}{
			"name": s,
		})
	case "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
		"Float", "ShortText", "Text", "LongText", "Time", "WGS84GeoPoint", "TokyoGeoPoint":
		return NewError2(TypeError, "The name specifies a built-in type and not available as a table name.", map[string]interface{}{
			"name": s,
		})
	}
	if s[0] == '_' {
		return NewError2(TypeError, "A table name must not start with '_'.", map[string]interface{}{
			"name": s,
		})
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		case c == '_':
		default:
			return NewError2(TypeError, "A table name must consist of [0-9A-Za-z_].", map[string]interface{}{
				"name": s,
			})
		}
	}
	return nil
}

// parseIDOptions parses options of _id.
func (cf *ColumnField) parseIDOptions(options []string) error {
	if len(options) > 1 {
		return NewError2(TypeError, "The tag must not contain more than one option.", map[string]interface{}{
			"name":    cf.Name,
			"options": options,
		})
	}
	if len(options) > 0 {
		cf.Type = options[0]
	}
	switch cf.Type {
	case "":
		cf.Type = "UInt32"
	case "UInt32":
	default:
		return NewError2(TypeError, "The type is not supported as _id.", map[string]interface{}{
			"type": cf.Type,
		})
	}
	return nil
}

// checkKeyType checks if cf.Type is valid as _key.
func (cf *ColumnField) checkKeyType() error {
	switch cf.Type {
	case "":
		// _key must not be a pointer.
		typ := cf.Field.Type
		switch typ.Kind() {
		case reflect.Bool:
			cf.Type = "Bool"
		case reflect.Int8:
			cf.Type = "Int8"
		case reflect.Int16:
			cf.Type = "Int16"
		case reflect.Int32:
			cf.Type = "Int32"
		case reflect.Int64, reflect.Int:
			cf.Type = "Int64"
		case reflect.Uint8:
			cf.Type = "UInt8"
		case reflect.Uint16:
			cf.Type = "UInt16"
		case reflect.Uint32:
			cf.Type = "UInt32"
		case reflect.Uint64, reflect.Uint:
			cf.Type = "UInt64"
		case reflect.Float32, reflect.Float64:
			cf.Type = "Float"
		case reflect.String:
			cf.Type = "ShortText"
		case reflect.Struct:
			switch reflect.Zero(typ).Interface().(type) {
			case time.Time:
				cf.Type = "Time"
			case Geo:
				cf.Type = "WGS84GeoPoint"
			}
		}
		if cf.Type == "" {
			return NewError2(TypeError, "The type is not supported as _key.", map[string]interface{}{
				"type": reflect.TypeOf(cf.Field.Type).Name(),
			})
		}
	case "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
		"Float", "ShortText", "Time", "WGS84GeoPoint", "TokyoGeoPoint":
	default:
		if err := checkTableName(cf.Type); err != nil {
			return NewError2(TypeError, "The type is not supported as _key.", map[string]interface{}{
				"type": cf.Type,
			})
		}
	}
	return nil
}

// checkKey checks if cf is valid as _key.
func (cf *ColumnField) checkKey() error {
	if err := cf.checkKeyType(); err != nil {
		return err
	}
	// TODO: check Flags, DefaultTokenizer, Normalizer and TokenFilters.
	return nil
}

// parseKeyOptions parses options of _key.
func (cf *ColumnField) parseKeyOptions(options []string) error {
	if len(options) > 5 {
		return NewError(TypeError, map[string]interface{}{
			"name":    cf.Name,
			"options": options,
			"error":   "The tag must not contain more than 5 options.",
		})
	}
	if len(options) > 0 {
		cf.Type = options[0]
	}
	if len(options) > 1 {
		cf.Flags = strings.Split(options[1], "|")
	}
	if len(options) > 2 {
		cf.DefaultTokenizer = options[2]
	}
	if len(options) > 3 {
		cf.Normalizer = options[3]
	}
	if len(options) > 4 {
		cf.TokenFilters = strings.Split(options[4], ",")
	}
	if err := cf.checkKey(); err != nil {
		return err
	}
	cf.Loadable = true
	return nil
}

// checkValue checks if cf is valid as _value.
func (cf *ColumnField) checkValue() error {
	switch cf.Type {
	case "":
		typ := cf.Field.Type
		for typ.Kind() == reflect.Ptr {
			typ = typ.Elem()
		}
		switch typ.Kind() {
		case reflect.Bool:
			cf.Type = "Bool"
		case reflect.Int8:
			cf.Type = "Int8"
		case reflect.Int16:
			cf.Type = "Int16"
		case reflect.Int32:
			cf.Type = "Int32"
		case reflect.Int64, reflect.Int:
			cf.Type = "Int64"
		case reflect.Uint8:
			cf.Type = "UInt8"
		case reflect.Uint16:
			cf.Type = "UInt16"
		case reflect.Uint32:
			cf.Type = "UInt32"
		case reflect.Uint64, reflect.Uint:
			cf.Type = "UInt64"
		case reflect.Float32, reflect.Float64:
			cf.Type = "Float"
		case reflect.Struct:
			switch reflect.Zero(typ).Interface().(type) {
			case time.Time:
				cf.Type = "Time"
			case Geo:
				cf.Type = "WGS84GeoPoint"
			}
		}
		if cf.Type == "" {
			return NewError2(TypeError, "The type is not supported as _value.", map[string]interface{}{
				"type": reflect.TypeOf(cf.Field.Type).Name(),
			})
		}
	case "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
		"Float", "Time", "WGS84GeoPoint", "TokyoGeoPoint":
	default:
		return NewError2(TypeError, "The type is not supported as _value.", map[string]interface{}{
			"type": cf.Type,
		})
	}
	return nil
}

// parseValueOptions parses options of _value.
func (cf *ColumnField) parseValueOptions(options []string) error {
	if len(options) > 1 {
		return NewError2(TypeError, "The tag must not contain more than one option.", map[string]interface{}{
			"name":    cf.Name,
			"options": options,
		})
	}
	if len(options) > 0 {
		cf.Type = options[0]
	}
	if err := cf.checkValue(); err != nil {
		return err
	}
	cf.Loadable = true
	return nil
}

// parseScoreOptions parses options of _score.
func (cf *ColumnField) parseScoreOptions(options []string) error {
	if len(options) > 1 {
		return NewError2(TypeError, "The tag must not contain more than one option.", map[string]interface{}{
			"name":    cf.Name,
			"options": options,
		})
	}
	if len(options) > 0 {
		cf.Type = options[0]
	}
	// If the command version is 1, the type of _score is Int32.
	// Otherwise, the type of _score is Float.
	switch cf.Type {
	case "":
		cf.Type = "Float"
	case "Int32", "Float":
	default:
		return NewError2(TypeError, "The type is not supported as _score.", map[string]interface{}{
			"type": cf.Type,
		})
	}
	return nil
}

// detectColumnType detects cf.Type from cf.Field.Type.
func (cf *ColumnField) detectColumnType() error {
	typ := cf.Field.Type
	dim := 0
Loop:
	for {
		switch typ.Kind() {
		case reflect.Ptr:
			typ = typ.Elem()
		case reflect.Array, reflect.Slice:
			dim++
			typ = typ.Elem()
		default:
			break Loop
		}
	}
	switch typ.Kind() {
	case reflect.Bool:
		cf.Type = "Bool"
	case reflect.Int8:
		cf.Type = "Int8"
	case reflect.Int16:
		cf.Type = "Int16"
	case reflect.Int32:
		cf.Type = "Int32"
	case reflect.Int64, reflect.Int:
		cf.Type = "Int64"
	case reflect.Uint8:
		cf.Type = "UInt8"
	case reflect.Uint16:
		cf.Type = "UInt16"
	case reflect.Uint32:
		cf.Type = "UInt32"
	case reflect.Uint64, reflect.Uint:
		cf.Type = "UInt64"
	case reflect.Float32, reflect.Float64:
		cf.Type = "Float"
	case reflect.String:
		cf.Type = "ShortText"
	case reflect.Struct:
		switch reflect.Zero(typ).Interface().(type) {
		case time.Time:
			cf.Type = "Time"
		case Geo:
			cf.Type = "WGS84GeoPoint"
		}
	}
	if cf.Type == "" {
		return NewError2(TypeError, "The type is not supported as a column.", map[string]interface{}{
			"type": reflect.TypeOf(cf.Field.Type).Name(),
		})
	}
	cf.Type = strings.Repeat("[]", dim) + cf.Type
	return nil
}

// checkColumnType checks if cf.Type is valid as a column.
//
// TODO: index columns should be supported.
func (cf *ColumnField) checkColumnType() error {
	if cf.Type == "" {
		return cf.detectColumnType()
	}
	typ := cf.Type
	for strings.HasPrefix(typ, "[]") {
		typ = typ[2:]
	}
	switch typ {
	case "Bool", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64",
		"Float", "ShortText", "Text", "LongText", "Time", "WGS84GeoPoint", "TokyoGeoPoint":
	default:
		if err := checkTableName(typ); err != nil {
			return NewError2(TypeError, "The type is not supported as a column.", map[string]interface{}{
				"type": cf.Type,
			})
		}
	}
	return nil
}

// checkColumnName checks if cf.Name is valid as a column name.
// If cf.Name specifies a pseudo column, it returns an error.
func (cf *ColumnField) checkColumnName() error {
	s := cf.Name
	if s == "" {
		return NewError2(TypeError, "A column name must not be empty.", map[string]interface{}{
			"name": s,
		})
	}
	if s[0] == '_' {
		return NewError2(TypeError, "A column name must not start with '_'.", map[string]interface{}{
			"name": s,
		})
	}
	loadable := true
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'A' && c <= 'Z':
		case c >= 'a' && c <= 'z':
		default:
			switch c {
			case '_':
			default:
				// A column name may contain various symbol characters
				// because functions such as snippet_html are available.
				loadable = false
			}

		}
	}
	cf.Loadable = loadable
	return nil
}

// checkColumn checks if cf is valid as a column.
func (cf *ColumnField) checkColumn() error {
	if err := cf.checkColumnName(); err != nil {
		return err
	}
	if err := cf.checkColumnType(); err != nil {
		return err
	}
	// TODO: check Flags.
	return nil
}

// parseColumnOptions parses options of a column.
func (cf *ColumnField) parseColumnOptions(options []string) error {
	if len(options) > 2 {
		return NewError2(TypeError, "The tag must not contain more than 2 options.", map[string]interface{}{
			"name":    cf.Name,
			"options": options,
		})
	}
	if len(options) > 0 {
		cf.Type = options[0]
	}
	if len(options) > 1 {
		cf.Flags = strings.Split(options[1], "|")
	}
	return cf.checkColumn()
}

// parseOptions parses options of a column.
func (cf *ColumnField) parseOptions(options []string) error {
	switch cf.Name {
	case "_id":
		return cf.parseIDOptions(options)
	case "_key":
		return cf.parseKeyOptions(options)
	case "_value":
		return cf.parseValueOptions(options)
	case "_score":
		return cf.parseScoreOptions(options)
	default:
		return cf.parseColumnOptions(options)
	}
}

// newColumnField returns a new ColumnField.
func newColumnField(field *reflect.StructField, index int) (*ColumnField, error) {
	tag := field.Tag.Get(columnFieldTagKey)
	if tag == "" {
		return nil, NewError2(TypeError, "The struct field must have a non-empty "+columnFieldTagKey+" tag.", map[string]interface{}{
			"name": field.Name,
			"tag":  field.Tag,
		})
	}
	values := strings.Split(tag, columnFieldTagDelim)
	cf := &ColumnField{
		Field: field,
		Index: index,
		Name:  values[0],
	}
	if err := cf.parseOptions(values[1:]); err != nil {
		return nil, err
	}
	return cf, nil
}

// RowStruct stores the details of a struct associated with a row.
type RowStruct struct {
	Columns       []*ColumnField
	ColumnsByName map[string]*ColumnField
}

var (
	rowStructs      = make(map[reflect.Type]*RowStruct)
	rowStructsMutex sync.Mutex
)

// GetRowStruct returns a RowStruct for the terminal type of v.
func GetRowStruct(v interface{}) (*RowStruct, error) {
	typ := reflect.TypeOf(v)
Loop:
	for {
		switch typ.Kind() {
		case reflect.Ptr, reflect.Interface, reflect.Array, reflect.Slice:
			typ = typ.Elem()
		case reflect.Struct:
			break Loop
		default:
			return nil, NewError2(TypeError, "The type is not supported as a row struct.", map[string]interface{}{
				"type": reflect.TypeOf(v).Name(),
			})
		}
	}
	rowStructsMutex.Lock()
	defer rowStructsMutex.Unlock()
	if rs, ok := rowStructs[typ]; ok {
		return rs, nil
	}
	var cfs []*ColumnField
	cfsByName := make(map[string]*ColumnField)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if len(field.PkgPath) != 0 { // Skip unexported fields.
			continue
		}
		if field.Tag.Get(columnFieldTagKey) == "" { // Skip untagged fields.
			continue
		}
		cf, err := newColumnField(&field, i)
		if err != nil {
			return nil, err
		}
		if cf.Name == "_key" {
			cfs = append([]*ColumnField{cf}, cfs...)
		} else {
			cfs = append(cfs, cf)
		}
		if _, ok := cfsByName[cf.Name]; ok {
			return nil, NewError2(TypeError, "The name appears more than once.", map[string]interface{}{
				"name": cf.Name,
			})
		}
		cfsByName[cf.Name] = cf
	}
	rs := &RowStruct{
		Columns:       cfs,
		ColumnsByName: cfsByName,
	}
	rowStructs[typ] = rs
	return rs, nil
}
