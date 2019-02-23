package mysql

import (
	"fmt"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
)

// https://github.com/mysqljs/sqlstring/blob/master/lib/SqlString.js

var idGlobalReplacer = strings.NewReplacer("`", "``")
var qualGlobalReplacer = strings.NewReplacer(".", "`.`")
var slashGlobalReplacer = strings.NewReplacer(`\`, `\\`)
var charsGlobalReplacer = strings.NewReplacer("\x00", `\0`, "\b", `\b`, "\t", `\t`, "\n", `\n`, "\r", `\r`, "\x1a", `\Z`, `"`, `\"`, `'`, `\'`)

// EscapeID escapes mysql field
func EscapeID(val string, forbidQualified bool) string {
	if forbidQualified {
		return "`" + idGlobalReplacer.Replace(val) + "`"
	}
	return "`" + qualGlobalReplacer.Replace(idGlobalReplacer.Replace(val)) + "`"
}

// EscapeIDs escapes mysql fields
func EscapeIDs(val []string, forbidQualified bool) string {
	var sql = ""

	for i, v := range val {
		if i != 0 {
			sql += `, `
		}
		sql += EscapeID(v, forbidQualified)
	}
	return sql
}

/* Danger functions */

// Escape escapes mysql value
func Escape(val interface{}, stringifyObjects bool) (str string, err error) {
	defer func() {
		err2 := recover()
		if err == nil && err2 != nil {
			err = fmt.Errorf("Panic: %v\r\n%s", err2, debug.Stack())
		}
	}()

	// Check nil
	if val == nil {
		return "NULL", nil
	}

	// force escape string
	if stringifyObjects {
		valStr, err := asString(val)
		if err != nil {
			return "", err
		}
		return escapeString(valStr), nil
	}

	// check string
	switch v := val.(type) {
	case string:
		return escapeString(v), nil
	case []byte:
		return escapeString(string(v)), nil
	case fmt.Stringer:
		return escapeString(v.String()), nil
	}

	// check pointer
	rv := reflect.ValueOf(val)
	rvKind := rv.Kind()
	for rvKind == reflect.Ptr || rvKind == reflect.Interface {
		rv = rv.Elem()
		rvKind = rv.Kind()
	}

	switch rvKind {
	// array
	case reflect.Array, reflect.Slice:
		return arrayToList(rv)
	// map
	case reflect.Struct, reflect.Map:
		return objectToValues(rv)
	// other
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10), nil
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64), nil
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32), nil
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool()), nil
	}
	return "", fmt.Errorf("can not escape %v", val)
}

func arrayToList(array reflect.Value) (string, error) {
	var sql = ""

	for i := 0; i < array.Len(); i++ {
		val := array.Index(i)
		valType := val.Kind()
		for valType == reflect.Interface || valType == reflect.Ptr {
			val = val.Elem()
			valType = val.Kind()
		}
		if i != 0 {
			sql += `, `
		}
		if valType == reflect.Slice || valType == reflect.Array {
			valStr, err := arrayToList(val)
			if err != nil {
				return "", err
			}
			sql += `(` + valStr + `)`
		} else {
			valStr, err := Escape(val.Interface(), true)
			if err != nil {
				return "", err
			}
			sql += valStr
		}
	}

	return sql, nil
}

func objectToValues(object reflect.Value) (string, error) {
	var sql = ""

	objectKind := object.Kind()
	for objectKind == reflect.Interface || objectKind == reflect.Ptr {
		object = object.Elem()
		objectKind = object.Kind()
	}

	switch objectKind {
	case reflect.Map:
		for i, key := range object.MapKeys() {
			if i != 0 {
				sql += `, `
			}

			val := object.MapIndex(key)
			xType := val.Kind()
			for xType == reflect.Ptr || xType == reflect.Interface {
				val = val.Elem()
			}

			valStr, err := Escape(val.Interface(), true)
			if err != nil {
				return "", err
			}
			xType = key.Kind()
			for xType == reflect.Ptr || xType == reflect.Interface {
				key = key.Elem()
			}
			keyStr, err := asString(key.Interface())
			if err != nil {
				return "", err
			}
			sql += EscapeID(keyStr, false) + "=" + valStr
		}
	case reflect.Struct:
		structKeys := object.Type()
		for i := 0; i < object.NumField(); i++ {
			val := object.Field(i)
			if !val.CanInterface() {
				continue
			}
			if i != 0 {
				sql += `, `
			}
			xType := val.Kind()
			for xType == reflect.Ptr || xType == reflect.Interface {
				val = val.Elem()
			}
			valStr, err := Escape(val.Interface(), true)
			if err != nil {
				return "", err
			}
			sql += EscapeID(structKeys.Field(i).Name, false) + "=" + valStr
		}
	}

	return sql, nil
}

/* Danger functions */

func escapeString(val string) string {
	return `'` + charsGlobalReplacer.Replace(slashGlobalReplacer.Replace(val)) + `'`
}

// MySQL package

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// EscapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L823-L932
func EscapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// EscapeStringBackslash is similar to escapeBytesBackslash but for string.
func EscapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// EscapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func EscapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// EscapeStringQuotes is similar to EscapeBytesQuotes but for string.
func EscapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// BuildFieldValue build fiels and values from struct or map, prepare = "=?" or ""
func BuildFieldValue(data interface{}, prepare string) (fields []string, values []interface{}, err error) {
	defer func() {
		err2 := recover()
		if err == nil && err2 != nil {
			err = fmt.Errorf("Panic: %v\r\n%s", err2, debug.Stack())
		}
	}()
	value := reflect.ValueOf(data)
	valueType := value.Kind()
	for valueType == reflect.Ptr || valueType == reflect.Interface {
		value = value.Elem()
		valueType = value.Kind()
	}

	switch valueType {
	case reflect.Map:
		for _, key := range value.MapKeys() {
			keyStr, err := asString(key.Interface())
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, EscapeID(keyStr, true)+prepare)
			values = append(values, value.MapIndex(key).Interface())
		}
	case reflect.Struct:
		structKeys := value.Type()
		for i := 0; i < value.NumField(); i++ {
			val := value.Field(i)
			if !val.CanInterface() {
				continue
			}
			fields = append(fields, EscapeID(structKeys.Field(i).Name, true)+prepare)
			values = append(values, val.Interface())
		}
	} // switch
	return
}

func asString(src interface{}) (string, error) {
	if src == nil {
		return "NULL", nil
	}

	switch v := src.(type) {
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case fmt.Stringer:
		return v.String(), nil
	}

	rv := reflect.ValueOf(src)
	rvKind := rv.Kind()
	for rvKind == reflect.Interface || rvKind == reflect.Ptr {
		rv = rv.Elem()
		rvKind = rv.Kind()
	}

	switch rvKind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10), nil
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 64), nil
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'g', -1, 32), nil
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool()), nil
	case reflect.String:
		return rv.String(), nil
	}
	return "", fmt.Errorf("can not convert %v to string", src)
}
