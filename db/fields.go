package db

import (
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

type FieldsSlice []string

func (f FieldsSlice) Join() string {
	return strings.Join(f, ", ")
}

// FieldNames returns the current list of fields associated with a type. By using
// the sqlx.Tx, we can use the existing reflection mapper functionality to
// get the field names back.
func FieldNames(tx *sqlx.Tx, arg interface{}) (FieldsSlice, error) {
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return nil, errors.NotSupportedf("map type: %T", arg)
		}
		fields := make(FieldsSlice, 0, len(m))
		for field := range m {
			fields = append(fields, field)
		}
		return fields, nil

	case k == reflect.Array || k == reflect.Slice:
		return nil, errors.NotSupportedf("%q not supported", k.String())
	default:
		props := tx.Mapper.FieldMap(reflect.ValueOf(arg))
		fields := make(FieldsSlice, 0, len(props))
		for field := range props {
			if strings.ContainsRune(field, '.') {
				continue
			}
			fields = append(fields, field)
		}
		return fields, nil
	}

}

// convertMapStringInterface attempts to convert v to map[string]interface{}.
// Unlike v.(map[string]interface{}), this function works on named types that
// are convertible to map[string]interface{} as well.
func convertMapStringInterface(v interface{}) (map[string]interface{}, bool) {
	var m map[string]interface{}
	mtype := reflect.TypeOf(m)
	t := reflect.TypeOf(v)
	if !t.ConvertibleTo(mtype) {
		return nil, false
	}
	return reflect.ValueOf(v).Convert(mtype).Interface().(map[string]interface{}), true

}
