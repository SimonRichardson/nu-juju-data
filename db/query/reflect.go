package query

import (
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/juju/errors"
)

type ReflectTag struct {
	Name      string
	OmitEmpty bool
}

type ReflectField struct {
	Name        string
	Tag         ReflectTag
	StructField reflect.Value
}

type ReflectStruct struct {
	Fields map[string]ReflectField
}

// FieldNames returns the field names for a given type.
func (r ReflectStruct) FieldNames() []string {
	names := make([]string, 0, len(r.Fields))
	for name := range r.Fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Reflect parses a reflect.Value returning a ReflectStruct of fields and tags
// for the reflect value.
func Reflect(value reflect.Value) (ReflectStruct, error) {
	// Dereference the pointer if it is one.
	value = reflect.Indirect(value)
	mustBe(value, reflect.Struct)

	refStruct := ReflectStruct{
		Fields: make(map[string]ReflectField),
	}

	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		rawTag := field.Tag.Get("db")
		tag, err := parseTag(rawTag)
		if err != nil {
			return ReflectStruct{}, errors.Trace(err)
		}

		name := tag.Name
		if name == "" {
			name = strings.ToLower(field.Name)
		}

		refStruct.Fields[name] = ReflectField{
			Name:        field.Name,
			Tag:         tag,
			StructField: value.Field(i),
		}
	}

	return refStruct, nil
}

func parseTag(tag string) (ReflectTag, error) {
	if tag == "" {
		return ReflectTag{}, errors.Errorf("unexpected empty tag")
	}

	var refTag ReflectTag
	options := strings.Split(tag, ",")
	switch len(options) {
	case 2:
		if strings.ToLower(options[1]) != "omitempty" {
			return ReflectTag{}, errors.Errorf("unexpected tag value %q", options[1])
		}
		refTag.OmitEmpty = true
		fallthrough
	case 1:
		refTag.Name = options[0]
	}
	return refTag, nil
}

type kinder interface {
	Kind() reflect.Kind
}

// mustBe checks a value against a kind, panicing with a reflect.ValueError
// if the kind isn't that which is required.
func mustBe(v kinder, expected reflect.Kind) {
	if k := v.Kind(); k != expected {
		panic(&reflect.ValueError{Method: methodName(), Kind: k})
	}
}

// methodName returns the caller of the function calling methodName
func methodName() string {
	pc, _, _, _ := runtime.Caller(2)
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "unknown method"
	}
	return f.Name()
}
