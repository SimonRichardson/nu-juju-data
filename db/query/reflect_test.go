package query

import (
	"reflect"
	"testing"
)

func TestReflect(t *testing.T) {
	s := struct {
		ID   int64  `db:"id"`
		Name string `db:"name,omitempty"`
	}{}
	info, err := Reflect(reflect.ValueOf(&s))
	assertNil(t, err)

	structMap, ok := info.(ReflectStruct)
	assertEquals(t, ok, true)

	assertTrue(t, len(structMap.Fields) == 2)
	assertEquals(t, structMap.FieldNames(), []string{"id", "name"})
}
