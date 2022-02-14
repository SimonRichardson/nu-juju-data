package query

import (
	"reflect"
	"runtime/debug"
	"testing"

	"github.com/juju/errors"
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

func assertNil(t *testing.T, err error) {
	if err != nil {
		t.Log(errors.ErrorStack(err))
		t.Log(string(debug.Stack()))
		t.Fatal(err)
	}
}

func assertTrue(t *testing.T, value bool) {
	if !value {
		t.Fatalf("expected value to be true")
	}
}

func assertEquals(t *testing.T, a, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("expected %v to equal %v", a, b)
	}
}
