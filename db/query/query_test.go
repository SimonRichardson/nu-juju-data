package query

import (
	"testing"
)

func TestParseNames(t *testing.T) {
	names, err := parseNames("SELECT * FROM :name, @age;", 0)
	assertNil(t, err)
	assertEquals(t, names, []string{"age", "name"})
}
