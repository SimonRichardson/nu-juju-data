package query

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestParseNames(t *testing.T) {
	names, err := parseNames("SELECT :name FROM @table WHERE $id=1 AND ?42=2 AND ?=3;", 0)
	assertNil(t, err)
	assertEquals(t, names, []bind{
		{'?', "42"},
		{'$', "id"},
		{':', "name"},
		{'@', "table"},
	})
}

func TestConstructNamedArgsWithMap(t *testing.T) {
	namedArgs, err := constructNamedArgs(map[string]interface{}{
		"name": "meshuggah",
		"age":  42,
	}, []bind{
		{':', "name"},
		{'@', "age"},
	})
	assertNil(t, err)
	assertEquals(t, namedArgs, []sql.NamedArg{
		{Name: "name", Value: "meshuggah"},
		{Name: "age", Value: 42},
	})
}

func TestConstructNamedArgsWithStruct(t *testing.T) {
	arg := struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}{
		Name: "meshuggah",
		Age:  42,
	}
	namedArgs, err := constructNamedArgs(arg, []bind{
		{':', "name"},
		{'@', "age"},
	})
	assertNil(t, err)
	assertEquals(t, namedArgs, []sql.NamedArg{
		{Name: "name", Value: "meshuggah"},
		{Name: "age", Value: 42},
	})
}

func TestQueryWithMap(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assertNil(t, err)

	_, err = db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

	tx, err := db.Begin()
	assertNil(t, err)

	err = Query{}.Query(tx, "SELECT name, age FROM test WHERE name=:name;", map[string]interface{}{
		"name": "fred",
	})
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
}

func TestQueryWithStruct(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assertNil(t, err)

	_, err = db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

	tx, err := db.Begin()
	assertNil(t, err)

	arg := struct {
		Name string `db:"name"`
	}{
		Name: "fred",
	}
	type Person = struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}

	querier := NewQuerier()

	var person Person
	getter, err := querier.Get(&person)
	assertNil(t, err)

	err = getter.Query(tx, "SELECT {Person} FROM test WHERE test.name=:name;", arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)

	assertEquals(t, person, Person{Name: "fred", Age: 21})
}
