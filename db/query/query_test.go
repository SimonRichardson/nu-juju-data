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
	type Person struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var person Person
	getter, err := querier.ForOne(&person)
	assertNil(t, err)

	err = getter.Query(tx, `SELECT {Person "test"} FROM test WHERE test.name=:name;`, arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
	assertEquals(t, person, Person{Name: "fred", Age: 21})

	expected := "SELECT test.age, test.name FROM test WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestExpandFields(t *testing.T) {
	stmt := "SELECT {Person}, {Other}, {Another} FROM test WHERE test.name=:name;"
	fields := []fieldBind{{
		name:  "Person",
		start: 7,
		end:   15,
	}, {
		name:  "Other",
		start: 17,
		end:   24,
	}, {
		name:  "Another",
		start: 26,
		end:   35,
	}}
	entities := []ReflectStruct{{
		Name: "Person",
		Fields: map[string]ReflectField{
			"name": {},
			"age":  {},
		},
	}, {
		Name: "Other",
		Fields: map[string]ReflectField{
			"x": {},
		},
	}, {
		Name: "Another",
		Fields: map[string]ReflectField{
			"y": {},
			"z": {},
		},
	}}

	res, err := expandFields(stmt, fields, entities)
	assertNil(t, err)

	expected := "SELECT age, name, x, y, z FROM test WHERE test.name=:name;"
	assertEquals(t, res, expected)
}
