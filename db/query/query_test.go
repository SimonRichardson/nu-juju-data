package query

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestParseNames(t *testing.T) {
	names, err := parseNames("SELECT :name FROM @table WHERE $id=1 AND ?42=2 AND ?=3;", 0)
	assertNil(t, err)
	assertEquals(t, names, []nameBinding{
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
	}, []nameBinding{
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
	namedArgs, err := constructNamedArgs(arg, []nameBinding{
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

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	person := make(map[string]interface{})
	getter, err := querier.ForOne(&person)
	assertNil(t, err)

	err = getter.Query(tx, "SELECT name, age FROM test WHERE name=:name;", map[string]interface{}{
		"name": "fred",
	})
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)

	assertEquals(t, person, map[string]interface{}{
		"name": "fred",
		"age":  int64(21),
	})

	expected := "SELECT name, age FROM test WHERE name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithScalar(t *testing.T) {
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

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var count int
	getter, err := querier.ForOne(&count)
	assertNil(t, err)

	err = getter.Query(tx, "SELECT COUNT(name) FROM test WHERE name=:name;", map[string]interface{}{
		"name": "fred",
	})
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)

	assertEquals(t, count, 1)

	expected := "SELECT COUNT(name) FROM test WHERE name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithScalarAndName(t *testing.T) {
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

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var count int
	var name string
	getter, err := querier.ForOne(&count, &name)
	assertNil(t, err)

	err = getter.Query(tx, "SELECT COUNT(name), name FROM test WHERE name=:name;", map[string]interface{}{
		"name": "fred",
	})
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)

	assertEquals(t, count, 1)
	assertEquals(t, name, "fred")

	expected := "SELECT COUNT(name), name FROM test WHERE name=:name;"
	assertEquals(t, processedStmt, expected)
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

func TestQueryWithStructOverlapping(t *testing.T) {
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
	type Record struct {
		Name string `db:"name"`
	}

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var person Person
	var record Record
	getter, err := querier.ForOne(&person, &record)
	assertNil(t, err)

	err = getter.Query(tx, `SELECT {Person "test"}, {Record "sqlite_master"} FROM test,sqlite_master WHERE test.name=:name;`, arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
	assertEquals(t, person, Person{Name: "fred", Age: 21})
	assertEquals(t, record, Record{Name: "test"})

	expected := "SELECT test.age, test.name AS _pfx_test_sfx_name, sqlite_master.name AS _pfx_sqlite_master_sfx_name FROM test,sqlite_master WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestExpandFields(t *testing.T) {
	stmt := "SELECT {Person}, {Other}, {Another} FROM test WHERE test.name=:name;"

	fields := []recordBinding{{
		name:   "Person",
		start:  7,
		end:    15,
		prefix: "test",
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

	intersections := map[string]map[string]struct{}{
		"Person": {
			"name": struct{}{},
		},
	}

	res, err := expandRecords(stmt, fields, entities, intersections)
	assertNil(t, err)

	expected := "SELECT test.age, test.name AS _pfx_test_sfx_name, x, y, z FROM test WHERE test.name=:name;"
	assertEquals(t, res, expected)
}
