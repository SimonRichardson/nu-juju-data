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

	err = getter.Query(tx, `SELECT {test INTO Person} FROM test WHERE test.name=:name;`, arg)
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

	err = getter.Query(tx, `SELECT {"test" INTO Person}, {"sqlite_master" INTO Record} FROM test,sqlite_master WHERE test.name=:name;`, arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
	assertEquals(t, person, Person{Name: "fred", Age: 21})
	assertEquals(t, record, Record{Name: "test"})

	expected := "SELECT test.age, test.name AS _pfx_test_sfx_name, sqlite_master.name AS _pfx_sqlite_master_sfx_name FROM test,sqlite_master WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryJoinWithStruct(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assertNil(t, err)

	_, err = db.Exec(`
CREATE TABLE people(
	name     TEXT,
	age      INTEGER,
	location INTEGER
);
CREATE TABLE location(
	id   INTEGER,
	city TEXT
);
INSERT INTO people(name, age, location) values ("fred", 21, 1), ("frank", 42, 2), ("jane", 23, 1);
INSERT INTO location(id, city) values (1, "london"), (2, "paris");
	`)
	assertNil(t, err)

	tx, err := db.Begin()
	assertNil(t, err)

	arg := struct {
		Name       string `db:"name"`
		LocationID int    `db:"loc_id"`
	}{
		Name:       "fred",
		LocationID: 1,
	}
	type Person struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
		City string `db:"city"`
	}

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var person Person
	getter, err := querier.ForOne(&person)
	assertNil(t, err)

	err = getter.Query(tx, `SELECT {Person} FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;`, arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
	assertEquals(t, person, Person{Name: "fred", Age: 21, City: "london"})

	expected := "SELECT age, city, name FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryJoinWithMultipleStructs(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assertNil(t, err)

	_, err = db.Exec(`
CREATE TABLE people(
	name     TEXT,
	age      INTEGER,
	location INTEGER
);
CREATE TABLE location(
	id   INTEGER,
	city TEXT
);
INSERT INTO people(name, age, location) values ("fred", 21, 1), ("frank", 42, 2), ("jane", 23, 1);
INSERT INTO location(id, city) values (1, "london"), (2, "paris");
	`)
	assertNil(t, err)

	tx, err := db.Begin()
	assertNil(t, err)

	arg := struct {
		Name       string `db:"name"`
		LocationID int    `db:"loc_id"`
	}{
		Name:       "fred",
		LocationID: 1,
	}
	type Person struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}
	type Location struct {
		City string `db:"city"`
	}

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var person Person
	var location Location
	getter, err := querier.ForOne(&person, &location)
	assertNil(t, err)

	err = getter.Query(tx, `SELECT {Person}, {Location} FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;`, arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
	assertEquals(t, person, Person{Name: "fred", Age: 21})
	assertEquals(t, location, Location{City: "london"})

	expected := "SELECT age, name, city FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryJoinWithMultiplePrefixStructs(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	assertNil(t, err)

	_, err = db.Exec(`
CREATE TABLE people(
	name     TEXT,
	age      INTEGER,
	location INTEGER
);
CREATE TABLE location(
	id   INTEGER,
	city TEXT
);
INSERT INTO people(name, age, location) values ("fred", 21, 1), ("frank", 42, 2), ("jane", 23, 1);
INSERT INTO location(id, city) values (1, "london"), (2, "paris");
	`)
	assertNil(t, err)

	tx, err := db.Begin()
	assertNil(t, err)

	arg := struct {
		Name       string `db:"name"`
		LocationID int    `db:"loc_id"`
	}{
		Name:       "fred",
		LocationID: 1,
	}
	type Person struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}
	type Location struct {
		City string `db:"city"`
	}

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var person Person
	var location Location
	getter, err := querier.ForOne(&person, &location)
	assertNil(t, err)

	err = getter.Query(tx, `SELECT {people INTO Person}, {location INTO Location} FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;`, arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
	assertEquals(t, person, Person{Name: "fred", Age: 21})
	assertEquals(t, location, Location{City: "london"})

	expected := "SELECT people.age, people.name, location.city FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithSlice(t *testing.T) {
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
		Age int `db:"age"`
	}{
		Age: 20,
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

	var persons []Person
	getter, err := querier.ForMany(&persons)
	assertNil(t, err)

	err = getter.Query(tx, `SELECT {test INTO Person} FROM test WHERE test.age>:age;`, arg)
	assertNil(t, err)

	err = tx.Commit()
	assertNil(t, err)
	assertEquals(t, persons, []Person{
		{Name: "fred", Age: 21},
		{Name: "frank", Age: 42},
	})

	expected := "SELECT test.age, test.name FROM test WHERE test.age>:age;"
	assertEquals(t, processedStmt, expected)
}

func TestParseRecords(t *testing.T) {
	stmt := `SELECT {test INTO Person}, {'foo' INTO Foo}, {"other" INTO Other}, {Another} FROM test WHERE test.name=:name;`
	bindings, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertNil(t, err)
	assertEquals(t, bindings, []recordBinding{{
		name:   "Person",
		prefix: "test",
		start:  7,
		end:    25,
	}, {
		name:   "Foo",
		prefix: "foo",
		start:  27,
		end:    43,
	}, {
		name:   "Other",
		prefix: "other",
		start:  45,
		end:    65,
	}, {
		name:   "Another",
		prefix: "",
		start:  67,
		end:    76,
	}})
}

func TestParseRecordsErrorsMissingINTO(t *testing.T) {
	stmt := `SELECT {test Person} FROM test WHERE test.name=:name;`
	_, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertEquals(t, err.Error(), `unexpected record statement "test Person"`)
}

func TestParseRecordsErrorsMissingMatchingQuote(t *testing.T) {
	stmt := `SELECT {'test INTO Person} FROM test WHERE test.name=:name;`
	_, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertEquals(t, err.Error(), `missing quote "'" terminator for record statement "test INTO Person"`)
}

func TestParseRecordsErrorsTooMuchInformation(t *testing.T) {
	stmt := `SELECT {test INTO Person AS} FROM test WHERE test.name=:name;`
	_, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertEquals(t, err.Error(), `unexpected record statement "test INTO Person AS"`)
}

func TestExpandFields(t *testing.T) {
	stmt := `SELECT {Person}, {Other}, {Another} FROM test WHERE test.name=:name;`

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
