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
	namedArgs, err := constructInputNamedArgs(map[string]interface{}{
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

func TestConstructInputNamedArgsWithStruct(t *testing.T) {
	arg := struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}{
		Name: "meshuggah",
		Age:  42,
	}
	namedArgs, err := constructInputNamedArgs(arg, []nameBinding{
		{':', "name"},
		{'@', "age"},
	})
	assertNil(t, err)
	assertEquals(t, namedArgs, []sql.NamedArg{
		{Name: "name", Value: "meshuggah"},
		{Name: "age", Value: 42},
	})
}

func TestExecWithMap(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
	`)
	assertNil(t, err)

	var processedStmts []string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmts = append(processedStmts, stmt)
	})

	runTx(t, db, func(tx *sql.Tx) error {
		_, err := querier.Exec(tx, "INSERT INTO test(name, age) VALUES (:name, :age);", map[string]interface{}{
			"name": "fred",
			"age":  21,
		})
		return err
	})

	person := make(map[string]interface{})

	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, "SELECT name, age FROM test WHERE name=:name;", map[string]interface{}{
			"name": "fred",
		})
	})

	assertEquals(t, person, map[string]interface{}{
		"name": "fred",
		"age":  int64(21),
	})

	expected := []string{
		"INSERT INTO test(name, age) VALUES (:name, :age);",
		"SELECT name, age FROM test WHERE name=:name;",
	}
	assertEquals(t, processedStmts, expected)
}

func TestExecWithStruct(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
	`)
	assertNil(t, err)

	var processedStmts []string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmts = append(processedStmts, stmt)
	})

	type Person struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}

	runTx(t, db, func(tx *sql.Tx) error {
		person := Person{
			Name: "fred",
			Age:  21,
		}
		_, err := querier.Exec(tx, "INSERT INTO test(name, age) VALUES (:name, :age);", person)
		return err
	})

	var person Person

	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, "SELECT name, age FROM test WHERE name=:name;", map[string]interface{}{
			"name": "fred",
		})
	})

	assertEquals(t, person, Person{
		Name: "fred",
		Age:  21,
	})

	expected := []string{
		"INSERT INTO test(name, age) VALUES (:name, :age);",
		"SELECT name, age FROM test WHERE name=:name;",
	}
	assertEquals(t, processedStmts, expected)
}

func TestQueryWithMap(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	person := make(map[string]interface{})

	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, "SELECT name, age FROM test WHERE name=:name;", map[string]interface{}{
			"name": "fred",
		})
	})

	assertEquals(t, person, map[string]interface{}{
		"name": "fred",
		"age":  int64(21),
	})

	expected := "SELECT name, age FROM test WHERE name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithScalar(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var count int
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&count)
		assertNil(t, err)

		return getter.Query(tx, "SELECT COUNT(name) FROM test WHERE name=:name;", map[string]interface{}{
			"name": "fred",
		})

	})

	assertEquals(t, count, 1)

	expected := "SELECT COUNT(name) FROM test WHERE name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithScalarAndName(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

	var processedStmt string

	querier := NewQuerier()
	querier.Hook(func(stmt string) {
		processedStmt = stmt
	})

	var count int
	var name string

	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&count, &name)
		assertNil(t, err)

		return getter.Query(tx, "SELECT COUNT(name), name FROM test WHERE name=:name;", map[string]interface{}{
			"name": "fred",
		})
	})

	assertEquals(t, count, 1)
	assertEquals(t, name, "fred")

	expected := "SELECT COUNT(name), name FROM test WHERE name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithStruct(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {test.name, test.age INTO Person} FROM test WHERE test.name=:name;`, map[string]interface{}{
			"name": "fred",
		})
	})

	assertEquals(t, person, Person{Name: "fred", Age: 21})

	expected := "SELECT test.age, test.name FROM test WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestPartialNameQueryWithStruct(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {test.name INTO Person} FROM test WHERE test.name=:name;`, map[string]interface{}{
			"name": "fred",
		})
	})

	assertEquals(t, person, Person{Name: "fred", Age: 0})

	expected := "SELECT test.name FROM test WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestWildcardQueryWithStruct(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {test.* INTO Person} FROM test WHERE test.name=:name;`, map[string]interface{}{
			"name": "fred",
		})
	})

	assertEquals(t, person, Person{Name: "fred", Age: 21})

	expected := "SELECT test.age, test.name FROM test WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithStructUsesCache(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		arg := struct {
			Name string `db:"name"`
		}{
			Name: "fred",
		}

		if err := getter.Query(tx, `SELECT {test.* INTO Person} FROM test WHERE test.name=:name;`, arg); err != nil {
			return err
		}

		return getter.Query(tx, `SELECT {test.* INTO Person} FROM test WHERE test.name=:name;`, arg)
	})

	assertEquals(t, person, Person{Name: "fred", Age: 21})

	expected := "SELECT test.age, test.name FROM test WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)

	_, ok := querier.stmtCache.Get(`SELECT {test.* INTO Person} FROM test WHERE test.name=:name;`)
	assertEquals(t, ok, true)
}

func TestQueryWithStructUsesCacheOverNumerousTx(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {test.* INTO Person} FROM test WHERE test.name=:name;`, struct {
			Name string `db:"name"`
		}{
			Name: "fred",
		})
	})
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {test.* INTO Person} FROM test WHERE test.name=:name;`, struct {
			Name string `db:"name"`
		}{
			Name: "fred",
		})
	})

	assertEquals(t, person, Person{Name: "fred", Age: 21})

	expected := "SELECT test.age, test.name FROM test WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)

	_, ok := querier.stmtCache.Get(`SELECT {test.* INTO Person} FROM test WHERE test.name=:name;`)
	assertEquals(t, ok, true)
}

func TestQueryWithStructOverlapping(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person, &record)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {"test.*" INTO Person}, {"sqlite_master.*" INTO Record} FROM test,sqlite_master WHERE test.name=:name;`, arg)
	})

	assertEquals(t, person, Person{Name: "fred", Age: 21})
	assertEquals(t, record, Record{Name: "test"})

	expected := "SELECT test.age, test.name AS _pfx_test_sfx_name, sqlite_master.name AS _pfx_sqlite_master_sfx_name FROM test,sqlite_master WHERE test.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryJoinWithStruct(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {Person} FROM people INNER JOIN location ON people.location=location.id WHERE location.id=:loc_id AND people.name=:name;`, struct {
			Name       string `db:"name"`
			LocationID int    `db:"loc_id"`
		}{
			Name:       "fred",
			LocationID: 1,
		})
	})
	assertEquals(t, person, Person{Name: "fred", Age: 21, City: "london"})

	expected := "SELECT age, city, name FROM people INNER JOIN location ON people.location=location.id WHERE location.id=:loc_id AND people.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryJoinWithMultipleStructs(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person, &location)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {Person}, {Location} FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;`, struct {
			Name       string `db:"name"`
			LocationID int    `db:"loc_id"`
		}{
			Name:       "fred",
			LocationID: 1,
		})
	})
	assertEquals(t, person, Person{Name: "fred", Age: 21})
	assertEquals(t, location, Location{City: "london"})

	expected := "SELECT age, name, city FROM people INNER JOIN location WHERE location.id=:loc_id AND people.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryJoinWithMultiplePrefixStructs(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForOne(&person, &location)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {people.* INTO Person}, {location.* INTO Location} FROM people INNER JOIN location ON people.location=location.id WHERE location.id=:loc_id AND people.name=:name;`, struct {
			Name       string `db:"name"`
			LocationID int    `db:"loc_id"`
		}{
			Name:       "fred",
			LocationID: 1,
		})
	})
	assertEquals(t, person, Person{Name: "fred", Age: 21})
	assertEquals(t, location, Location{City: "london"})

	expected := "SELECT people.age, people.name, location.city FROM people INNER JOIN location ON people.location=location.id WHERE location.id=:loc_id AND people.name=:name;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithSlice(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
CREATE TABLE test(
	name TEXT,
	age  INTEGER
);
INSERT INTO test(name, age) values ("fred", 21), ("frank", 42);
	`)
	assertNil(t, err)

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
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForMany(&persons)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {test.* INTO Person} FROM test WHERE test.age>:age;`, struct {
			Age int `db:"age"`
		}{
			Age: 20,
		})
	})
	assertNil(t, err)
	assertEquals(t, persons, []Person{
		{Name: "fred", Age: 21},
		{Name: "frank", Age: 42},
	})

	expected := "SELECT test.age, test.name FROM test WHERE test.age>:age;"
	assertEquals(t, processedStmt, expected)
}

func TestQueryWithSliceMultipleItems(t *testing.T) {
	db := setupDB(t)

	_, err := db.Exec(`
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

	var persons []Person
	var locations []Location
	runTx(t, db, func(tx *sql.Tx) error {
		getter, err := querier.ForMany(&persons, &locations)
		assertNil(t, err)

		return getter.Query(tx, `SELECT {people.* INTO Person}, {location.* INTO Location} FROM people INNER JOIN location ON people.location=location.id WHERE location=:loc_id AND people.age>:age;`, struct {
			Age        int `db:"age"`
			LocationID int `db:"loc_id"`
		}{
			Age:        20,
			LocationID: 1,
		})
	})
	assertNil(t, err)
	assertEquals(t, persons, []Person{
		{Name: "fred", Age: 21},
		{Name: "jane", Age: 23},
	})

	expected := "SELECT people.age, people.name, location.city FROM people INNER JOIN location ON people.location=location.id WHERE location=:loc_id AND people.age>:age;"
	assertEquals(t, processedStmt, expected)
}

func TestParseRecords(t *testing.T) {
	stmt := `SELECT {test.*, test.name, test.age INTO Person} FROM test WHERE test.name=:name;`
	bindings, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertNil(t, err)
	assertEquals(t, bindings, []recordBinding{{
		name:     "Person",
		prefix:   "test",
		fields:   map[string]struct{}{"*": {}, "name": {}, "age": {}},
		wildcard: true,
		start:    7,
		end:      48,
	}})
}

func TestParseMultipleRecords(t *testing.T) {
	stmt := `SELECT {test.*, test.name, test.age INTO Person}, {'foo.*' INTO Foo}, {"other.*" INTO Other}, {Another} FROM test WHERE test.name=:name;`
	bindings, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertNil(t, err)
	assertEquals(t, bindings, []recordBinding{{
		name:     "Person",
		prefix:   "test",
		fields:   map[string]struct{}{"*": {}, "name": {}, "age": {}},
		wildcard: true,
		start:    7,
		end:      48,
	}, {
		name:     "Foo",
		prefix:   "foo",
		fields:   map[string]struct{}{"*": {}},
		wildcard: true,
		start:    50,
		end:      68,
	}, {
		name:     "Other",
		prefix:   "other",
		fields:   map[string]struct{}{"*": {}},
		wildcard: true,
		start:    70,
		end:      92,
	}, {
		name:     "Another",
		prefix:   "",
		fields:   map[string]struct{}{},
		wildcard: true,
		start:    94,
		end:      103,
	}})
}

func TestParseRecordsErrorsMissingINTO(t *testing.T) {
	stmt := `SELECT {test Person} FROM test WHERE test.name=:name;`
	_, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertEquals(t, err.Error(), `unexpected record expression "test Person"`)
}

func TestParseRecordsErrorsMissingMatchingQuote(t *testing.T) {
	stmt := `SELECT {'test.name INTO Person} FROM test WHERE test.name=:name;`
	_, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertEquals(t, err.Error(), `missing quote "'" terminator for record expression "test.name INTO Person"`)
}

func TestParseRecordsErrorsTooMuchInformation(t *testing.T) {
	stmt := `SELECT {test INTO Person AS} FROM test WHERE test.name=:name;`
	_, err := parseRecords(stmt, indexOfRecordArgs(stmt))
	assertEquals(t, err.Error(), `unexpected record expression "test INTO Person AS"`)
}

func TestExpandFields(t *testing.T) {
	stmt := `SELECT {test.* INTO Person}, {x INTO Other}, {y INTO Another} FROM test WHERE test.name=:name;`

	fields := []recordBinding{{
		name:     "Person",
		wildcard: true,
		start:    7,
		end:      27,
		prefix:   "test",
	}, {
		name: "Other",
		fields: map[string]struct{}{
			"x": {},
		},
		start: 29,
		end:   43,
	}, {
		name: "Another",
		fields: map[string]struct{}{
			"y": {},
		},
		start: 45,
		end:   61,
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

	expected := "SELECT test.age, test.name AS _pfx_test_sfx_name, x, y FROM test WHERE test.name=:name;"
	assertEquals(t, res, expected)
}
