package query

import (
	"database/sql"
	"reflect"
	"runtime/debug"

	"github.com/juju/errors"
)

type tester interface {
	// Log formats its arguments using default formatting, analogous to Println,
	// and records the text in the error log. For tests, the text will be printed only if
	// the test fails or the -test.v flag is set. For benchmarks, the text is always
	// printed to avoid having performance depend on the value of the -test.v flag.
	Log(args ...interface{})

	// Fatal is equivalent to Log followed by FailNow.
	Fatal(args ...interface{})

	// Fatalf is equivalent to Logf followed by FailNow.
	Fatalf(format string, args ...interface{})
}

func assertNil(t tester, err error) {
	if err != nil {
		t.Log(errors.ErrorStack(err))
		t.Log(string(debug.Stack()))
		t.Fatal(err)
	}
}

func assertTrue(t tester, value bool) {
	if !value {
		t.Fatalf("expected value to be true")
	}
}

func assertEquals(t tester, a, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("expected %v to equal %v", a, b)
	}
}

func setupDB(t tester) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	assertNil(t, err)
	return db
}

func runTx(t tester, db *sql.DB, fn func(*sql.Tx) error) {
	tx, err := db.Begin()
	assertNil(t, err)

	if err := fn(tx); err != nil {
		tx.Rollback()
		assertNil(t, err)
	}

	err = tx.Commit()
	assertNil(t, err)
}
