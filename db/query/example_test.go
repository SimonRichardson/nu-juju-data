package query_test

import (
	"database/sql"
	"fmt"

	"github.com/SimonRichardson/nu-juju-data/db/query"
)

func ExampleQuerier_Hook() {
	querier := query.NewQuerier()
	querier.Hook(func(s string) {
		fmt.Println(s)
	})
}

func ExampleQuerier_ForMany() {
	type Person struct {
		Name string `db:"name"`
	}
	var persons []Person

	querier := query.NewQuerier()
	query, _ := querier.ForMany(&persons)
	query.Query(&sql.Tx{}, `SELECT {person INTO Person} FROM person;`)
}

func ExampleQuerier_ForOne() {
	type Person struct {
		Name string `db:"name"`
	}
	var person Person

	querier := query.NewQuerier()
	query, _ := querier.ForOne(&person)
	query.Query(&sql.Tx{}, `SELECT {person INTO Person} FROM person WHERE name=:name;`, map[string]interface{}{
		"name": "fred",
	})
}

func ExampleQuerier_Exec() {
	type Person struct {
		Name string `db:"name"`
		Age  int    `db:"age"`
	}

	person := Person{
		Name: "fred",
		Age:  21,
	}

	querier := query.NewQuerier()
	querier.Exec(&sql.Tx{}, "INSERT INTO test(name, age) VALUES (:name, :age);", person)
}
