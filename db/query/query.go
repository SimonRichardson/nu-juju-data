package query

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"github.com/juju/errors"
)

type Querier struct {
	reflect *ReflectCache
}

// Select creates a query for a set of given types.
// It should be noted that the select can be cached and the query can be called
// multiple times.
func (q *Querier) Select(values ...interface{}) Query {
	entities := make([]ReflectStruct, len(values))
	for i, value := range values {
		var err error
		if entities[i], err = q.reflect.Reflect(value); err != nil {
			// Should we panic here?
			panic(err)
		}
	}

	return Query{
		entities: entities,
	}
}

type Query struct {
	entities []ReflectStruct
}

func (q Query) Query(tx *sql.Tx, stmt string, args ...interface{}) error {
	// 1. If the query contains named arguments, extract all the names.
	// 2. If the query contains names:
	//    a. Use the first argument as the source of the names.
	//    b. If the first argument is not a map or a struct{} then error out.
	//    c. If nothing matches error out to be helpful.
	//    d. Supply the additional arguments to the query.
	// 3. No names with in the query, pass all arguments to the query.
	var names []string
	if offset := indexOfNamedArgs(stmt); offset >= 0 {
		var err error
		if names, err = parseNames(stmt, offset); err != nil {
			return errors.Trace(err)
		}
	}

	// Ensure we have arguments if we have names.
	if len(args) == 0 && len(names) > 0 {
		return errors.Errorf("expected arguments for named parameters")
	}

	var inputs []sql.NamedArg
	if len(args) >= 1 {
		// Select the first argument and check if it's a map or struct.
		var err error
		if inputs, err = constructNamedArgs(args[0], names); err != nil {
			return errors.Trace(err)
		}
	}

	fmt.Println(inputs)

	return nil
}

var prefixes = map[rune]struct{}{
	':': {},
	'@': {},
	'$': {},
}

// indexOfNamedArgs returns the potential starting index of a named argument
// with  if the statement contains the named args
// prefix.
// This can return a false positives.
func indexOfNamedArgs(stmt string) int {
	for prefix := range prefixes {
		if index := strings.IndexRune(stmt, prefix); index >= 0 {
			return index
		}
	}
	return -1
}

// parseNames extracts the named arguments from a given statement.
//
// Spec: https://www.sqlite.org/c3ref/bind_blob.html
//
// Literals may be replaced by a parameter that matches one of following
// templates:
//  - ?
//  - ?NNN
//  - :VVV
//  - @VVV
//  - $VVV
// In the templates above:
//  - NNN represents an integer literal
//  - VVV represents an alphanumeric identifier.
//
func parseNames(stmt string, offset int) ([]string, error) {
	var names []string

	// Use the offset to jump ahead of the statement.
	for i := offset; i < len(stmt); i++ {
		r := rune(stmt[i])
		if _, ok := prefixes[r]; ok {
			// Consume the following runes, until you locate a breaking value.
			var name string
			for i = i + 1; i < len(stmt); i++ {
				r = rune(stmt[i])

				if unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsNumber(r) {
					name += string(r)
					continue
				}
				if unicode.IsSpace(r) || r == ',' || r == ';' {
					break
				}
				return nil, errors.Errorf("unexpected named argument found in statement %q", stmt)
			}
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func constructNamedArgs(arg interface{}, names []string) ([]sql.NamedArg, error) {
	return nil, nil
}
