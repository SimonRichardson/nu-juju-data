package query

import (
	"database/sql"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/juju/errors"
)

// Hook is used to analyze the queries that are being queried.
type Hook func(string)

type Querier struct {
	reflect *ReflectCache
	hook    Hook
}

// NewQuerier creates a new querier for selecting queries.
func NewQuerier() *Querier {
	return &Querier{
		reflect: NewReflectCache(),
	}
}

// Hook assigns the hook to the querier. Each hook call precedes the actual
// query.
func (q *Querier) Hook(hook Hook) {
	q.hook = hook
}

// ForOne creates a query for a set of given types.
// It should be noted that the select can be cached and the query can be called
// multiple times.
func (q *Querier) ForOne(values ...interface{}) (Query, error) {
	entities := make([]ReflectStruct, len(values))
	names := make([]string, len(values))
	for i, value := range values {
		var err error
		if entities[i], err = q.reflect.Reflect(value); err != nil {
			return Query{}, errors.Trace(err)
		}
		if !entities[i].Ptr {
			return Query{}, errors.Errorf("expected a pointer, not a value for %d of type %T", i, value)
		}

		names[i] = entities[i].Name
	}

	return Query{
		entities: entities,
		names:    names,
		hook:     q.hook,
	}, nil
}

type Query struct {
	entities []ReflectStruct
	names    []string
	hook     Hook
}

// 1. If the query contains named arguments, extract all the names.
// 2. If the query contains names:
//    a. Use the first argument as the source of the names.
//    b. If the first argument is not a map or a struct{} then error out.
//    c. If nothing matches error out to be helpful.
//    d. Supply the additional arguments to the query.
// 3. No names with in the query, pass all arguments to the query.
func (q Query) Query(tx *sql.Tx, stmt string, args ...interface{}) error {
	var names []bind
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
	if len(names) > 0 && len(args) >= 1 {
		// Select the first argument and check if it's a map or struct.
		var err error
		if inputs, err = constructNamedArgs(args[0], names); err != nil {
			return errors.Trace(err)
		}
		// Drop the first argument, as that's used for named arguments.
		args = args[1:]
	}

	// Put the named arguments at the end of the query.
	for _, input := range inputs {
		args = append(args, input)
	}

	if offset := indexOfFieldArgs(stmt); offset >= 0 {
		fields, err := parseFields(stmt, offset)
		if err != nil {
			return errors.Trace(err)
		}
		stmt, err = expandFields(stmt, fields, q.entities)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Call the hook, before making the query.
	if q.hook != nil {
		q.hook(stmt)
	}

	rows, err := tx.Query(stmt, args...)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Grab the columns of the rows returned.
	columns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Traverse the entities available, this is where it becomes very difficult
	// for use. As the sql library doesn't provide the namespaced columns for
	// us to inspect, so if you have overlapping column names it becomes hard
	// to know where to locate that information, without a SQL AST.
	columnar := make([]interface{}, len(columns))
	for i, column := range columns {
		var found bool
		for _, entity := range q.entities {
			if _, ok := entity.Fields[column]; !ok {
				continue
			}
			columnar[i] = entity.Fields[column].Value.Addr().Interface()
			found = true
			break
		}
		if !found {
			return errors.Errorf("missing destination name %q in types %v", column, q.names)
		}
	}
	for rows.Next() {
		if err := rows.Scan(columnar...); err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(rows.Err())
}

type bindCharPredicate func(rune) bool

func alphaNumeric(a rune) bool {
	return unicode.IsLetter(a) || unicode.IsDigit(a) || unicode.IsNumber(a)
}

func numeric(a rune) bool {
	return unicode.IsDigit(a) || unicode.IsNumber(a)
}

var prefixes = map[rune]bindCharPredicate{
	':': alphaNumeric,
	'@': alphaNumeric,
	'$': alphaNumeric,
	'?': numeric,
}

// indexOfNamedArgs returns the potential starting index of a named argument
// within the statement contains the named args prefix.
// This can return a false positives.
func indexOfNamedArgs(stmt string) int {
	// Let's be explicit that we've found something, we could just use the
	// res to see if it's moved, but that's more cryptic.
	var found bool
	res := len(stmt) + 1
	for prefix := range prefixes {
		if index := strings.IndexRune(stmt, prefix); index >= 0 && index < res {
			res = index
			found = true
		}
	}
	if found {
		return res
	}
	return -1
}

type bind struct {
	prefix rune
	name   string
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
func parseNames(stmt string, offset int) ([]bind, error) {
	var names []bind

	// Use the offset to jump ahead of the statement.
	for i := offset; i < len(stmt); i++ {
		r := rune(stmt[i])
		if predicate, ok := prefixes[r]; ok {
			// We need to special case empty '?' as they're valid, but are not
			// valid binds.
			if r == '?' && i+1 < len(stmt) && isNameTerminator(rune(stmt[i+1])) {
				continue
			}

			// Consume the following runes, until you locate a breaking value.
			var name string
			for i = i + 1; i < len(stmt); i++ {
				char := rune(stmt[i])

				if predicate(char) {
					name += string(char)
					continue
				}
				if isNameTerminator(char) {
					break
				}
				return nil, errors.Errorf("unexpected named argument found in statement %q", stmt)
			}
			names = append(names, bind{
				prefix: r,
				name:   name,
			})

			// Locate the index of the next name. We use this to skip over
			// any complexities.
			if i >= len(stmt) {
				// We're done processing the stmt.
				break
			}
			index := indexOfNamedArgs(stmt[i:])
			if index == -1 {
				// No additional names, skip.
				break
			}
			// We want to reduce the index by 1, so that we also pick up the
			// prefix, otherwise we skip over it.
			i += (index - 1)
		}
	}
	sort.Slice(names, func(i int, j int) bool {
		return names[i].name < names[j].name
	})
	return names, nil
}

func isNameTerminator(a rune) bool {
	return unicode.IsSpace(a) || a == ',' || a == ';' || a == '='
}

func constructNamedArgs(arg interface{}, names []bind) ([]sql.NamedArg, error) {
	t := reflect.TypeOf(arg)
	k := t.Kind()
	switch {
	case k == reflect.Map && t.Key().Kind() == reflect.String:
		m, ok := convertMapStringInterface(arg)
		if !ok {
			return nil, errors.NotSupportedf("map type: %T", arg)
		}
		nameValues := make([]sql.NamedArg, len(names))
		for k, name := range names {
			if value, ok := m[name.name]; ok {
				nameValues[k] = sql.Named(name.name, value)
				continue
			}

			return nil, errors.Errorf("key %q missing from map", name.name)
		}
		return nameValues, nil

	case k == reflect.Array || k == reflect.Slice:
		return nil, errors.NotSupportedf("%q not supported", k.String())
	default:
		ref, err := Reflect(reflect.ValueOf(arg))
		if err != nil {
			return nil, errors.Trace(err)
		}
		nameValues := make([]sql.NamedArg, len(names))
		for k, name := range names {
			if field, ok := ref.Fields[name.name]; ok {
				fieldValue := field.Value.Interface()
				nameValues[k] = sql.Named(name.name, fieldValue)
				continue
			}

			return nil, errors.Errorf("field %q missing from type %T", name.name, arg)
		}

		return nameValues, nil
	}
}

// convertMapStringInterface attempts to convert v to map[string]interface{}.
// Unlike v.(map[string]interface{}), this function works on named types that
// are convertible to map[string]interface{} as well.
func convertMapStringInterface(v interface{}) (map[string]interface{}, bool) {
	var m map[string]interface{}
	mType := reflect.TypeOf(m)
	t := reflect.TypeOf(v)
	if !t.ConvertibleTo(mType) {
		return nil, false
	}
	return reflect.ValueOf(v).Convert(mType).Interface().(map[string]interface{}), true
}

// indexOfFieldArgs returns the potential starting index of a field argument
// if the statement contains the field args offset position.
func indexOfFieldArgs(stmt string) int {
	return strings.IndexRune(stmt, '{')
}

type fieldBind struct {
	name       string
	start, end int
}

func (f fieldBind) translate(expantion int) int {
	return expantion - (f.end - f.start)
}

func parseFields(stmt string, offset int) ([]fieldBind, error) {
	var fields []fieldBind
	for i := offset; i < len(stmt); i++ {
		r := rune(stmt[i])
		if r != '{' {
			return fields, nil
		}

		var name string
	inner:
		for i = i + 1; i < len(stmt); i++ {
			char := rune(stmt[i])

			switch {
			case unicode.IsLetter(char) || unicode.IsSpace(char):
				fallthrough

			case len(name) > 0 && unicode.IsDigit(char):
				fallthrough

			case char == '_':
				name += string(char)

			case char == '}':
				break inner

			default:
				return nil, errors.Errorf("unexpected struct name in statement")
			}
		}
		fields = append(fields, fieldBind{
			name:  name,
			start: offset,
			end:   i + 1,
		})

		if i >= len(stmt) {
			// We're done processing the stmt.
			break
		}
		index := indexOfFieldArgs(stmt[i:])
		if index == -1 {
			// No additional names, skip.
			break
		}
		// We want to reduce the index by 1, so that we also pick up the
		// prefix, otherwise we skip over it.
		offset = i + index
		i = offset - 1
	}
	return fields, nil
}

func expandFields(stmt string, fields []fieldBind, entities []ReflectStruct) (string, error) {
	var offset int
	for _, field := range fields {

		var found bool
		for _, entity := range entities {
			if field.name != entity.Name {
				continue
			}

			// We've located the entity, now swap out all of it's field names.
			fieldList := strings.Join(entity.FieldNames(), ", ")
			stmt = stmt[:offset+field.start] + fieldList + stmt[offset+field.end:]

			// Translate the offset to take into account the new expantions.
			offset += field.translate(len(fieldList))

			found = true
			break
		}

		if !found {
			return "", errors.Errorf("no entity found with the name %q", field.name)
		}
	}

	return stmt, nil
}
