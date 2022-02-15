package query

import (
	"database/sql"
	"reflect"
	"sort"
	"strings"
	"unicode"

	"github.com/juju/errors"
)

const (
	// AliasPrefix is a prefix used to decode the mappings from column name.
	AliasPrefix = "_pfx_"
	// AliasSeparator is a separator used to decode the mappings from column
	// name.
	AliasSeparator = "_sfx_"
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
	entities := make([]ReflectInfo, len(values))

	for i, value := range values {
		var err error

		if entities[i], err = q.reflect.Reflect(value); err != nil {
			return Query{}, errors.Trace(err)
		}

		// Ensure that all the types are the same. This is a current
		// restriction to reduce complications later on. Given enough time and
		// energy we can implement this at a later date.
		if i > 1 && entities[i-1].Kind() != entities[i].Kind() {
			return Query{}, errors.Errorf("expected all input values to be of the same kind %q, got %q", entities[i-1].Kind(), entities[i].Kind())
		}
	}

	query := Query{
		entities: entities,
		hook:     q.hook,
	}
	if len(values) == 0 {
		query.executePlan = query.defaultScan
		return query, nil
	}

	switch entities[0].Kind() {
	case reflect.Struct:
		structs := make([]ReflectStruct, len(values))
		for i, entity := range entities {
			structs[i] = entity.(ReflectStruct)
		}

		query.executePlan = func(tx *sql.Tx, stmt string, args []interface{}) error {
			return query.structScan(tx, stmt, args, structs)
		}

	case reflect.Map:
		if len(values) > 1 {
			return Query{}, errors.Errorf("expected one map for query, got %d", len(values))
		}
		query.executePlan = func(tx *sql.Tx, stmt string, args []interface{}) error {
			return query.mapScan(tx, stmt, args, entities[0].(ReflectValue))
		}

	default:
		query.executePlan = query.defaultScan
	}
	return query, nil
}

type Query struct {
	entities    []ReflectInfo
	hook        Hook
	executePlan func(*sql.Tx, string, []interface{}) error
}

func (q Query) Query(tx *sql.Tx, stmt string, args ...interface{}) error {
	var names []nameBinding
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

	return q.executePlan(tx, stmt, args)
}

func (q Query) defaultScan(tx *sql.Tx, stmt string, args []interface{}) error {
	rows, columns, err := q.query(tx, stmt, args)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	if len(columns) != len(q.entities) {
		return errors.Errorf("number of entities does not match column length %d, got %d", len(columns), len(q.entities))
	}

	columnar := make([]interface{}, len(columns))
	for i := range columns {
		if _, ok := q.entities[i].(ReflectStruct); ok {
			return errors.NotSupportedf("mixed entities")
		}

		refValue := q.entities[i].(ReflectValue)
		columnar[i] = refValue.Value.Addr().Interface()
	}

	return q.scan(rows, columnar)
}

func (q Query) mapScan(tx *sql.Tx, stmt string, args []interface{}, entity ReflectValue) error {
	rows, columns, err := q.query(tx, stmt, args)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	columnar := make([]interface{}, len(columns))
	for i, column := range columns {
		columnar[i] = zeroScanType(column.DatabaseTypeName())
	}
	if err := q.scan(rows, columnar); err != nil {
		return errors.Trace(err)
	}

	for i, column := range columns {
		columnName := column.Name()
		colRef := reflect.ValueOf(columnName)
		entity.Value.SetMapIndex(colRef, reflect.Indirect(reflect.ValueOf(columnar[i])))
	}

	return nil
}

func zeroScanType(t string) interface{} {
	switch t {
	case "TEXT", "VARCHAR":
		var a string
		return &a
	case "INTEGER", "BIGINT":
		var a int64
		return &a
	default:
		panic("unexpected type: " + t)
	}
}

func (q Query) structScan(tx *sql.Tx, stmt string, args []interface{}, entities []ReflectStruct) error {
	var fields []recordBinding
	if offset := indexOfRecordArgs(stmt); offset >= 0 {
		var err error
		fields, err = parseRecords(stmt, offset)
		if err != nil {
			return errors.Trace(err)
		}

		// Workout if any of the entities have overlapping fields.
		intersections := fieldIntersections(entities)

		stmt, err = expandRecords(stmt, fields, entities, intersections)
		if err != nil {
			return errors.Trace(err)
		}
	}

	rows, columns, err := q.query(tx, stmt, args)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Traverse the entities available, this is where it becomes very difficult
	// for use. As the sql library doesn't provide the namespaced columns for
	// us to inspect, so if you have overlapping column names it becomes hard
	// to know where to locate that information, without a SQL AST.
	columnar := make([]interface{}, len(columns))
	for i, column := range columns {
		columnName := column.Name()

		var prefix string
		if strings.HasPrefix(columnName, AliasPrefix) {
			parts := strings.Split(columnName[len(AliasPrefix):], AliasSeparator)
			prefix = parts[0]
			columnName = parts[1]
		}

		var found bool
		for _, entity := range entities {
			field, ok := entity.Fields[columnName]
			if !ok {
				continue
			}
			if prefix != "" {
				var bindingFound bool
				for _, binding := range fields {
					if binding.name == entity.Name && binding.prefix == prefix {
						bindingFound = true
						break
					}
				}
				if !bindingFound {
					continue
				}
			}

			columnar[i] = field.Value.Addr().Interface()
			found = true
			break
		}
		if !found {
			return errors.Errorf("missing destination name %q in types %v", column.Name(), entityNames(q.entities))
		}
	}

	return q.scan(rows, columnar)
}

func (q Query) query(tx *sql.Tx, stmt string, args []interface{}) (*sql.Rows, []*sql.ColumnType, error) {
	// Call the hook, before making the query.
	if q.hook != nil {
		q.hook(stmt)
	}

	rows, err := tx.Query(stmt, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// Grab the columns of the rows returned.
	columns, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, nil, errors.Trace(err)
	}
	return rows, columns, nil
}

func (q Query) scan(rows *sql.Rows, args []interface{}) error {
	for rows.Next() {
		if err := rows.Scan(args...); err != nil {
			return errors.Trace(err)
		}
	}

	return errors.Trace(rows.Err())
}

func entityNames(entities []ReflectInfo) []string {
	var names []string
	for _, entity := range entities {
		if rs, ok := entity.(ReflectStruct); ok {
			names = append(names, rs.Name)
		}
	}
	return names
}

type bindCharPredicate func(rune) bool

func alphaNumeric(a rune) bool {
	return unicode.IsLetter(a) || unicode.IsDigit(a) || unicode.IsNumber(a) || a == '_'
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

type nameBinding struct {
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
func parseNames(stmt string, offset int) ([]nameBinding, error) {
	var names []nameBinding

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
			names = append(names, nameBinding{
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

func constructNamedArgs(arg interface{}, names []nameBinding) ([]sql.NamedArg, error) {
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
		return nil, errors.NotSupportedf("%q", k.String())
	default:
		ref, err := Reflect(reflect.ValueOf(arg))
		if err != nil {
			return nil, errors.Trace(err)
		}
		refStruct, ok := ref.(ReflectStruct)
		if !ok {
			return nil, errors.NotSupportedf("%q", k)
		}

		nameValues := make([]sql.NamedArg, len(names))
		for k, name := range names {
			if field, ok := refStruct.Fields[name.name]; ok {
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

// indexOfRecordArgs returns the potential starting index of a record argument
// if the statement contains the record args offset position.
func indexOfRecordArgs(stmt string) int {
	return strings.IndexRune(stmt, '{')
}

type recordBinding struct {
	name       string
	prefix     string
	start, end int
}

func (f recordBinding) translate(expantion int) int {
	return expantion - (f.end - f.start)
}

func parseRecords(stmt string, offset int) ([]recordBinding, error) {
	var records []recordBinding
	for i := offset; i < len(stmt); i++ {
		r := rune(stmt[i])
		if r != '{' {
			return records, nil
		}

		// Parse the Record syntax `{Record}` or optionally `{test INTO Record}`
		var record string
		quotes := make(map[rune]int)
	inner:
		for i = i + 1; i < len(stmt); i++ {
			char := rune(stmt[i])

			switch {
			case unicode.IsLetter(char) || unicode.IsSpace(char):
				fallthrough
			case char == '_':
				record += string(char)
			case char == '"' || char == '\'':
				quotes[char]++
				continue
			case char == '}':
				break inner

			default:
				return nil, errors.Errorf("unexpected struct name in statement")
			}
		}

		var name, prefix string
		parts := strings.Split(strings.TrimSpace(record), " ")
		if num := len(parts); num == 1 {
			name = parts[0]
		} else if num == 3 && strings.ToLower(parts[1]) == "into" {
			prefix = parts[0]
			name = parts[2]
		} else {
			return nil, errors.Errorf("unexpected record statement %q", record)
		}

		// This is a very basic algorithm.
		for char, amount := range quotes {
			if amount%2 != 0 {
				return nil, errors.Errorf("missing quote %q terminator for record statement %q", string(char), record)
			}
		}

		records = append(records, recordBinding{
			name:   strings.TrimSpace(name),
			prefix: prefix,
			start:  offset,
			end:    i + 1,
		})

		if i >= len(stmt) {
			// We're done processing the stmt.
			break
		}
		index := indexOfRecordArgs(stmt[i:])
		if index == -1 {
			// No additional names, skip.
			break
		}
		// We want to reduce the index by 1, so that we also pick up the
		// prefix, otherwise we skip over it.
		offset = i + index
		i = offset - 1
	}
	return records, nil
}

func expandRecords(stmt string, records []recordBinding, entities []ReflectStruct, intersections map[string]map[string]struct{}) (string, error) {
	var offset int
	for _, record := range records {

		var found bool
		for _, entity := range entities {
			if record.name != entity.Name {
				continue
			}

			// Locate any field intersections from the records that's been
			// pre-computed.
			entityInter := intersections[entity.Name]

			// We've located the entity, now swap out all of it's record names.
			names := make([]string, 0, len(entity.Fields))
			for name := range entity.Fields {
				if record.prefix == "" {
					names = append(names, name)
					continue
				}
				var alias string
				if _, ok := entityInter[name]; ok {
					alias = " AS " + AliasPrefix + record.prefix + AliasSeparator + name
				}
				names = append(names, record.prefix+"."+name+alias)
			}
			sort.Strings(names)
			recordList := strings.Join(names, ", ")
			stmt = stmt[:offset+record.start] + recordList + stmt[offset+record.end:]

			// Translate the offset to take into account the new expantions.
			offset += record.translate(len(recordList))

			found = true
			break
		}

		if !found {
			return "", errors.Errorf("no entity found with the name %q", record.name)
		}
	}

	return stmt, nil
}

func fieldIntersections(entities []ReflectStruct) map[string]map[string]struct{} {
	// Don't create anything if we can never overlap.
	if len(entities) <= 1 {
		return nil
	}

	fields := make(map[string][]ReflectStruct)
	for _, entity := range entities {
		// Group the entity fields associated with other entities with similar
		// names.
		for _, field := range entity.FieldNames() {
			fields[field] = append(fields[field], entity)
		}
	}

	// Read the group and identify the overlaps by the entity name, not via
	// the field (inverting the group).
	results := make(map[string]map[string]struct{})
	for fieldName, entities := range fields {
		// Ignore entities that aren't grouped.
		if len(entities) <= 1 {
			continue
		}

		for _, entity := range entities {
			if _, ok := results[entity.Name]; !ok {
				results[entity.Name] = make(map[string]struct{})
			}
			results[entity.Name][fieldName] = struct{}{}
		}
	}

	return results
}
