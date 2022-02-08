package schema

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"
)

// Dump returns a SQL text dump of all rows across all tables.
func Dump(backend Backend, schema *Schema) (string, error) {
	var statements []string
	err := backend.Run(func(ctx context.Context, tx *sql.Tx) error {
		// Firstly, parse the schema table, checking for the currently applied
		// schema version.
		stmts, err := parseTableStatements(tx, "schema", strings.Trim(schemaTable, "\n"))
		if err != nil {
			return errors.Annotatef(err, "failed to dump table schema")
		}
		statements = append(statements, stmts...)

		// Secondly, get the currently applied schema.
		schemas, err := schema.applied(ctx, tx)
		if err != nil {
			return errors.Trace(err)
		}

		// Thirdly, parse only the tables out of the applied schema, so that
		// we can correctly inspect every table.
		for _, table := range parseTables(schemas) {
			stmts, err := parseTableStatements(tx, table.name, table.statements)
			if err != nil {
				return errors.Annotatef(err, "failed to dump table %s", table)
			}
			statements = append(statements, stmts...)
		}

		// Fourthly, it's advised to remove the sqlite_sequence if we want to
		// replay the schema from the dump, so no sequence items are
		// correctly started.
		stmts, err = parseTableStatements(tx, "sqlite_sequence", "DELETE FROM sqlite_sequence")
		if err != nil {
			return errors.Annotatef(err, "failed to dump table sqlite_sequence")
		}
		statements = append(statements, stmts...)

		return nil
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	var sql string
	sql += "BEGIN TRANSACTION;\n"
	sql += strings.Join(statements, ";\n") + ";\n"
	sql += "COMMIT;\n"

	return sql, nil
}

type tableSchema struct {
	name       string
	statements string
}

// parseTables return a sorted slice of table names to their schema
// definition, taking a full schema generated with Schema.Applied().
func parseTables(schemas []string) []tableSchema {
	tables := make(map[string]string)
	for _, statement := range schemas {
		statement = strings.Trim(statement, " \n") + ";"
		if !strings.HasPrefix(statement, "CREATE TABLE") {
			continue
		}
		table := strings.Split(statement, " ")[2]
		tables[table] = statement
	}

	sorted := make([]tableSchema, 0, len(tables))
	for table, statements := range tables {
		sorted = append(sorted, tableSchema{
			name:       table,
			statements: statements,
		})
	}
	sort.Slice(sorted, func(i int, j int) bool {
		return sorted[i].name < sorted[j].name
	})
	return sorted
}

// parseTableStatements dumps a single table, returning the SQL statements
// containing statements for its schema and data.
func parseTableStatements(tx *sql.Tx, table, schema string) ([]string, error) {
	statements := []string{schema}

	// Query all rows.
	rows, err := tx.Query(fmt.Sprintf("SELECT * FROM %s ORDER BY rowid", table))
	if err != nil {
		return nil, errors.Annotatef(err, "failed to fetch rows")
	}
	defer rows.Close()

	// Figure column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.Annotatef(err, "failed to get columns")
	}

	// Generate an INSERT statement for each row.
	for i := 0; rows.Next(); i++ {
		raw := make([]interface{}, len(columns))
		row := make([]interface{}, len(columns))
		for i := range raw {
			row[i] = &raw[i]
		}
		err := rows.Scan(row...)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to scan row %d", i)
		}

		values := make([]string, len(columns))
		for j, v := range raw {
			switch v := v.(type) {
			case int64:
				values[j] = strconv.FormatInt(v, 10)
			case string:
				values[j] = fmt.Sprintf("'%s'", v)
			case []byte:
				values[j] = fmt.Sprintf("'%s'", string(v))
			case time.Time:
				values[j] = strconv.FormatInt(v.Unix(), 10)
			case nil:
				values[j] = "NULL"
			default:
				return nil, errors.Errorf("unexpected column type %q for row %d", columns[j], i)
			}
		}
		statement := fmt.Sprintf("INSERT INTO %s VALUES(%s)", table, strings.Join(values, ","))
		statements = append(statements, statement)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return statements, nil
}
