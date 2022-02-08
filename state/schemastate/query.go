package schemastate

import (
	"context"
	"database/sql"

	"github.com/juju/errors"
)

// doesSchemaTableExist return whether the schema table is present in the
// database.
func doesSchemaTableExist(ctx context.Context, tx *sql.Tx) (bool, error) {
	statement := `
SELECT COUNT(name) FROM sqlite_master WHERE type = 'table' AND name = 'schema'
`
	rows, err := tx.QueryContext(ctx, statement)
	if err != nil {
		return false, errors.Trace(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return false, errors.Errorf("schema table query returned no rows")
	}

	var count int
	if err := rows.Scan(&count); err != nil {
		return false, errors.Trace(err)
	}
	if err := rows.Err(); err != nil {
		return false, errors.Trace(err)
	}

	return count == 1, nil
}

const schemaTable = `
CREATE TABLE schema (
    id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    version    INTEGER NOT NULL,
    updated_at DATETIME NOT NULL,
    UNIQUE (version)
)
`

// Create the schema table.
func createSchemaTable(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, schemaTable)
	return errors.Trace(err)
}

// Return the highest patch version currently applied. Zero means that no
// patches have been applied yet.
func queryCurrentVersion(ctx context.Context, tx *sql.Tx) (int, error) {
	versions, err := selectSchemaVersions(ctx, tx)
	if err != nil {
		return -1, errors.Errorf("failed to fetch patch versions: %v", err)
	}

	var current int
	if len(versions) > 0 {
		err = checkSchemaVersionsHaveNoHoles(versions)
		if err != nil {
			return -1, errors.Trace(err)
		}
		// Highest recorded version
		current = versions[len(versions)-1]
	}

	return current, nil
}

// Return all versions in the schema table, in increasing order.
func selectSchemaVersions(ctx context.Context, tx *sql.Tx) ([]int, error) {
	statement := `
SELECT version FROM schema ORDER BY version
`
	rows, err := tx.QueryContext(ctx, statement)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var values []int
	for rows.Next() {
		var value int
		err := rows.Scan(&value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		values = append(values, value)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return values, nil
}

// Check that the given list of update version numbers doesn't have "holes",
// that is each version equal the preceding version plus 1.
func checkSchemaVersionsHaveNoHoles(versions []int) error {
	// Ensure that there are no "holes" in the recorded versions.
	for i := range versions[:len(versions)-1] {
		if versions[i+1] != versions[i]+1 {
			return errors.Errorf("missing patches: %d to %d", versions[i], versions[i+1])
		}
	}
	return nil
}

// Check that all the given patches are applied.
func checkAllPatchesAreApplied(ctx context.Context, tx *sql.Tx, patches []Patch) error {
	versions, err := selectSchemaVersions(ctx, tx)
	if err != nil {
		return errors.Errorf("failed to fetch patch versions: %v", err)
	}

	if len(versions) == 0 {
		return errors.Errorf("expected schema table to contain at least one row")
	}

	err = checkSchemaVersionsHaveNoHoles(versions)
	if err != nil {
		return errors.Trace(err)
	}

	current := versions[len(versions)-1]
	if current != len(patches) {
		return errors.Errorf("patch level is %d, expected %d", current, len(patches))
	}
	return nil
}

// Ensure that the schema exists.
func ensureSchemaTableExists(ctx context.Context, tx *sql.Tx) error {
	exists, err := doesSchemaTableExist(ctx, tx)
	if err != nil {
		return errors.Errorf("failed to check if schema table is there: %v", err)
	}
	if !exists {
		if err := createSchemaTable(ctx, tx); err != nil {
			return errors.Errorf("failed to create schema table: %v", err)
		}
	}
	return nil
}

// Apply any pending patch that was not yet applied.
func ensurePatchsAreApplied(ctx context.Context, tx *sql.Tx, current int, patches []Patch, hook Hook) error {
	if current > len(patches) {
		return errors.Errorf(
			"schema version '%d' is more recent than expected '%d'",
			current, len(patches))
	}

	// If there are no patches, there's nothing to do.
	if len(patches) == 0 {
		return nil
	}

	// Apply missing patches.
	for _, patch := range patches[current:] {
		// If the context has any underlying errors, close out immediately.
		if err := ctx.Err(); err != nil {
			return errors.Trace(err)
		}

		if err := hook(ctx, tx, current); err != nil {
			return errors.Annotatef(err, "failed to execute hook (version %d)", current)
		}

		if err := patch(ctx, tx); err != nil {
			return errors.Errorf("failed to apply patch %d: %v", current, err)
		}
		current++

		if err := insertSchemaVersion(ctx, tx, current); err != nil {
			return errors.Errorf("failed to insert version %d", current)
		}
	}

	return nil
}

// Insert a new version into the schema table.
func insertSchemaVersion(ctx context.Context, tx *sql.Tx, new int) error {
	statement := `
INSERT INTO schema (version, updated_at) VALUES (?, strftime("%s"))
`
	_, err := tx.ExecContext(ctx, statement, new)
	return err
}

// Return a list of SQL statements that can be used to create all tables in the
// database.
func selectTablesSQL(ctx context.Context, tx *sql.Tx) ([]string, error) {
	statement := `
SELECT sql FROM sqlite_master WHERE
  type IN ('table', 'index', 'view', 'trigger') AND
  name != 'schema' AND
  name NOT LIKE 'sqlite_%'
ORDER BY name
`
	rows, err := tx.QueryContext(ctx, statement)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, errors.Trace(err)
		}
		tables = append(tables, name)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return tables, nil
}
