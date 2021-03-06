package schemastate

import (
	"context"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

// Schema captures the schema of a database in terms of a series of ordered
// updates.
type Schema struct {
	patches []Patch
	hook    Hook
}

// Patch applies a specific schema change to a database, and returns an error
// if anything goes wrong.
type Patch func(context.Context, *sqlx.Tx) error

// Hook is a callback that gets fired when a update gets applied.
type Hook func(context.Context, *sqlx.Tx, int) error

// New creates a new schema Schema with the given patches.
func New(patches []Patch) *Schema {
	return &Schema{
		patches: patches,
		hook:    omitHook,
	}
}

// Empty creates a new schema with no patches.
func Empty() *Schema {
	return New([]Patch{})
}

// Add a new update to the schema. It will be appended at the end of the
// existing series.
func (s *Schema) Add(update Patch) {
	s.patches = append(s.patches, update)
}

// Hook instructs the schema to invoke the given function whenever a update is
// about to be applied. The function gets passed the update version number and
// the running transaction, and if it returns an error it will cause the schema
// transaction to be rolled back. Any previously installed hook will be
// replaced.
func (s *Schema) Hook(hook Hook) {
	s.hook = hook
}

// Len returns the number of total patches in the schema.
func (s *Schema) Len() int {
	return len(s.patches)
}

// ChangeSet returns the schema changes for the schema when they're applied.
type ChangeSet struct {
	Current, Applied int
}

// Ensure makes sure that the actual schema in the given database matches the
// one defined by our updates.
//
// All updates are applied transactionally. In case any error occurs the
// transaction will be rolled back and the database will remain unchanged.
//
// A update will be applied only if it hasn't been before (currently applied
// updates are tracked in the a 'schema' table, which gets automatically
// created).
//
// If no error occurs, the integer returned by this method is the
// initial version that the schema has been upgraded from.
func (s *Schema) Ensure(backend Backend) (ChangeSet, error) {
	var (
		current = -1
		applied = -1
	)
	err := backend.Run(func(ctx context.Context, t *sqlx.Tx) error {
		err := ensureSchemaTableExists(ctx, t)
		if err != nil {
			return errors.Trace(err)
		}

		current, err = queryCurrentVersion(ctx, t)
		if err != nil {
			return errors.Trace(err)
		}

		err = ensurePatchsAreApplied(ctx, t, current, s.patches, s.hook)
		if err != nil {
			return errors.Trace(err)
		}

		applied, err = queryCurrentVersion(ctx, t)
		if err != nil {
			return errors.Trace(err)
		}

		return nil
	})
	return ChangeSet{
		Current: current,
		Applied: applied,
	}, errors.Trace(err)
}

// Applied returns the SQL commands that has been applied to the database. The
// applied text returns a flattened list SQL statements that can be used as a
// fresh install if required.
func (s *Schema) Applied(backend Backend) (string, error) {
	var applied []string
	err := backend.Run(func(ctx context.Context, tx *sqlx.Tx) error {
		var err error
		applied, err = s.applied(ctx, tx)
		return errors.Trace(err)
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	return strings.Join(applied, ";\n"), nil
}

func (s *Schema) applied(ctx context.Context, tx *sqlx.Tx) ([]string, error) {
	if err := checkAllPatchesAreApplied(ctx, tx, s.patches); err != nil {
		return nil, errors.Trace(err)
	}
	statements, err := selectTablesSQL(ctx, tx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Add a statement for inserting the current schema version row.
	statements = append(
		statements,
		fmt.Sprintf(`
INSERT INTO schema (version, updated_at) VALUES (%d, strftime("%%s"))
`, len(s.patches)))

	return statements, nil
}

// omitHook always returns a nil, omitting the error.
func omitHook(context.Context, *sqlx.Tx, int) error { return nil }
