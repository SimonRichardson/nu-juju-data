package schema

import (
	"context"
	"database/sql"

	"github.com/juju/errors"
)

type State interface {
	// Run is a convince function for running one shot transactions, which
	// correctly handles the rollback semantics and retries where available.
	Run(func(context.Context, *sql.Tx) error) error
}

// Schema captures the schema of a database in terms of a series of ordered
// updates.
type Schema struct {
	patches []Patch
	hook    Hook
}

// Patch applies a specific schema change to a database, and returns an error
// if anything goes wrong.
type Patch func(context.Context, *sql.Tx) error

// Hook is a callback that gets fired when a update gets applied.
type Hook func(int, context.Context, *sql.Tx) error

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
func (s *Schema) Ensure(st State) (ChangeSet, error) {
	var (
		current = -1
		applied = -1
	)
	err := st.Run(func(ctx context.Context, t *sql.Tx) error {
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

// omitHook always returns a nil, omitting the error.
func omitHook(int, context.Context, *sql.Tx) error { return nil }
