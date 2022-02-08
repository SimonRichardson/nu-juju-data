package schemastate

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

type Backend interface {
	// Run is a convince function for running one shot transactions, which
	// correctly handles the rollback semantics and retries where available.
	Run(func(context.Context, *sqlx.Tx) error) error
}

type SchemaManager struct {
	backend Backend
	schema  *Schema
}

// NewManager creates a new manager from a backend.
func NewManager(backend Backend) *SchemaManager {
	return &SchemaManager{
		backend: backend,
		schema:  New(patches),
	}
}

func (m *SchemaManager) StartUp(ctx context.Context) error {
	m.schema.Hook(func(ctx context.Context, tx *sqlx.Tx, current int) error {
		fmt.Println("Applying:", current)
		return nil
	})
	// Ignore the change set from ensure for now.
	_, err := m.schema.Ensure(m.backend)
	return errors.Trace(err)
}

func (m *SchemaManager) Stop() {}

// Applied returns the applied schema.
func (m *SchemaManager) Applied() (string, error) {
	return m.schema.Applied(m.backend)
}

// Schema returns the underlying schema that is being managed.
func (m *SchemaManager) Schema() *Schema {
	return m.schema
}
