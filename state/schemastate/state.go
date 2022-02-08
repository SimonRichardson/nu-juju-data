package schemastate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/SimonRichardson/nu-juju-data/schema"
	"github.com/juju/errors"
)

type Backend interface {
	// Run is a convince function for running one shot transactions, which
	// correctly handles the rollback semantics and retries where available.
	Run(func(context.Context, *sql.Tx) error) error
}

type SchemaManager struct {
	backend Backend
	schema  *schema.Schema
}

// NewManager creates a new manager from a backend.
func NewManager(backend Backend) *SchemaManager {
	return &SchemaManager{
		backend: backend,
		schema:  schema.New(patches),
	}
}

func (m *SchemaManager) StartUp(ctx context.Context) error {
	m.schema.Hook(func(ctx context.Context, tx *sql.Tx, current int) error {
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
func (m *SchemaManager) Schema() *schema.Schema {
	return m.schema
}
