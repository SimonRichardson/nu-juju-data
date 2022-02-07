package db

import (
	"context"
	"database/sql"

	"github.com/juju/errors"
)

// State creates a new state manager for handling transactions with the required
// retry semantics.
type State struct {
	db *sql.DB
}

// NewState creates a new State from a given *sql.DB
func NewState(db *sql.DB) *State {
	return &State{
		db: db,
	}
}

// Run is a convince function for running one shot transactions, which correctly
// handles the rollback semantics and retries where available.
// The run function maybe called multiple times if the transaction is being
// retried.
func (s *State) Run(fn func(context.Context, *sql.Tx) error) error {
	txn, err := s.CreateTxn(context.Background())
	if err != nil {
		return errors.Trace(err)
	}

	return txn.Stage(fn).Commit()
}

// CreateTxn creates a transaction builder. The transaction builder accumulates
// a series of functions that can be executed on a given commit.
func (s *State) CreateTxn(ctx context.Context) (TxnBuilder, error) {
	return &txnBuilder{
		db:  s.db,
		ctx: ctx,
	}, nil
}

// TxnBuilder allows the building of a series of functions that will be called
// during a transaction commit. Only upon commit is the transaction constructed
// and the function called.
// The functions in the txn builder maybe called multiple times depending on
// how many retries are employed.
type TxnBuilder interface {
	Stage(func(context.Context, *sql.Tx) error) TxnBuilder
	Commit() error
}

// txnBuilder creates a type for executing transactions and ensuring rollback
// symantics are employed.
type txnBuilder struct {
	db        *sql.DB
	ctx       context.Context
	runnables []func(context.Context, *sql.Tx) error
}

// Context returns the underlying TxnBuilder context.
func (t *txnBuilder) Context() context.Context {
	return t.ctx
}

// Stage adds a function to a given transaction context. The transaction
// isn't committed until the commit method is called.
// The run function maybe called multiple times if the transaction is being
// retried.
func (t *txnBuilder) Stage(fn func(context.Context, *sql.Tx) error) TxnBuilder {
	t.runnables = append(t.runnables, fn)
	return t
}

// Commit commits the transaction.
func (t *txnBuilder) Commit() error {
	return withRetry(func() error {
		// Ensure that we don't attempt to retry if the context has been
		// cancelled or errored out.
		if err := t.ctx.Err(); err != nil {
			return errors.Trace(err)
		}

		rawTx, err := t.db.BeginTx(t.ctx, nil)
		if err != nil {
			// Nested transactions are not supported, if we get an error during
			// the begin transaction phase, attempt to rollback both
			// transactions, so that they can correctly start again.
			if rawTx != nil {
				_, _ = rawTx.Exec("ROLLBACK")
			}
			return errors.Trace(err)
		}

		for _, fn := range t.runnables {
			if err := fn(t.ctx, rawTx); err != nil {
				// Ensure we rollback when attempt to run each function with in
				// a transaction commit.
				_ = rawTx.Rollback()
				return errors.Trace(err)
			}
		}
		return rawTx.Commit()
	})
}
