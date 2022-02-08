package state

import (
	"context"
	"sync"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
)

type Backend interface {
	// Run is a convince function for running one shot transactions, which
	// correctly handles the rollback semantics and retries where available.
	Run(func(context.Context, *sqlx.Tx) error) error
}

// StateManager is implemented by types responsible for observing
// the system and manipulating it to reflect the desired state.
type StateManager interface {
	// StartUp asks manager to perform any expensive initialization.
	StartUp(context.Context) error
	// Stop asks the manager to terminate all activities running
	// concurrently.  It must not return before these activities
	// are finished.
	Stop()
}

// StateEngine controls the dispatching of state changes to state managers.
//
// Most of the actual work performed by the state engine is in fact done
// by the individual managers registered. These managers must be able to
// cope with Ensure calls in any order, coordinating among themselves
// solely via the state.
type StateEngine struct {
	backend Backend
	started bool
	stopped bool
	// managers in use
	mutex    sync.Mutex
	managers []StateManager
}

// NewStateEngine returns a new state engine.
func NewStateEngine(backend Backend) *StateEngine {
	return &StateEngine{
		backend: backend,
	}
}

// AddManager adds the provided manager to take part in state operations.
func (se *StateEngine) AddManager(m StateManager) {
	se.mutex.Lock()
	defer se.mutex.Unlock()

	se.managers = append(se.managers, m)
}

// StartUp asks all managers to perform any expensive initialization.
// It is a noop after the first invocation.
func (se *StateEngine) StartUp(ctx context.Context) error {
	se.mutex.Lock()
	defer se.mutex.Unlock()
	if se.started {
		return nil
	}

	se.started = true
	for _, m := range se.managers {
		if err := m.StartUp(ctx); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// Stop asks all managers to terminate activities running concurrently.
func (se *StateEngine) Stop() {
	se.mutex.Lock()
	defer se.mutex.Unlock()

	if se.stopped {
		return
	}
	for _, m := range se.managers {
		m.Stop()
	}
	se.stopped = true
}

// Backend returns the current system backend state.
func (se *StateEngine) Backend() Backend {
	return se.backend
}
