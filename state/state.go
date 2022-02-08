package state

import (
	"context"
	"sync"

	"github.com/SimonRichardson/nu-juju-data/state/actionstate"
	"github.com/SimonRichardson/nu-juju-data/state/schemastate"
	"gopkg.in/tomb.v2"
)

// State is the central manager of the system, keeping track
// of all available state managers and related helpers.
type State struct {
	stateEng *StateEngine
	tomb     *tomb.Tomb
	// managers
	mutex   sync.Mutex
	started bool

	schemaMgr *schemastate.SchemaManager
	actionMgr *actionstate.ActionManager
}

// NewState state creates a managed system state encapsulating a backend.
func NewState(backend Backend) *State {
	s := &State{
		tomb:     new(tomb.Tomb),
		stateEng: NewStateEngine(backend),
	}

	// Ensure we register the new schema manager first.
	s.schemaMgr = schemastate.NewManager(backend)
	s.stateEng.AddManager(s.schemaMgr)

	s.actionMgr = actionstate.NewManager(backend)
	s.stateEng.AddManager(s.actionMgr)

	return s
}

// StartUp proceeds to run any expensive State or managers initialization.
// After this is done once it is a noop.
func (s *State) StartUp(ctx context.Context) error {
	// Use the mutex to prevent multiple calls to startup causing the engine
	// to startup.
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.started {
		return nil
	}
	s.started = true

	return s.stateEng.StartUp(ctx)
}

// Stop stops the ensure loop and the managers under the StateEngine.
func (s *State) Stop() error {
	s.tomb.Kill(nil)
	err := s.tomb.Wait()
	s.stateEng.Stop()
	return err
}

// Backend returns the system backend managed by the state.
func (s *State) Backend() Backend {
	return s.stateEng.Backend()
}

// StateEngine returns the state engine used by state.
func (s *State) StateEngine() *StateEngine {
	return s.stateEng
}

// SchemaManager returns the schema manager from the state.
func (s *State) SchemaManager() *schemastate.SchemaManager {
	return s.schemaMgr
}

// ActionManager returns the action manager from the state.
func (s *State) ActionManager() *actionstate.ActionManager {
	return s.actionMgr
}
