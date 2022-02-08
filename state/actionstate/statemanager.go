package actionstate

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/SimonRichardson/nu-juju-data/model"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/juju/names"
	"github.com/juju/utils"
)

type Backend interface {
	// Run is a convince function for running one shot transactions, which
	// correctly handles the rollback semantics and retries where available.
	Run(func(context.Context, *sqlx.Tx) error) error
}

type ActionManager struct {
	backend Backend
}

// NewManager creates a new manager from a backend.
func NewManager(backend Backend) *ActionManager {
	return &ActionManager{
		backend: backend,
	}
}

func (m *ActionManager) StartUp(ctx context.Context) error {
	// TODO (stickupkid): Prepare any queries within a transaction, to help
	// with performance.
	return nil
}

func (m *ActionManager) Stop() {}

// ActionByID returns one action by id.
func (m *ActionManager) ActionByID(tx *sqlx.Tx, id int64) (model.Action, error) {
	var action Action
	err := tx.Get(&action, "SELECT * FROM actions WHERE id=$1", id)
	if err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return model.Action{}, errors.NotFoundf("action %v", id)
		}
		return model.Action{}, errors.Trace(err)
	}

	return convertAction(action)
}

// ActionByTag returns one action by tag.
func (m *ActionManager) ActionByTag(tx *sqlx.Tx, tag names.ActionTag) (model.Action, error) {
	var action Action
	err := tx.Get(&action, "SELECT * FROM actions WHERE tag=$1", tag.Id())
	if err != nil {
		if errors.Cause(err) == sql.ErrNoRows {
			return model.Action{}, errors.NotFoundf("action %q", tag.Id())
		}
		return model.Action{}, errors.Trace(err)
	}
	return convertAction(action)
}

// ActionsByName returns a slice of actions that have the same name.
func (m *ActionManager) ActionsByName(tx *sqlx.Tx, name string) ([]model.Action, error) {
	var actions []Action
	err := tx.Select(&actions, "SELECT * FROM actions WHERE name=$1 ORDER BY tag", name)
	if err != nil {
		return nil, errors.Trace(err)
	}

	results := make([]model.Action, len(actions))
	for k, action := range actions {
		if results[k], err = convertAction(action); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return results, nil
}

// AddAction adds an action, returning the given action.
func (m *ActionManager) AddAction(tx *sqlx.Tx, receiver names.Tag, operationID, actionName string, payload map[string]interface{}) (model.Action, error) {
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return model.Action{}, errors.Trace(err)
	}

	uuid, err := utils.NewUUID()
	if err != nil {
		return model.Action{}, errors.Trace(err)
	}

	result, err := tx.NamedExec(`
	INSERT INTO actions (tag, receiver, name, parameters_json, operation, enqueued, status)
	VALUES (:tag, :receiver, :name, :parameters, :operation, DateTime('now'), 'pending')
	`, map[string]interface{}{
		"tag":        names.NewActionTag(uuid.String()).String(),
		"receiver":   receiver.String(),
		"name":       actionName,
		"parameters": payloadData,
		"operation":  operationID,
	})
	if err != nil {
		return model.Action{}, errors.Trace(err)
	}

	modified, err := result.RowsAffected()
	if err != nil {
		return model.Action{}, errors.Trace(err)
	}
	if modified != 1 {
		return model.Action{}, errors.Errorf("expected one action to be inserted: %d", modified)
	}

	// Get the ID, so we can return the action.
	id, err := result.LastInsertId()
	if err != nil {
		return model.Action{}, errors.Trace(err)
	}

	return m.ActionByID(tx, id)
}

func convertAction(action Action) (model.Action, error) {
	var parameters map[string]interface{}
	if err := json.Unmarshal(action.Parameters, &parameters); err != nil {
		return model.Action{}, errors.Trace(err)
	}

	tag, err := names.ParseActionTag(action.Tag)
	if err != nil {
		return model.Action{}, errors.Trace(err)
	}

	status := model.ActionPending
	if action.Status.Valid {
		status = model.ActionStatus(action.Status.String)
	}

	enqueued := time.Time{}
	if action.Enqueued.Valid {
		enqueued = action.Enqueued.Time
	}

	started := time.Time{}
	if action.Enqueued.Valid {
		started = action.Started.Time
	}

	completed := time.Time{}
	if action.Enqueued.Valid {
		completed = action.Completed.Time
	}

	return model.Action{
		ID:         action.ID,
		Tag:        tag,
		Receiver:   action.Receiver,
		Name:       action.Name,
		Parameters: parameters,
		Enqueued:   enqueued,
		Started:    started,
		Completed:  completed,
		Operation:  action.Operation,
		Status:     status,
		Message:    action.Message.String,
	}, nil
}
