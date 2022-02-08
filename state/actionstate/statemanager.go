package actionstate

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/juju/names"
)

type Backend interface {
	// Run is a convince function for running one shot transactions, which
	// correctly handles the rollback semantics and retries where available.
	Run(func(context.Context, *sql.Tx) error) error
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
func (m *ActionManager) ActionByID(tx *sqlx.Tx, id int64) (Action, error) {
	var action Action
	err := tx.Get(&action, "SELECT * FROM actions WHERE id=$1", id)
	return action, errors.Trace(err)
}

// ActionByTag returns one action by tag.
func (m *ActionManager) ActionByTag(tx *sqlx.Tx, tag names.ActionTag) (Action, error) {
	var action Action
	err := tx.Get(&action, "SELECT * FROM actions WHERE tag=$1", tag.Id())
	return action, errors.Trace(err)
}

// ActionsByName returns a slice of actions that have the same name.
func (m *ActionManager) ActionsByName(tx *sqlx.Tx, name string) ([]Action, error) {
	var actions []Action
	err := tx.Select(&actions, "SELECT * FROM actions WHERE name=$1 ORDER BY tag", name)
	return actions, errors.Trace(err)
}

// AddAction adds an action, returning the given action.
func (m *ActionManager) AddAction(tx *sqlx.Tx, receiver names.Tag, operationID, actionName string, payload map[string]interface{}) (Action, error) {
	// Marshal the payload first, before attempting to construct any query.
	// TODO (stickupkid): We might consider moving the marshalling outside of
	// the method because of retries.
	payloadData, err := json.Marshal(payload)
	if err != nil {
		return Action{}, errors.Annotatef(err, "marshalling action payload")
	}

	result, err := tx.NamedExec(`
	INSERT INTO actions (receiver, name, parameters_json, operation, enqueued, status)
	VALUES (:receiver, :name, :parameters_json, :operation, DateTime('now'), 'pending')
	`, map[string]interface{}{
		"receiver":        receiver,
		"name":            actionName,
		"parameters_json": payloadData,
		"operation":       operationID,
	})
	if err != nil {
		return Action{}, errors.Trace(err)
	}

	modified, err := result.RowsAffected()
	if err != nil {
		return Action{}, errors.Trace(err)
	}
	if modified != 1 {
		return Action{}, errors.Errorf("expected one action to be inserted: %d", modified)
	}

	// Get the ID, so we can return the action.
	id, err := result.LastInsertId()
	if err != nil {
		return Action{}, errors.Trace(err)
	}

	// Update the tag with the new ID, so that both tag and id correctly match.
	result, err = tx.NamedExec(`UPDATE actions SET tag=:tag WHERE id=:id`, map[string]interface{}{
		"tag": names.NewActionTag(strconv.FormatInt(id, 10)),
		"id":  id,
	})
	if err != nil {
		return Action{}, errors.Trace(err)
	}

	modified, err = result.RowsAffected()
	if err != nil {
		return Action{}, errors.Trace(err)
	}
	if modified != 1 {
		return Action{}, errors.Errorf("expected one action to be inserted: %d", modified)
	}

	return m.ActionByID(tx, id)
}
