package actionstate

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/SimonRichardson/nu-juju-data/db"
	"github.com/SimonRichardson/nu-juju-data/model"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/juju/names"
)

type Action struct {
	ID  int64  `db:"id"`
	Tag string `db:"tag"`

	// Receiver is the Name of the Unit or any other ActionReceiver for
	// which this Action is queued.
	Receiver string `db:"receiver"`

	// Name identifies the action that should be run; it should
	// match an action defined by the unit's charm.
	Name string `db:"name"`

	// Parameters holds the action's parameters, if any; it should validate
	// against the schema defined by the named action in the unit's charm.
	Parameters []byte `db:"parameters_json"`

	// Enqueued is the time the action was added.
	Enqueued sql.NullTime `db:"enqueued"`

	// Started reflects the time the action began running.
	Started sql.NullTime `db:"started"`

	// Completed reflects the time that the action was finished.
	Completed sql.NullTime `db:"completed"`

	// Operation is the parent operation of the action.
	Operation string `db:"operation"`

	// Status represents the end state of the Action; ActionFailed for an
	// action that was removed prematurely, or that failed, and
	// ActionCompleted for an action that successfully completed.
	Status sql.NullString `db:"status"`

	// Message captures any error returned by the action.
	Message sql.NullString `db:"message"`
}

// Fields returns the list of fields directly from an Action type.
func (a Action) Fields(tx *sqlx.Tx) string {
	fields, err := db.FieldNames(tx, a)
	if err != nil {
		panic("programtic error: " + err.Error())
	}
	return fields.Join()
}

func (a Action) ToModel() (model.Action, error) {
	var parameters map[string]interface{}
	if err := json.Unmarshal(a.Parameters, &parameters); err != nil {
		return model.Action{}, errors.Trace(err)
	}

	tag, err := names.ParseActionTag(a.Tag)
	if err != nil {
		return model.Action{}, errors.Trace(err)
	}

	status := model.ActionPending
	if a.Status.Valid {
		status = model.ActionStatus(a.Status.String)
	}

	enqueued := time.Time{}
	if a.Enqueued.Valid {
		enqueued = a.Enqueued.Time
	}

	started := time.Time{}
	if a.Enqueued.Valid {
		started = a.Started.Time
	}

	completed := time.Time{}
	if a.Enqueued.Valid {
		completed = a.Completed.Time
	}

	return model.Action{
		ID:         a.ID,
		Tag:        tag,
		Receiver:   a.Receiver,
		Name:       a.Name,
		Parameters: parameters,
		Enqueued:   enqueued,
		Started:    started,
		Completed:  completed,
		Operation:  a.Operation,
		Status:     status,
		Message:    a.Message.String,
	}, nil
}
