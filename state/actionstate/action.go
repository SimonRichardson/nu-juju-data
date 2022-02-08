package actionstate

import (
	"database/sql"
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
