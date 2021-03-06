package model

import (
	"time"

	"github.com/juju/names"
)

// ActionStatus represents the possible end states for an action.
type ActionStatus string

const (
	// ActionError signifies that the action did get run due to an error.
	ActionError ActionStatus = "error"

	// ActionFailed signifies that the action did not complete successfully.
	ActionFailed ActionStatus = "failed"

	// ActionCompleted indicates that the action ran to completion as intended.
	ActionCompleted ActionStatus = "completed"

	// ActionCancelled means that the Action was cancelled before being run.
	ActionCancelled ActionStatus = "cancelled"

	// ActionPending is the default status when an Action is first queued.
	ActionPending ActionStatus = "pending"

	// ActionRunning indicates that the Action is currently running.
	ActionRunning ActionStatus = "running"

	// ActionAborting indicates that the Action is running but should be
	// aborted.
	ActionAborting ActionStatus = "aborting"

	// ActionAborted indicates the Action was aborted.
	ActionAborted ActionStatus = "aborted"
)

type Action struct {
	ID  int64
	Tag names.ActionTag

	// Receiver is the Name of the Unit or any other ActionReceiver for
	// which this Action is queued.
	Receiver string

	// Name identifies the action that should be run; it should
	// match an action defined by the unit's charm.
	Name string

	// Parameters holds the action's parameters, if any; it should validate
	// against the schema defined by the named action in the unit's charm.
	Parameters map[string]interface{}

	// Enqueued is the time the action was added.
	Enqueued time.Time

	// Started reflects the time the action began running.
	Started time.Time

	// Completed reflects the time that the action was finished.
	Completed time.Time

	// Operation is the parent operation of the action.
	Operation string

	// Status represents the end state of the Action; ActionFailed for an
	// action that was removed prematurely, or that failed, and
	// ActionCompleted for an action that successfully completed.
	Status ActionStatus

	// Message captures any error returned by the action.
	Message string
}
