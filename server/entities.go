package server

import "time"

// ActionMessage represents a progress message logged by an action.
type ActionMessage struct {
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

type OutputAction struct {
	ID  int64  `json:"id"`
	Tag string `json:"tag"`

	// Receiver is the Name of the Unit or any other ActionReceiver for
	// which this Action is queued.
	Receiver string `json:"receiver"`

	// Name identifies the action that should be run; it should
	// match an action defined by the unit's charm.
	Name string `json:"name"`

	// Parameters holds the action's parameters, if any; it should validate
	// against the schema defined by the named action in the unit's charm.
	Parameters map[string]interface{} `json:"parameters"`

	// Enqueued is the time the action was added.
	Enqueued time.Time `json:"enqueued"`

	// Started reflects the time the action began running.
	Started time.Time `json:"started"`

	// Completed reflects the time that the action was finished.
	Completed time.Time `json:"completed"`

	// Operation is the parent operation of the action.
	Operation string `json:"operation"`

	// Status represents the end state of the Action; ActionFailed for an
	// action that was removed prematurely, or that failed, and
	// ActionCompleted for an action that successfully completed.
	Status string `json:"status"`

	// Message captures any error returned by the action.
	Message string `json:"message"`
}

type InputAction struct {
	// Receiver is the Name of the Unit or any other ActionReceiver for
	// which this Action is queued.
	Receiver string `json:"receiver"`

	// Name identifies the action that should be run; it should
	// match an action defined by the unit's charm.
	Name string `json:"name"`

	// Parameters holds the action's parameters, if any; it should validate
	// against the schema defined by the named action in the unit's charm.
	Parameters map[string]interface{} `json:"parameters"`

	// Operation is the parent operation of the action.
	Operation string `json:"operation"`
}
