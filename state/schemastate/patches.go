package schemastate

import (
	"context"
	"database/sql"

	"github.com/juju/errors"
)

var patches = []Patch{
	patchV0,
	patchV1,
}

func patchV0(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(context.TODO(), `
CREATE TABLE IF NOT EXISTS actions (
	id INTEGER PRIMARY KEY AUTOINCREMENT, 
	receiver TEXT,
	name TEXT,
	parameters_json TEXT,
	operation TEXT,
	status TEXT,
	message TEXT,
	enqueued DATETIME,
	started DATETIME,
	completed DATETIME
);
-- The actions logs and results are split into two different tables. This is to 
-- enable the ability to truncate the tables whilst still keeping the actions 
-- intact. 
CREATE TABLE IF NOT EXISTS actions_logs (
	id INTEGER PRIMARY KEY,
	action_id INTEGER,
	output TEXT,
	timestamp DATETIME,
	FOREIGN KEY (action_id)	REFERENCES actions (id)
);
CREATE TABLE IF NOT EXISTS actions_results (
	action_id INTEGER PRIMARY KEY,
	result_json TEXT,
	FOREIGN KEY (action_id)	REFERENCES actions (id)
);
		`,
	)
	return errors.Trace(err)
}

func patchV1(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(context.TODO(), `
CREATE TABLE IF NOT EXISTS operations (
	id INTEGER PRIMARY KEY AUTOINCREMENT, 
	summary TEXT,
	status TEXT,
	enqueued DATETIME,
	started DATETIME,
	completed DATETIME
);
CREATE TABLE IF NOT EXISTS operations_results (
	operation_id INTEGER PRIMARY KEY,
	fail TEXT,
	FOREIGN KEY (operation_id) REFERENCES operations (id)
);
		`,
	)
	return errors.Trace(err)
}
