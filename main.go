package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/SimonRichardson/nu-juju-data/db"
	"github.com/SimonRichardson/nu-juju-data/repl"
	"github.com/SimonRichardson/nu-juju-data/schema"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/juju/clock"
	"github.com/juju/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	doItLive()
}

func doItLive() {
	var apiAddr string
	var dbAddr string
	var join *[]string
	var dir string
	var verbose bool

	cmd := &cobra.Command{
		Use:   "nu-juju-data",
		Short: "Demo to show the nu-juju-data",
		RunE: func(cmd *cobra.Command, args []string) error {
			logFunc := func(l client.LogLevel, format string, a ...interface{}) {
				if !verbose {
					return
				}
				log.Printf(fmt.Sprintf("%s: %s: %s\n", apiAddr, l.String(), format), a...)
			}

			// Setup up the database.
			app, err := app.New(dir, app.WithAddress(dbAddr), app.WithCluster(*join), app.WithLogFunc(logFunc))
			if err != nil {
				return err
			}
			if err := app.Ready(context.Background()); err != nil {
				return err
			}
			dqliteDB, err := app.Open(context.Background(), "demo")
			if err != nil {
				return err
			}

			replSock := filepath.Join(dir, "juju.sock")
			_ = os.Remove(replSock)
			_, err = repl.New(replSock, dbGetter{db: dqliteDB}, clock.WallClock)
			if err != nil {
				return err
			}

			state := db.NewState(dqliteDB)

			schema := schema.New(patches)
			changeSet, err := schema.Ensure(state)
			if err != nil {
				return err
			}
			fmt.Println("changeset:", changeSet)

			ch := make(chan os.Signal, 1)
			signal.Notify(ch, unix.SIGPWR)
			signal.Notify(ch, unix.SIGINT)
			signal.Notify(ch, unix.SIGQUIT)
			signal.Notify(ch, unix.SIGTERM)
			signal.Notify(ch, unix.SIGKILL)
			select {
			case <-ch:
			}
			dqliteDB.Close()

			app.Handover(context.Background())
			app.Close()

			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&apiAddr, "api", "a", "", "address used to expose the demo API")
	flags.StringVarP(&dbAddr, "db", "d", "", "address used for internal database replication")
	join = flags.StringSliceP("join", "j", nil, "database addresses of existing nodes")
	flags.StringVarP(&dir, "dir", "D", "/tmp/dqlite-demo", "data directory")
	flags.BoolVarP(&verbose, "verbose", "v", false, "verbose logging")

	cmd.MarkFlagRequired("api")
	cmd.MarkFlagRequired("db")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

type dbGetter struct {
	db *sql.DB
}

func (g dbGetter) GetExistingDB(_ string) (*sql.DB, error) {
	return g.db, nil
}

var patches = []schema.Patch{
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
	FOREIGN KEY (operation_id)	REFERENCES operations (id)
);
		`,
	)
	return errors.Trace(err)
}
