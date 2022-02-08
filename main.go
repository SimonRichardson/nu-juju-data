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
	"github.com/SimonRichardson/nu-juju-data/server"
	"github.com/SimonRichardson/nu-juju-data/state"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/juju/clock"
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

			backend := db.NewSQLDatabase(dqliteDB, app.Driver())
			state := state.NewState(backend)
			if err := state.StartUp(context.Background()); err != nil {
				return err
			}

			// Log out the current applied schema.
			// fmt.Println(state.SchemaManager().Applied())

			server := server.New(state)
			listener, err := server.Serve(apiAddr)
			if err != nil {
				return err
			}

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

			listener.Close()

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
