package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/SimonRichardson/nu-juju-data/model"
	"github.com/SimonRichardson/nu-juju-data/state"
	"github.com/jmoiron/sqlx"
	"github.com/juju/errors"
	"github.com/juju/names"
)

type Server struct {
	state *state.State
}

func New(state *state.State) *Server {
	return &Server{
		state: state,
	}
}

func (s Server) Serve(address string) (net.Listener, error) {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
		if len(parts) == 0 {
			http.Error(w, "empty collection", http.StatusBadRequest)
			return
		}
		switch parts[0] {
		case "actions":
			s.handleActions(w, r)
		default:
			http.Error(w, fmt.Sprintf("unexpected collection %q", parts[0]), http.StatusBadRequest)
		}
	})

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	go http.Serve(listener, nil)

	return listener, err
}

func (s Server) handleActions(w http.ResponseWriter, r *http.Request) {
	actionManager := s.state.ActionManager()

	switch r.Method {
	case "POST":
		defer r.Body.Close()

		var input InputAction
		if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		receiverTag, err := names.ParseTag(input.Operation)
		if err != nil {
			http.Error(w, errors.Annotatef(err, "receiver tag").Error(), http.StatusBadRequest)
			return
		}

		var action model.Action
		err = s.state.Backend().Run(func(ctx context.Context, tx *sqlx.Tx) error {
			var err error
			action, err = actionManager.AddAction(tx, receiverTag, input.Operation, input.Name, input.Parameters)
			return errors.Trace(err)
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Convert to an action entity before sending.
		output, err := outputAction(action)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		if err := encoder.Encode(output); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}

	case "GET":
		reqValue, ok := getActionReqValue(r)
		if !ok {
			http.Error(w, fmt.Sprintf("id %q not found", reqValue), http.StatusNotFound)
			return
		}
		id, err := strconv.ParseInt(reqValue, 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("invalid id %q", reqValue), http.StatusBadRequest)
			return
		}

		var action model.Action
		err = s.state.Backend().Run(func(ctx context.Context, tx *sqlx.Tx) error {
			var err error
			action, err = actionManager.ActionByID(tx, id)
			return errors.Trace(err)
		})
		if err != nil {
			if errors.IsNotFound(err) {
				http.Error(w, err.Error(), http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}

		// Convert to an action entity before sending.
		output, err := outputAction(action)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		encoder := json.NewEncoder(w)
		encoder.SetIndent("", "    ")
		if err := encoder.Encode(output); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}

func getActionReqValue(r *http.Request) (string, bool) {
	parts := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
	if len(parts) != 2 {
		return "", false
	}
	if parts[1] == "" {
		return "", false
	}
	return parts[1], true
}

func outputAction(a model.Action) (OutputAction, error) {
	return OutputAction{
		ID:         a.ID,
		Tag:        a.Tag.String(),
		Receiver:   a.Receiver,
		Name:       a.Name,
		Parameters: a.Parameters,
		Enqueued:   a.Enqueued,
		Started:    a.Started,
		Completed:  a.Completed,
		Operation:  a.Operation,
		Status:     string(a.Status),
		Message:    a.Message,
	}, nil
}
