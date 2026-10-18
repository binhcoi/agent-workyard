package coordinator

import (
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"agentworkyard/internal/core"
	"agentworkyard/internal/worker"
)

// Handler is local supervisor control. It launches no work in request contexts.
func (s *Service) Handler(token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /identity", func(w http.ResponseWriter, r *http.Request) {
		reply(w, map[string]core.ID{"coordinator": s.ID})
	})
	mux.HandleFunc("POST /project", func(w http.ResponseWriter, r *http.Request) {
		var project core.Project
		if err := decode(w, r, &project); err != nil {
			fail(w, err)
			return
		}
		if err := s.PutProject(r.Context(), project); err != nil {
			fail(w, err)
			return
		}
		reply(w, project)
	})
	mux.HandleFunc("POST /assignment", func(w http.ResponseWriter, r *http.Request) {
		var assignment core.Assignment
		if err := decode(w, r, &assignment); err != nil {
			fail(w, err)
			return
		}
		if err := s.PutAssignment(r.Context(), assignment); err != nil {
			fail(w, err)
			return
		}
		reply(w, map[string]core.ID{"assignment": assignment.ID, "coordinator": s.ID})
	})
	mux.HandleFunc("GET /assignments", func(w http.ResponseWriter, r *http.Request) {
		assignments, err := s.Assignments()
		respond(w, assignments, err)
	})
	mux.HandleFunc("POST /task", func(w http.ResponseWriter, r *http.Request) {
		var taskRequest TaskRequest
		if err := decode(w, r, &taskRequest); err != nil {
			fail(w, err)
			return
		}
		workerID, err := s.SubmitTask(r.Context(), taskRequest)
		respond(w, map[string]core.ID{"worker": workerID, "task": taskRequest.Task.ID}, err)
	})
	mux.HandleFunc("GET /tasks", func(w http.ResponseWriter, r *http.Request) {
		records, err := s.List()
		respond(w, records, err)
	})
	mux.HandleFunc("GET /resources", func(w http.ResponseWriter, r *http.Request) {
		resources, err := s.Resources(r.Context())
		respond(w, resources, err)
	})
	mux.HandleFunc("GET /runtime", func(w http.ResponseWriter, r *http.Request) {
		runtime, err := s.adapter.Runtime(r.Context(), s.catalog.Resource.MachineID)
		respond(w, runtime, err)
	})
	mux.HandleFunc("GET /task/{id}", func(w http.ResponseWriter, r *http.Request) {
		record, err := s.Get(core.ID(r.PathValue("id")))
		respond(w, record, err)
	})
	mux.HandleFunc("GET /task/{id}/conversation", func(w http.ResponseWriter, r *http.Request) {
		record, err := s.Get(core.ID(r.PathValue("id")))
		conversation := worker.ConversationPage{
			ConversationID: record.Observation.Conversation.ID,
			Entries:        record.Observation.Entries,
		}
		respond(w, conversation, err)
	})
	mux.HandleFunc("GET /task/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		record, err := s.Get(core.ID(r.PathValue("id")))
		if err == nil && record.Observation.Result == nil {
			w.WriteHeader(http.StatusConflict)
			reply(w, map[string]any{
				"state":      record.Observation.Worker.State,
				"diagnostic": record.Diagnostic,
				"result":     nil,
			})
			return
		}
		respond(w, record.Observation.Result, err)
	})
	mux.HandleFunc("GET /task/{id}/events", func(w http.ResponseWriter, r *http.Request) {
		events, err := s.Events(core.ID(r.PathValue("id")))
		respond(w, events, err)
	})
	mux.HandleFunc("GET /task/{id}/attempts", func(w http.ResponseWriter, r *http.Request) {
		attempts, err := s.Attempts(core.ID(r.PathValue("id")))
		respond(w, attempts, err)
	})
	mux.HandleFunc("POST /task/{id}/resolve", func(w http.ResponseWriter, r *http.Request) {
		var request ResolveRequest
		if err := decode(w, r, &request); err != nil {
			fail(w, err)
			return
		}
		record, err := s.ResolveTask(r.Context(), core.ID(r.PathValue("id")), request)
		respond(w, record, err)
	})
	mux.HandleFunc("POST /task/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		cancellation, err := s.CancelTask(r.Context(), core.ID(r.PathValue("id")))
		respond(w, cancellation, err)
	})
	// Apply authentication before any route can read state or submit work.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if token == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			reply(w, map[string]string{"error": "unauthorized"})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func decode(w http.ResponseWriter, r *http.Request, destination any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON object")
	}
	return nil
}

func reply(w http.ResponseWriter, value any) { _ = json.NewEncoder(w).Encode(value) }
func fail(w http.ResponseWriter, err error) {
	code := http.StatusBadRequest
	if errors.Is(err, sql.ErrNoRows) {
		code = http.StatusNotFound
	}
	if errors.Is(err, ErrResolutionBlocked) {
		code = http.StatusConflict
	}
	w.WriteHeader(code)
	reply(w, map[string]string{"error": err.Error()})
}

func respond(w http.ResponseWriter, value any, err error) {
	if err != nil {
		fail(w, err)
		return
	}
	reply(w, value)
}
