package coordinator

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
	"agentworkyard/internal/worker"
)

func unknownAttempt(t *testing.T, s *Service, f *fakeAdapter, phase string) Record {
	t.Helper()
	r := newPendingRecord(taskFor(t, s, "uncertain"))
	observation, err := f.Prepare(context.Background(), r.Start)
	if err != nil {
		t.Fatal(err)
	}
	r.Observation = observation
	r.Phase = phase
	if err := s.save(&r, "worker.prepare_intent", ""); err != nil {
		t.Fatal(err)
	}
	if phase == "starting" || phase == "active" {
		if err := s.save(&r, "worker.start_intent", ""); err != nil {
			t.Fatal(err)
		}
	}
	r.Observation.Worker.State = core.StateUnknown
	if err := s.save(&r, "worker.unknown", "fixture crash"); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestResolutionEvidenceAndCapacity(t *testing.T) {
	for _, tc := range []struct {
		name, phase, action        string
		native                     core.ExecutionState
		noTurn, readError, blocked bool
		want                       core.ExecutionState
	}{
		{"ambiguous empty history", "starting", "retry", core.StateUnknown, true, false, true, core.StateUnknown},
		{"running history", "active", "retry", core.StateRunning, false, false, true, core.StateUnknown},
		{"missing history", "active", "retry", core.StateUnknown, false, true, true, core.StateUnknown},
		{"cancel cannot override ambiguity", "starting", "cancel", core.StateUnknown, true, false, true, core.StateUnknown},
		{"prepared empty history", "prepared", "cancel", core.StateUnknown, true, false, false, core.StateCancelled},
		{"lost preparation response", "preparing", "cancel", core.StateUnknown, false, true, false, core.StateCancelled},
		{"prepared retry", "prepared", "retry", core.StateUnknown, true, false, false, core.StatePending},
		{"prepared conflicting native evidence", "prepared", "retry", core.StateRunning, false, false, true, core.StateUnknown},
		{"native failed retry", "active", "retry", core.StateFailed, false, false, false, core.StatePending},
		{"native interrupted retry", "active", "retry", core.StateCancelled, false, false, false, core.StatePending},
		{"native completed blocks retry", "starting", "retry", core.StateCompleted, false, false, true, core.StateCompleted},
		{"native completed reconcile", "starting", "reconcile", core.StateCompleted, false, false, false, core.StateCompleted},
		{"native completed cancel retains result", "starting", "cancel", core.StateCompleted, false, false, false, core.StateCompleted},
		{"native failed reconcile", "active", "reconcile", core.StateFailed, false, false, false, core.StateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, f, _, closeStore := fixture(t)
			defer closeStore()
			old := unknownAttempt(t, s, f, tc.phase)
			obs := old.Observation
			obs.Worker.State = tc.native
			obs.Recovery = &worker.RecoveryEvidence{NoTurn: tc.noTurn, Detail: "native fixture evidence"}
			if tc.native == core.StateCompleted {
				obs.Result = &core.Result{Summary: "recovered result"}
			}
			f.recoverObservation = &obs
			if tc.readError {
				f.recoverError = errors.New("native thread unavailable")
			}
			got, err := s.ResolveTask(context.Background(), old.Request.Task.ID, ResolveRequest{AttemptID: old.Attempt.ID, Action: tc.action, Reason: "operator decision"})
			if tc.blocked != errors.Is(err, ErrResolutionBlocked) || (!tc.blocked && err != nil) {
				t.Fatalf("resolution error: %v", err)
			}
			if got.Observation.Worker.State != tc.want {
				t.Fatalf("wanted %s, got %+v", tc.want, got)
			}
			if f.count() != 0 || f.recovered != 1 {
				t.Fatal("resolution executed or skipped native reconciliation")
			}
			resources, err := s.Resources(context.Background())
			wantInUse := 0
			if tc.want == core.StateUnknown {
				wantInUse = 1
			}
			if err != nil || *resources[0].Capacity.InUse != wantInUse {
				t.Fatalf("capacity: %+v, %v", resources, err)
			}
			if tc.want == core.StateCompleted && got.Observation.Result.Summary != "recovered result" {
				t.Fatal("recovered result lost")
			}
			if tc.want == core.StatePending {
				if got.Attempt.ID == old.Attempt.ID || got.Attempt.Number != 2 || !reflect.DeepEqual(got.Request, old.Request) {
					t.Fatal("invalid successor")
				}
			} else if got.Attempt.ID != old.Attempt.ID {
				t.Fatal("resolution changed attempt identity")
			}
		})
	}
}

func TestRetryHistoryIdempotencyRestartAndLateUpdates(t *testing.T) {
	s, f, path, closeStore := fixture(t)
	old := unknownAttempt(t, s, f, "starting")
	obs := old.Observation
	obs.Worker.State = core.StateCancelled
	obs.Entries = []worker.ConversationEntry{{ID: "old", Text: "old evidence"}}
	obs.Usage = []core.Usage{{AttemptID: old.Attempt.ID}}
	f.recoverObservation = &obs
	request := ResolveRequest{AttemptID: old.Attempt.ID, Action: "retry", Reason: "native interruption confirmed"}
	var wg sync.WaitGroup
	results := make(chan Record, 8)
	errs := make(chan error, 8)
	for range 8 {
		wg.Go(func() { r, err := s.ResolveTask(context.Background(), "uncertain", request); results <- r; errs <- err })
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var next Record
	for r := range results {
		if next.Attempt.ID != "" && next.Attempt.ID != r.Attempt.ID {
			t.Fatal("duplicate retries")
		}
		next = r
	}
	if f.recovered != 1 {
		t.Fatal("replayed retry performed extra reconciliation")
	}
	// A delayed callback from the retired worker cannot replace or poison retry.
	if err := s.apply(worker.Update{Observation: obs, Kind: "turn/completed"}); err != nil {
		t.Fatal(err)
	}
	attempts, err := s.Attempts("uncertain")
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts: %+v %v", attempts, err)
	}
	if attempts[0].Resolution.RetryAttemptID != next.Attempt.ID || len(attempts[0].Observation.Entries) != 1 || len(attempts[0].Observation.Usage) != 1 {
		t.Fatal("lost attempt evidence/link")
	}
	events, err := s.Events("uncertain")
	if err != nil {
		t.Fatal(err)
	}
	foundOld, foundNext := false, false
	for _, event := range events {
		foundOld = foundOld || event.WorkerID == old.Attempt.WorkerID
		foundNext = foundNext || event.WorkerID == next.Attempt.WorkerID
	}
	if !foundOld || !foundNext {
		t.Fatal("task events lost attempt history")
	}
	if id, err := s.SubmitTask(context.Background(), old.Request); err != nil || id != next.Attempt.WorkerID {
		t.Fatal("submission idempotency lost")
	}
	closeStore()
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	fresh, err := New(st, fake(), testCatalog)
	if err != nil {
		t.Fatal(err)
	}
	got, err := fresh.ResolveTask(context.Background(), "uncertain", request)
	if err != nil || got.Attempt.ID != next.Attempt.ID {
		t.Fatalf("replayed retry after restart: %+v %v", got, err)
	}
	if err := fresh.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, _ := fresh.Get("uncertain")
	if current.Attempt.ID != next.Attempt.ID || current.Observation.Worker.State != core.StateRunning {
		t.Fatal("retry did not dispatch")
	}
	_, err = fresh.ResolveTask(context.Background(), "uncertain", ResolveRequest{AttemptID: old.Attempt.ID, Action: "cancel", Reason: "stale request"})
	if !errors.Is(err, ErrResolutionBlocked) {
		t.Fatal("stale action accepted")
	}
	// Replaying the first retry after a later retry still returns its original
	// successor and leaves the newest task attempt untouched.
	current.Observation.Worker.State = core.StateCancelled
	if err := fresh.apply(worker.Update{Observation: current.Observation, Kind: "turn/completed"}); err != nil {
		t.Fatal(err)
	}
	third, err := fresh.ResolveTask(context.Background(), "uncertain", ResolveRequest{AttemptID: next.Attempt.ID, Action: "retry", Reason: "second interruption confirmed"})
	if err != nil || third.Attempt.Number != 3 {
		t.Fatalf("second retry: %+v %v", third, err)
	}
	replayed, err := fresh.ResolveTask(context.Background(), "uncertain", request)
	latest, _ := fresh.Get("uncertain")
	if err != nil || replayed.Attempt.ID != next.Attempt.ID || latest.Attempt.ID != third.Attempt.ID {
		t.Fatal("old retry replay changed current attempt", err)
	}
}

func TestLegacyPreparedResolutionUsesJournal(t *testing.T) {
	s, f, _, closeStore := fixture(t)
	defer closeStore()
	r := newPendingRecord(taskFor(t, s, "uncertain"))
	observation, err := f.Prepare(context.Background(), r.Start)
	if err != nil {
		t.Fatal(err)
	}
	r.Observation = observation
	r.Observation.Worker.State = core.StateUnknown
	// The original recovery implementation overwrote Phase with reconciled and
	// never created an attempt record. Preserve this upgrade recovery path.
	r.Phase = "reconciled"
	if err := s.store.Update(func(tx *store.Tx) error {
		if err := tx.Put("task", r.Request.Task.ID, r); err != nil {
			return err
		}
		return tx.Event(r.Attempt.WorkerID, "worker.prepare_intent", "legacy checkpoint")
	}); err != nil {
		t.Fatal(err)
	}
	obs := r.Observation
	obs.Recovery = &worker.RecoveryEvidence{NoTurn: true, Detail: "empty native history"}
	f.recoverObservation = &obs
	got, err := s.ResolveTask(context.Background(), "uncertain", ResolveRequest{AttemptID: r.Attempt.ID})
	if err != nil || !got.NotStarted || got.Attempt.State != core.StateFailed {
		t.Fatalf("legacy recovery: %+v %v", got, err)
	}
	got, err = s.ResolveTask(context.Background(), "uncertain", ResolveRequest{AttemptID: r.Attempt.ID, Action: "cancel", Reason: "abandon unstarted work"})
	stored, _ := s.Get("uncertain")
	if err != nil || stored.Attempt.State != core.StateCancelled || got.Resolution.Action != "cancel" {
		t.Fatal("cancel after reconcile was not durable")
	}
}

func TestResolutionHTTPDiagnosticsAndAuthentication(t *testing.T) {
	s, f, _, closeStore := fixture(t)
	defer closeStore()
	r := unknownAttempt(t, s, f, "starting")
	handler := s.Handler("secret")
	for _, tc := range []struct {
		token, body string
		status      int
	}{
		{"", `{}`, http.StatusUnauthorized},
		{"secret", `{}`, http.StatusBadRequest},
		{"secret", `{"AttemptID":"wrong"}`, http.StatusConflict},
		{"secret", `{"AttemptID":"` + string(r.Attempt.ID) + `","Action":"retry","Reason":"inspect"}`, http.StatusConflict},
	} {
		req := httptest.NewRequest("POST", "/task/uncertain/resolve", strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+tc.token)
		res := httptest.NewRecorder()
		handler.ServeHTTP(res, req)
		if res.Code != tc.status {
			t.Fatalf("%s: %d %s", tc.body, res.Code, res.Body)
		}
		if bytes.Contains([]byte(tc.body), []byte(`"Reason"`)) && !strings.Contains(res.Body.String(), "capacity remains reserved") {
			t.Fatal("unclear refusal", res.Body.String())
		}
	}
	req := httptest.NewRequest("GET", "/task/uncertain/attempts", nil)
	req.Header.Set("Authorization", "Bearer secret")
	res := httptest.NewRecorder()
	handler.ServeHTTP(res, req)
	var attempts []Record
	if err := json.Unmarshal(res.Body.Bytes(), &attempts); err != nil || res.Code != 200 || len(attempts) != 1 {
		t.Fatalf("attempt inspection: %s %v", res.Body, err)
	}
}

func TestRetryTransactionRollsBackWithoutLosingAttempt(t *testing.T) {
	s, f, path, closeStore := fixture(t)
	defer closeStore()
	r := unknownAttempt(t, s, f, "starting")
	obs := r.Observation
	obs.Worker.State = core.StateFailed
	f.recoverObservation = &obs
	db, err := sql.Open("sqlite", filepath.Join(path, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Fail the second attempt write after the old attempt and its event have
	// been written inside the transaction. Neither half of the retry may commit.
	_, err = db.Exec(`CREATE TRIGGER reject_retry BEFORE INSERT ON records
		WHEN NEW.kind = 'attempt' AND json_extract(NEW.data, '$.Attempt.Number') = 2
		BEGIN SELECT RAISE(ABORT, 'injected retry failure'); END`)
	if err != nil {
		t.Fatal(err)
	}
	request := ResolveRequest{AttemptID: r.Attempt.ID, Action: "retry", Reason: "confirmed native failure"}
	if _, err = s.ResolveTask(context.Background(), "uncertain", request); err == nil {
		t.Fatal("injected transaction failure lost")
	}
	current, err := s.Get("uncertain")
	if err != nil || current.Attempt.ID != r.Attempt.ID || current.Resolution != nil || current.Attempt.State != core.StateFailed {
		t.Fatalf("partial retry committed: %+v %v", current, err)
	}
	attempts, err := s.Attempts("uncertain")
	if err != nil || len(attempts) != 1 {
		t.Fatal("successor leaked", err)
	}
	events, err := s.Events("uncertain")
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Kind == "worker.resolved" || event.Kind == "worker.retry_queued" {
			t.Fatal("partial retry event committed")
		}
	}
	if _, err = db.Exec("DROP TRIGGER reject_retry"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.ResolveTask(context.Background(), "uncertain", request); err != nil {
		t.Fatal(err)
	}
}

func TestRetryRequiresCurrentRuntimeResourceAndOpenAssignment(t *testing.T) {
	for _, reason := range []string{"unavailable", "catalog changed", "assignment closed"} {
		t.Run(reason, func(t *testing.T) {
			s, f, _, closeStore := fixture(t)
			defer closeStore()
			r := unknownAttempt(t, s, f, "starting")
			obs := r.Observation
			obs.Worker.State = core.StateFailed
			f.recoverObservation = &obs
			switch reason {
			case "unavailable":
				s.unavailable = true
			case "catalog changed":
				s.catalog.Resource.Model = "different"
			case "assignment closed":
				assignments, err := s.Assignments()
				if err != nil {
					t.Fatal(err)
				}
				assignments[0].State = core.AssignmentClosed
				if err := s.PutAssignment(context.Background(), assignments[0]); err != nil {
					t.Fatal(err)
				}
			}
			_, err := s.ResolveTask(context.Background(), "uncertain", ResolveRequest{AttemptID: r.Attempt.ID, Action: "retry", Reason: "retry"})
			if !errors.Is(err, ErrResolutionBlocked) {
				t.Fatal("unsafe retry permitted", err)
			}
			attempts, err := s.Attempts("uncertain")
			if err != nil || len(attempts) != 1 {
				t.Fatal("unexpected retry attempt", err)
			}
		})
	}
}

func TestUnknownCapacityDispatchRequiresSafeResolution(t *testing.T) {
	s, f, _, closeStore := fixture(t)
	defer closeStore()
	r := unknownAttempt(t, s, f, "starting")
	if _, err := s.SubmitTask(context.Background(), taskFor(t, s, "queued")); err != nil {
		t.Fatal(err)
	}
	request := ResolveRequest{AttemptID: r.Attempt.ID}
	if _, err := s.ResolveTask(context.Background(), "uncertain", request); !errors.Is(err, ErrResolutionBlocked) {
		t.Fatal(err)
	}
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.count() != 0 {
		t.Fatal("unknown reservation allowed overbooking")
	}
	obs := r.Observation
	obs.Worker.State = core.StateCancelled
	f.recoverObservation = &obs
	if _, err := s.ResolveTask(context.Background(), "uncertain", request); err != nil {
		t.Fatal(err)
	}
	if err := s.dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	queued, err := s.Get("queued")
	if err != nil || queued.Attempt.State != core.StateRunning || f.count() != 1 {
		t.Fatal("safe resolution did not release capacity", err)
	}
}
