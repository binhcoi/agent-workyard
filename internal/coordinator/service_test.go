package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
	"agentworkyard/internal/worker"
)

type fakeAdapter struct {
	worker.Adapter
	mu                 sync.Mutex
	starts             int
	recovered          int
	updates            chan worker.Update
	observations       map[core.ID]worker.Observation
	recoverObservation *worker.Observation
	startError         error
}

func fake() *fakeAdapter {
	return &fakeAdapter{updates: make(chan worker.Update, 32), observations: map[core.ID]worker.Observation{}}
}

func (f *fakeAdapter) Prepare(_ context.Context, request worker.StartRequest) (worker.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	observation := worker.Observation{
		Worker: core.Worker{
			ID:             request.WorkerID,
			TaskID:         request.TaskID,
			AttemptID:      request.AttemptID,
			ConversationID: request.ConversationID,
			RuntimeID:      request.Selection.Resource.RuntimeID,
			State:          core.StatePending,
		},
		Conversation: core.Conversation{
			ID:       request.ConversationID,
			WorkerID: request.WorkerID,
		},
		Handle: json.RawMessage(`{"thread":"native"}`),
	}
	f.observations[request.WorkerID] = observation
	return observation, nil
}

func (f *fakeAdapter) Start(_ context.Context, address worker.Address) (worker.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	observation := f.observations[address.WorkerID]
	observation.Worker.State = core.StateRunning
	f.observations[address.WorkerID] = observation
	return observation, f.startError
}

func (f *fakeAdapter) Recover(_ context.Context, _ worker.StartRequest, observation worker.Observation) (worker.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recovered++
	if f.recoverObservation != nil {
		return *f.recoverObservation, nil
	}
	observation.Worker.State = core.StateUnknown
	return observation, nil
}

func (f *fakeAdapter) Cancel(context.Context, worker.Address) (worker.Cancellation, error) {
	return worker.Cancellation{Requested: true, Confirmed: false, Evidence: "interrupt accepted"}, nil
}

func (f *fakeAdapter) Updates() <-chan worker.Update { return f.updates }
func (f *fakeAdapter) Close() error                  { return nil }
func (f *fakeAdapter) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts
}

func (f *fakeAdapter) complete(record Record, state core.ExecutionState) {
	observation := record.Observation
	observation.Worker.State = state
	observation.Entries = []worker.ConversationEntry{{ID: "user", Speaker: "user", Text: "brief"}, {ID: "answer", Speaker: "worker", Text: "answer"}}
	if state == core.StateCompleted {
		observation.Result = &core.Result{Summary: "verified answer", Artifacts: []core.Artifact{{URI: "artifact.txt"}}}
	}
	f.updates <- worker.Update{Observation: observation, Kind: "turn.completed", Evidence: "native terminal event"}
}

var testCatalog = Catalog{
	Resource: core.ResourceRef{
		MachineID: "local",
		RuntimeID: "fake-local",
		Harness:   "fake",
		Model:     "test",
	},
	MaxConcurrent: 1,
}

func fixture(t *testing.T) (*Service, *fakeAdapter, string, func()) {
	t.Helper()
	stateDir := t.TempDir()
	stateStore, err := store.Open(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	adapter := fake()
	service, err := New(stateStore, adapter, testCatalog)
	if err != nil {
		t.Fatal(err)
	}
	projectRoot := t.TempDir()
	if err = service.PutProject(context.Background(), core.Project{ID: "project", Root: projectRoot, DefaultMachineID: "local"}); err != nil {
		t.Fatal(err)
	}
	assignment := core.Assignment{
		ID:                   "assignment",
		ProjectID:            "project",
		ManagingSupervisorID: "supervisor-one",
		Objective:            "objective",
	}
	if err = service.PutAssignment(context.Background(), assignment); err != nil {
		t.Fatal(err)
	}
	return service, adapter, stateDir, func() { stateStore.Close() }
}

func taskFor(t *testing.T, service *Service, id string) TaskRequest {
	t.Helper()
	var project core.Project
	if err := service.store.Get("project", "project", &project); err != nil {
		t.Fatal(err)
	}
	return TaskRequest{
		Task: core.Task{
			ID:           core.ID(id),
			AssignmentID: "assignment",
			ProfileID:    "general-worker",
			Brief:        core.TaskBrief{Objective: "do work"},
		},
		Instructions: "worker instructions",
		WorkingDir:   project.Root,
		Selection:    core.Selection{Resource: testCatalog.Resource},
	}
}

func runService(service *Service) func() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.Run(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

func waitState(t *testing.T, service *Service, id core.ID, state core.ExecutionState) Record {
	t.Helper()
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := service.Get(id)
		if err == nil && record.Observation.Worker.State == state {
			return record
		}
		select {
		case err := <-service.Errors():
			t.Fatalf("service failed: %v", err)
		case <-deadline:
			t.Fatalf("expected %s, got %+v, %v", state, record, err)
		case <-ticker.C:
		}
	}
}

func TestSupervisorExitIdempotencyCapacityAndDurableResult(t *testing.T) {
	service, adapter, _, closeStore := fixture(t)
	defer closeStore()
	stop := runService(service)
	defer stop()
	// Cancelling the caller's context after submission cannot cancel the worker.
	ctx, cancel := context.WithCancel(context.Background())
	request := taskFor(t, service, "one")
	id, err := service.SubmitTask(ctx, request)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	record := waitState(t, service, "one", core.StateRunning)
	duplicate, err := service.SubmitTask(context.Background(), request)
	if err != nil || duplicate != id {
		t.Fatalf("duplicate: %s %v", duplicate, err)
	}
	changed := request
	changed.Task.Brief.Objective = "different"
	if _, err = service.SubmitTask(context.Background(), changed); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	secondRequest := taskFor(t, service, "two")
	if _, err = service.SubmitTask(context.Background(), secondRequest); err != nil {
		t.Fatal(err)
	}
	resources, err := service.Resources(context.Background())
	if err != nil || *resources[0].Capacity.InUse != 1 {
		t.Fatalf("capacity: %+v %v", resources, err)
	}
	second, err := service.Get("two")
	if err != nil || second.Phase != "queued" || adapter.count() != 1 {
		t.Fatalf("second worker launched beyond capacity: %+v %v", second, err)
	}
	adapter.complete(record, core.StateCompleted)
	finished := waitState(t, service, "one", core.StateCompleted)
	waitState(t, service, "two", core.StateRunning)
	if finished.Observation.Result.Summary != "verified answer" || len(finished.Observation.Entries) != 2 {
		t.Fatal("lost conversation/result")
	}
	var task core.Task
	if err = service.store.Get("task_state", "one", &task); err != nil || task.Acceptance != core.AcceptancePending || task.State != core.StateCompleted {
		t.Fatalf("completion/acceptance separation: %+v %v", task, err)
	}
	events, err := service.Events("one")
	if err != nil || len(events) < 5 {
		t.Fatalf("events: %+v %v", events, err)
	}
	for i := 1; i < len(events); i++ {
		if events[i].Sequence <= events[i-1].Sequence {
			t.Fatal("events out of order")
		}
	}
	assignments, err := service.Assignments()
	if err != nil || assignments[0].ManagingSupervisorID != "supervisor-one" || assignments[0].OwningCoordinatorID != service.ID {
		t.Fatalf("ownership: %+v %v", assignments, err)
	}
}

func TestCancellationRequiresTerminalEvidence(t *testing.T) {
	service, adapter, _, closeStore := fixture(t)
	defer closeStore()
	stop := runService(service)
	defer stop()
	if _, err := service.SubmitTask(context.Background(), taskFor(t, service, "running")); err != nil {
		t.Fatal(err)
	}
	record := waitState(t, service, "running", core.StateRunning)
	cancellation, err := service.CancelTask(context.Background(), "running")
	if err != nil || !cancellation.Requested || cancellation.Confirmed {
		t.Fatalf("premature cancellation confirmation: %+v %v", cancellation, err)
	}
	still, _ := service.Get("running")
	if still.Observation.Worker.State != core.StateRunning || !still.CancelRequested {
		t.Fatal("request became terminal without evidence")
	}
	adapter.complete(record, core.StateCancelled)
	waitState(t, service, "running", core.StateCancelled)
}

func TestQueuedCancellationDoesNotLaunch(t *testing.T) {
	service, adapter, _, closeStore := fixture(t)
	defer closeStore()
	request := taskFor(t, service, "queued")
	if _, err := service.SubmitTask(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	cancellation, err := service.CancelTask(context.Background(), "queued")
	if err != nil || !cancellation.Confirmed {
		t.Fatalf("queued cancel: %+v %v", cancellation, err)
	}
	stop := runService(service)
	defer stop()
	waitState(t, service, "queued", core.StateCancelled)
	if adapter.count() != 0 {
		t.Fatal("cancelled task launched")
	}
}

func TestRestartReconcilesWithoutRelaunch(t *testing.T) {
	for _, recoveredState := range []core.ExecutionState{core.StateCompleted, core.StateUnknown} {
		t.Run(string(recoveredState), func(t *testing.T) {
			service, adapter, path, closeStore := fixture(t)
			request := taskFor(t, service, "one")
			if _, err := service.SubmitTask(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			stop := runService(service)
			record := waitState(t, service, "one", core.StateRunning)
			stop()
			recoveredAdapter := fake()
			observation := record.Observation
			observation.Worker.State = recoveredState
			if recoveredState == core.StateCompleted {
				observation.Result = &core.Result{Summary: "recovered native result"}
				observation.Entries = []worker.ConversationEntry{{Speaker: "worker", Text: "persisted answer"}}
			}
			recoveredAdapter.recoverObservation = &observation
			// Reopen the database to establish persistence beyond the first Service.
			closeStore()
			stateStore, err := store.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer stateStore.Close()
			recoveredService, err := New(stateStore, recoveredAdapter, testCatalog)
			if err != nil {
				t.Fatal(err)
			}
			if recoveredService.ID != service.ID {
				t.Fatal("coordinator identity changed")
			}
			stopNext := runService(recoveredService)
			defer stopNext()
			got := waitState(t, recoveredService, "one", recoveredState)
			if recoveredAdapter.count() != 0 || adapter.count() != 1 {
				t.Fatal("recovery re-executed a task")
			}
			if got.Observation.Worker.ID != record.Observation.Worker.ID || got.Observation.Conversation.ID != record.Observation.Conversation.ID {
				t.Fatal("worker identity changed")
			}
			if recoveredState == core.StateCompleted && got.Observation.Result.Summary != "recovered native result" {
				t.Fatal("recovery lost result")
			}
			same, err := recoveredService.SubmitTask(context.Background(), request)
			if err != nil || same != record.Observation.Worker.ID {
				t.Fatalf("duplicate after restart: %s %v", same, err)
			}
		})
	}
}

func TestAmbiguousStartAndRuntimeLossNeverCompleteOrRetry(t *testing.T) {
	service, adapter, _, closeStore := fixture(t)
	defer closeStore()
	adapter.startError = errors.New("transport lost after request")
	if _, err := service.SubmitTask(context.Background(), taskFor(t, service, "ambiguous")); err != nil {
		t.Fatal(err)
	}
	stop := runService(service)
	defer stop()
	record := waitState(t, service, "ambiguous", core.StateUnknown)
	if record.Diagnostic == "" || record.Observation.Result != nil || adapter.count() != 1 {
		t.Fatal("ambiguous start misreported")
	}
	if _, err := service.SubmitTask(context.Background(), taskFor(t, service, "queued")); err != nil {
		t.Fatal(err)
	}
	close(adapter.updates)
	record = waitState(t, service, "ambiguous", core.StateUnknown)
	if record.Observation.Result != nil {
		t.Fatal("disconnect invented a result")
	}
}

func TestHTTPAuthorizationAndAddressableConversation(t *testing.T) {
	service, _, _, closeStore := fixture(t)
	defer closeStore()
	taskRequest := taskFor(t, service, "one")
	if _, err := service.SubmitTask(context.Background(), taskRequest); err != nil {
		t.Fatal(err)
	}
	handler := service.Handler("secret")
	for _, tc := range []struct {
		method, path, token, body string
		status                    int
	}{
		{method: "GET", path: "/tasks", status: 401},
		{method: "GET", path: "/tasks", token: "wrong", status: 401},
		{method: "GET", path: "/task/one/conversation", token: "secret", status: 200},
		{method: "GET", path: "/task/one/result", token: "secret", status: 409},
		{method: "GET", path: "/task/missing", token: "secret", status: 404},
		{method: "POST", path: "/task", token: "secret", body: `{"unexpected":true}`, status: 400},
		{method: "POST", path: "/project", token: "secret", body: `{} {}`, status: 400},
	} {
		request := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		request.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, request)
		if w.Code != tc.status {
			t.Fatalf("%s: got %d %s", tc.path, w.Code, w.Body.String())
		}
	}
}

func TestDefiniteRejectionFailsAndReleasesCapacity(t *testing.T) {
	service, adapter, _, closeStore := fixture(t)
	defer closeStore()
	adapter.startError = errors.Join(worker.ErrRejected, errors.New("invalid model"))
	for _, id := range []string{"one", "two"} {
		if _, err := service.SubmitTask(context.Background(), taskFor(t, service, id)); err != nil {
			t.Fatal(err)
		}
	}
	stop := runService(service)
	defer stop()
	waitState(t, service, "one", core.StateFailed)
	waitState(t, service, "two", core.StateFailed)
	if adapter.count() != 2 {
		t.Fatal("rejection stalled queued work")
	}
	resources, err := service.Resources(context.Background())
	if err != nil || *resources[0].Capacity.InUse != 0 {
		t.Fatalf("capacity leaked: %+v %v", resources, err)
	}
}

func TestStoredTerminalResultDoesNotContactRuntimeOnRestart(t *testing.T) {
	service, adapter, path, closeStore := fixture(t)
	request := taskFor(t, service, "done")
	if _, err := service.SubmitTask(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	stop := runService(service)
	record := waitState(t, service, "done", core.StateRunning)
	adapter.complete(record, core.StateCompleted)
	waitState(t, service, "done", core.StateCompleted)
	stop()
	closeStore()
	stateStore, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer stateStore.Close()
	recoveredAdapter := fake()
	recoveredService, err := New(stateStore, recoveredAdapter, testCatalog)
	if err != nil {
		t.Fatal(err)
	}
	stop = runService(recoveredService)
	defer stop()
	got := waitState(t, recoveredService, "done", core.StateCompleted)
	if got.Observation.Result == nil || got.Observation.Result.Summary != "verified answer" || len(got.Observation.Entries) != 2 {
		t.Fatal("stored result/history not recovered")
	}
	recoveredAdapter.mu.Lock()
	defer recoveredAdapter.mu.Unlock()
	if recoveredAdapter.recovered != 0 || recoveredAdapter.starts != 0 {
		t.Fatal("terminal state unnecessarily contacted runtime")
	}
}
