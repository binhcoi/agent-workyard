package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"agentworkyard/internal/coordinator"
	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
)

// Exceeds both production channel capacities together. The peer emits these
// before acknowledging turn/start and never invokes a model.
const burstItemCount = 400

func waitForBurst(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for !condition() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for burst progress")
		case <-time.After(time.Millisecond):
		}
	}
}

func TestRealPipeStartBackpressurePreservesOrderedUpdates(t *testing.T) {
	for _, disconnect := range []bool{false, true} {
		t.Run(fmt.Sprintf("disconnect=%t", disconnect), func(t *testing.T) {
			a, _ := testAdapter(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			objective := "burst verify fixture"
			if disconnect {
				objective = "burst eof verify fixture"
			}
			prepared, err := a.Prepare(ctx, testRequest("burst", objective))
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan error, 1)
			go func() { _, err := a.Start(ctx, addr(prepared)); started <- err }()
			waitForBurst(t, func() bool { return len(a.updates) == cap(a.updates) })

			// A full channel must not hold the session mutex or block inspection.
			inspected := make(chan error, 1)
			go func() { _, err := a.Inspect(ctx, addr(prepared)); inspected <- err }()
			select {
			case err := <-inspected:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("backpressure held the adapter mutex")
			}

			var revision uint64
			items, updates := 0, 0
			completed := false
			deadline := time.After(5 * time.Second)
			for !completed || disconnect {
				select {
				case update, ok := <-a.Updates():
					if !ok {
						if !completed {
							t.Fatal("disconnect lost native completion")
						}
						disconnect = false
						break
					}
					if update.Observation.Revision <= revision {
						t.Fatal("observation revisions regressed")
					}
					revision = update.Observation.Revision
					var params nativeNotification
					if err := json.Unmarshal([]byte(update.Evidence), &params); err != nil {
						t.Fatal(err)
					}
					if strings.HasPrefix(params.Item.ID, "burst-") {
						if params.Item.ID != fmt.Sprintf("burst-%d", items) {
							t.Fatalf("lost or reordered item: %s after %d items", params.Item.ID, items)
						}
						items++
					}
					updates++
					completed = completed || update.Kind == "turn/completed"
				case <-deadline:
					t.Fatal("burst blocked launch or lost updates")
				}
			}
			if items != burstItemCount || updates != burstItemCount+5 {
				t.Fatalf("lost updates: items=%d updates=%d", items, updates)
			}
			select {
			case err := <-started:
				// EOF may race the response, but cannot erase terminal evidence.
				if err != nil && objective != "burst eof verify fixture" {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("start did not finish after draining updates")
			}
			done, err := a.Inspect(ctx, addr(prepared))
			if err != nil || done.Worker.State != core.StateCompleted || done.Result == nil || done.Result.Summary != "checked fixture" || len(done.Entries) != burstItemCount+2 {
				t.Fatalf("lost completion: %+v %v", done, err)
			}
		})
	}
}

func TestCloseUnblocksFullNotificationPipeline(t *testing.T) {
	a, _ := testAdapter(t)
	prepared, err := a.Prepare(context.Background(), testRequest("burst", "burst verify fixture"))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	go func() { a.Start(context.Background(), addr(prepared)); close(started) }()
	waitForBurst(t, func() bool { return len(a.updates) == cap(a.updates) })
	closed := make(chan struct{})
	go func() { a.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked on a full notification pipeline")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("shutdown left Start running")
	}
	for range a.Updates() {
	}
}

func TestCoordinatorPersistsBurstBeforeStartAcknowledgement(t *testing.T) {
	gate := filepath.Join(t.TempDir(), "release-start")
	t.Setenv("WORKYARD_PROTOCOL_START_GATE", gate)
	a, _ := testAdapter(t)
	stateDir := t.TempDir()
	stateStore, err := store.Open(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stateStore.Close() })
	catalog := coordinator.Catalog{Resource: core.ResourceRef{MachineID: "local", RuntimeID: "codex-local", Harness: "codex", Model: "fixture"}, MaxConcurrent: 1}
	s, err := coordinator.New(stateStore, a, catalog)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	if err := s.PutProject(ctx, core.Project{ID: "project", Root: root, DefaultMachineID: "local"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutAssignment(ctx, core.Assignment{ID: "assignment", ProjectID: "project", ManagingSupervisorID: "supervisor", Objective: "offline burst"}); err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitTask(ctx, coordinator.TaskRequest{
		Task:      core.Task{ID: "burst", AssignmentID: "assignment", ProfileID: "general-worker", Brief: core.TaskBrief{Objective: "burst tools verify fixture"}},
		Selection: core.Selection{Resource: catalog.Resource}, WorkingDir: root, Instructions: "verify offline fixture",
	})
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() { s.Run(ctx); close(stopped) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			t.Error("coordinator shutdown blocked")
		}
	})
	// Read SQLite directly so the old implementation fails with a bounded timeout
	// instead of hanging this test while holding the coordinator mutex.
	waitForBurst(t, func() bool {
		var current coordinator.Record
		if err := stateStore.Get("task", "burst", &current); err != nil {
			t.Fatal(err)
		}
		return current.Observation.Worker.State == core.StateCompleted
	})
	done, err := s.Get("burst")
	if err != nil || done.Observation.Worker.State != core.StateCompleted || done.Observation.Result == nil || len(done.Observation.Entries) != 2 {
		t.Fatalf("completion was not durable before start reply: %+v %v", done, err)
	}
	events, err := s.Events("burst")
	if err != nil {
		t.Fatal(err)
	}
	items := 0
	startIntent := false
	for _, event := range events {
		if event.Kind == "worker.start_intent" {
			startIntent = true
		}
		if event.Kind == "worker.start_observed" {
			t.Fatal("peer acknowledged start before its gate was released")
		}
		if event.Kind == "item/completed" {
			if !startIntent {
				t.Fatal("notification persisted before durable start intent")
			}
			var params nativeNotification
			if err := json.Unmarshal([]byte(event.Detail), &params); err != nil {
				t.Fatal(err)
			}
			if strings.HasPrefix(params.Item.ID, "burst-") {
				if params.Item.ID != fmt.Sprintf("burst-%d", items) {
					t.Fatal("durable notifications were lost or reordered")
				}
				items++
			}
		}
	}
	if items != burstItemCount {
		t.Fatalf("only %d burst items persisted", items)
	}
	if err := os.WriteFile(gate, nil, 0600); err != nil {
		t.Fatal(err)
	}
	waitForBurst(t, func() bool {
		var current coordinator.Record
		if err := stateStore.Get("task", "burst", &current); err != nil {
			t.Fatal(err)
		}
		return current.Observation.Revision > done.Observation.Revision
	})
	current, err := s.Get("burst")
	if err != nil || current.Observation.Worker.State != core.StateCompleted || current.Observation.Result.Summary != done.Observation.Result.Summary || current.Observation.Revision < done.Observation.Revision {
		t.Fatal("start reply regressed durable completion", err)
	}
	cancel()
	<-stopped
	if err := stateStore.Close(); err != nil {
		t.Fatal(err)
	}
	stateStore, err = store.Open(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	var persisted coordinator.Record
	if err := stateStore.Get("task", "burst", &persisted); err != nil || persisted.Attempt.ID != done.Attempt.ID || persisted.Observation.Result == nil || persisted.Observation.Result.Summary != "checked fixture" {
		t.Fatal("reopen lost the burst result", err)
	}
}
