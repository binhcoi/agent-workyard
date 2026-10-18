package coordinator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"agentworkyard/internal/core"
	"agentworkyard/internal/worker"
)

type gatedLifecycleAdapter struct {
	*fakeAdapter
	stage      string
	entered    chan struct{}
	release    chan struct{}
	replyError error
}

func (a *gatedLifecycleAdapter) gate(ctx context.Context, observation worker.Observation) (worker.Observation, error) {
	stale := observation
	stale.Revision = 1
	for i := range 80 { // Exceeds the fake adapter's bounded update channel.
		observation.Revision = uint64(i + 2)
		observation.Entries = []worker.ConversationEntry{{ID: "progress", Text: fmt.Sprint(i)}}
		if i == 79 && a.stage != "prepare" {
			observation.Worker.State = core.StateCompleted
			observation.Result = &core.Result{Summary: "completion before RPC reply"}
		}
		a.mu.Lock()
		a.observations[observation.Worker.ID] = observation
		a.mu.Unlock()
		select {
		case a.updates <- worker.Update{Observation: observation, Kind: "native.progress", Evidence: fmt.Sprint(i)}:
		case <-ctx.Done():
			return stale, ctx.Err()
		}
	}
	close(a.entered)
	select {
	case <-a.release:
		return stale, a.replyError
	case <-ctx.Done():
		return stale, ctx.Err()
	}
}

func (a *gatedLifecycleAdapter) Prepare(ctx context.Context, request worker.StartRequest) (worker.Observation, error) {
	observation, err := a.fakeAdapter.Prepare(ctx, request)
	if a.stage == "prepare" && err == nil {
		return a.gate(ctx, observation)
	}
	return observation, err
}

func (a *gatedLifecycleAdapter) Start(ctx context.Context, address worker.Address) (worker.Observation, error) {
	observation, err := a.fakeAdapter.Start(ctx, address)
	if a.stage == "start" && err == nil {
		return a.gate(ctx, observation)
	}
	return observation, err
}

func (a *gatedLifecycleAdapter) Recover(ctx context.Context, request worker.StartRequest, saved worker.Observation) (worker.Observation, error) {
	if a.stage == "recover" {
		return a.gate(ctx, saved)
	}
	return a.fakeAdapter.Recover(ctx, request, saved)
}

func (a *gatedLifecycleAdapter) Cancel(ctx context.Context, address worker.Address) (worker.Cancellation, error) {
	a.mu.Lock()
	observation := a.observations[address.WorkerID]
	a.mu.Unlock()
	_, err := a.gate(ctx, observation)
	return worker.Cancellation{Requested: true}, err
}

func TestLifecycleRPCsContinueConsumingUpdates(t *testing.T) {
	for _, stage := range []string{"prepare", "start", "recover", "cancel"} {
		for _, lostReply := range []bool{false, true} {
			if stage == "prepare" && lostReply {
				continue
			}
			t.Run(fmt.Sprintf("%s/lostReply=%t", stage, lostReply), func(t *testing.T) {
				s, f, _, closeStore := fixture(t)
				defer closeStore()
				a := &gatedLifecycleAdapter{fakeAdapter: f, stage: stage, entered: make(chan struct{}), release: make(chan struct{})}
				if lostReply {
					a.replyError = errors.New("reply lost after native notifications")
				}
				s.adapter = a
				id := core.ID("burst")
				if stage == "recover" {
					unknownAttempt(t, s, f, "starting")
					id = "uncertain"
				} else if _, err := s.SubmitTask(context.Background(), taskFor(t, s, string(id))); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				stop := runService(s)
				defer func() { cancel(); close(a.release); stop() }()
				cancelled := make(chan error, 1)
				if stage == "cancel" {
					waitState(t, s, id, core.StateRunning)
					go func() { _, err := s.CancelTask(ctx, id); cancelled <- err }()
				}
				select {
				case <-a.entered:
				case <-time.After(3 * time.Second):
					t.Fatal("native call could not deliver its burst while waiting for reply")
				}
				deadline := time.After(3 * time.Second)
				for {
					current, err := s.Get(id)
					if err != nil {
						t.Fatal(err)
					}
					if current.Observation.Revision == 81 {
						break
					}
					select {
					case <-deadline:
						t.Fatal("updates were not committed during native call")
					case <-time.After(time.Millisecond):
					}
				}
				// Release a stale response, or a transport error, after all updates
				// have committed. Neither may overwrite the newer durable snapshot.
				a.release <- struct{}{}
				if stage == "cancel" {
					if err := <-cancelled; (err != nil) != lostReply {
						t.Fatal(err)
					}
				} else {
					s.operations.Lock()
					s.operations.Unlock()
				}
				current, err := s.Get(id)
				if err != nil || current.Observation.Revision != 81 || current.Observation.Entries[0].Text != "79" {
					t.Fatalf("reply regressed observations: %+v %v", current, err)
				}
				if stage != "prepare" && (current.Observation.Worker.State != core.StateCompleted || current.Observation.Result == nil) {
					t.Fatal("reply erased native terminal evidence")
				}
				events, err := s.Events(id)
				if err != nil {
					t.Fatal(err)
				}
				next := 0
				for _, event := range events {
					if event.Kind == "native.progress" {
						if event.Detail != fmt.Sprint(next) {
							t.Fatal("observations were lost or reordered")
						}
						next++
					}
				}
				if next != 80 {
					t.Fatalf("only %d observations persisted", next)
				}
			})
		}
	}
}

func TestOlderUpdatesAreJournaledWithoutRegressingSnapshot(t *testing.T) {
	s, _, _, closeStore := fixture(t)
	defer closeStore()
	record := newPendingRecord(taskFor(t, s, "ordered"))
	record.Observation.Revision = 10
	record.Observation.Worker.State = core.StateCompleted
	record.Observation.Result = &core.Result{Summary: "newest snapshot"}
	if err := s.save(&record, "worker.start_observed", ""); err != nil {
		t.Fatal(err)
	}
	for _, state := range []core.ExecutionState{core.StateRunning, core.StateFailed} {
		older := record.Observation
		older.Revision = 9
		older.Worker.State = state
		older.Result = nil
		if err := s.apply(worker.Update{Observation: older, Kind: "older.notification", Evidence: string(state)}); err != nil {
			t.Fatal(err)
		}
	}
	current, err := s.Get("ordered")
	if err != nil || current.Observation.Revision != 10 || current.Observation.Worker.State != core.StateCompleted || current.Observation.Result == nil {
		t.Fatal("older update regressed the checkpoint", err)
	}
	events, err := s.Events("ordered")
	if err != nil || len(events) != 3 || events[1].Detail != "running" || events[2].Detail != "failed" {
		t.Fatal("older native observations were not journaled in order", err)
	}
}
