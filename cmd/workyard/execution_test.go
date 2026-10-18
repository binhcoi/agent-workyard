package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"agentworkyard/internal/coordinator"
	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
	"agentworkyard/internal/worker"
)

type resolutionAdapter struct {
	worker.Adapter
	state core.ExecutionState
	reads int
}

func (a *resolutionAdapter) Recover(_ context.Context, _ worker.StartRequest, observation worker.Observation) (worker.Observation, error) {
	a.reads++
	observation.Worker.State = a.state
	observation.Recovery = &worker.RecoveryEvidence{Detail: "native fixture state: " + string(a.state)}
	return observation, nil
}

// This exercises the real CLI parser, authenticated HTTP client/server, durable
// lifecycle and recovery refusal. The deterministic adapter performs no inference.
func TestResolutionCLI(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	adapter := &resolutionAdapter{state: core.StateUnknown}
	catalog := coordinator.Catalog{Resource: core.ResourceRef{MachineID: "local", RuntimeID: "codex-local", Harness: "codex", Model: "fixture"}, MaxConcurrent: 1}
	s, err := coordinator.New(st, adapter, catalog)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	ctx := context.Background()
	if err := s.PutProject(ctx, core.Project{ID: "project", Root: root, DefaultMachineID: "local"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutAssignment(ctx, core.Assignment{ID: "assignment", ProjectID: "project", ManagingSupervisorID: "supervisor", Objective: "inspect"}); err != nil {
		t.Fatal(err)
	}
	request := coordinator.TaskRequest{Task: core.Task{ID: "one", AssignmentID: "assignment", ProfileID: "general-worker", Brief: core.TaskBrief{Objective: "inspect"}}, WorkingDir: root, Instructions: "inspect", Selection: core.Selection{Resource: catalog.Resource}}
	if _, err := s.SubmitTask(ctx, request); err != nil {
		t.Fatal(err)
	}
	record, err := s.Get("one")
	if err != nil {
		t.Fatal(err)
	}
	record.Phase = "starting"
	record.Attempt.State = core.StateUnknown
	record.Observation.Worker.State = core.StateUnknown
	if err := st.Update(func(tx *store.Tx) error { return tx.Put("task", "one", record) }); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(s.Handler("secret"))
	defer server.Close()
	if err := writeEndpointFile(dir, endpoint{URL: server.URL, Token: "secret"}); err != nil {
		t.Fatal(err)
	}
	call := func(operation string, extra ...string) ([]byte, error) {
		t.Helper()
		args := append([]string{"task", operation, "--state", dir, "--id", "one"}, extra...)
		var out, stderr bytes.Buffer
		err := run(args, &out, &stderr)
		return out.Bytes(), err
	}
	for _, flags := range [][]string{
		nil,
		{"--attempt", string(record.Attempt.ID), "--action", "force"},
		{"--attempt", string(record.Attempt.ID), "--action", "retry"},
	} {
		if data, err := call("resolve", flags...); err == nil || len(data) != 0 || adapter.reads != 0 {
			t.Fatal("invalid CLI command made a request")
		}
	}
	flags := []string{"--attempt", string(record.Attempt.ID), "--action", "retry", "--reason", "native interruption confirmed"}
	if data, err := call("resolve", flags...); err == nil || !bytes.Contains([]byte(err.Error()), []byte("capacity remains reserved")) || len(data) != 0 {
		t.Fatal("missing refusal diagnostic", err)
	}
	adapter.state = core.StateCancelled
	data, err := call("resolve", flags...)
	if err != nil {
		t.Fatal(err)
	}
	var next coordinator.Record
	if err := json.Unmarshal(data, &next); err != nil || next.Attempt.Number != 2 {
		t.Fatal("retry response", err)
	}
	if _, err := call("resolve", flags...); err != nil {
		t.Fatal("idempotent retry", err)
	}
	data, err = call("attempts")
	var attempts []coordinator.Record
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &attempts); err != nil || len(attempts) != 2 || attempts[0].Resolution.RetryAttemptID != next.Attempt.ID {
		t.Fatal("missing CLI history", err)
	}
	data, err = call("status")
	if err != nil || !bytes.Contains(data, []byte(next.Attempt.ID)) {
		t.Fatal("status lost retry identity", err)
	}
}
