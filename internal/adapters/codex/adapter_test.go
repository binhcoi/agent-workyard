package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"agentworkyard/internal/core"
	"agentworkyard/internal/worker"
)

// This is a protocol peer, not another worker adapter. It exercises real pipes,
// JSON-RPC correlation, early notifications, disconnects and process cleanup.
func TestMain(m *testing.M) {
	if os.Getenv("WORKYARD_CODEX_PROTOCOL_PEER") == "1" {
		protocolPeer()
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func protocolPeer() {
	scan := bufio.NewScanner(os.Stdin)
	encode := json.NewEncoder(os.Stdout)
	counter := 0
	threads := map[string]string{}
	final := `{"Summary":"checked fixture","Artifacts":[{"URI":"fixture.txt","Description":"input evidence"}],"Verification":[{"Command":"inspect fixture.txt","Outcome":"passed","Evidence":[]}],"Blockers":[]}`
	send := func(value any) {
		if err := encode.Encode(value); err != nil {
			os.Exit(2)
		}
	}
	for scan.Scan() {
		var message envelope
		if json.Unmarshal(scan.Bytes(), &message) != nil {
			os.Exit(3)
		}
		if logPath := os.Getenv("WORKYARD_PROTOCOL_LOG"); logPath != "" {
			logFile, _ := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			if logFile != nil {
				logFile.Write(append(scan.Bytes(), '\n'))
				logFile.Close()
			}
		}
		var params struct {
			ThreadID string                  `json:"threadId"`
			Input    []struct{ Text string } `json:"input"`
		}
		json.Unmarshal(message.Params, &params)
		reply := func(value any) { send(map[string]any{"id": message.ID, "result": value}) }
		notify := func(method string, value any) { send(map[string]any{"method": method, "params": value}) }
		switch message.Method {
		case "initialize":
			reply(map[string]any{"userAgent": "protocol-peer"})
		case "initialized":
		case "test/no-response":
			// Keep reading other requests without acknowledging this one.
		case "test/stall-stdin":
			reply(map[string]any{})
			// The parent must interrupt its pipe write and reap this offline peer.
			time.Sleep(time.Hour)
		case "thread/start":
			counter++
			id := fmt.Sprintf("thread-%d", counter)
			threads[id] = ""
			reply(map[string]any{"thread": map[string]any{"id": id}})
		case "turn/start":
			text := params.Input[0].Text
			turn := params.ThreadID + "-turn"
			threads[params.ThreadID] = text
			if strings.Contains(text, "reject") {
				send(map[string]any{"id": message.ID, "error": map[string]any{"code": -32602, "message": "model unavailable"}})
				continue
			}
			notify("turn/started", map[string]any{"threadId": params.ThreadID, "turn": map[string]any{"id": turn, "status": "inProgress"}})
			if strings.Contains(text, "disconnect") {
				return
			}
			if strings.Contains(text, "hold") {
				reply(map[string]any{"turn": map[string]any{"id": turn, "status": "inProgress"}})
				continue
			}
			if strings.Contains(text, "burst") {
				for i := range burstItemCount {
					item := map[string]any{"id": fmt.Sprintf("burst-%d", i), "type": "agentMessage", "phase": "commentary", "text": "progress"}
					if strings.Contains(text, "tools") {
						item = map[string]any{"id": fmt.Sprintf("burst-%d", i), "type": "commandExecution", "command": "printf progress", "cwd": "/fixture", "commandActions": []any{}, "status": "completed", "exitCode": 0, "aggregatedOutput": "progress"}
					}
					notify("item/completed", map[string]any{
						"threadId": params.ThreadID, "turnId": turn,
						"item": item,
					})
				}
			}
			notify("item/completed", map[string]any{
				"threadId":      params.ThreadID,
				"turnId":        turn,
				"completedAtMs": 1234,
				"item": map[string]any{
					"id":      "user-" + params.ThreadID,
					"type":    "userMessage",
					"content": []any{map[string]any{"type": "text", "text": text}},
				},
			})
			notify("item/completed", map[string]any{
				"threadId":      params.ThreadID,
				"turnId":        turn,
				"completedAtMs": 1235,
				"item": map[string]any{
					"id":    "answer-" + params.ThreadID,
					"type":  "agentMessage",
					"phase": "final_answer",
					"text":  final,
				},
			})
			notify("thread/tokenUsage/updated", map[string]any{
				"threadId": params.ThreadID,
				"turnId":   turn,
				"tokenUsage": map[string]any{
					"total": map[string]any{
						"inputTokens":       42,
						"outputTokens":      17,
						"cachedInputTokens": 12,
					},
				},
			})
			notify("turn/completed", map[string]any{"threadId": params.ThreadID, "turn": map[string]any{"id": turn, "status": "completed", "items": []any{}}})
			if strings.Contains(text, "burst") {
				if gate := os.Getenv("WORKYARD_PROTOCOL_START_GATE"); gate != "" {
					for {
						if _, err := os.Stat(gate); err == nil {
							break
						}
						time.Sleep(time.Millisecond)
					}
				}
			}
			reply(map[string]any{"turn": map[string]any{"id": turn, "status": "inProgress"}})
			if strings.Contains(text, "burst eof") {
				return
			}
		case "turn/interrupt":
			reply(map[string]any{})
			notify("turn/completed", map[string]any{
				"threadId": params.ThreadID,
				"turn": map[string]any{
					"id":     params.ThreadID + "-turn",
					"status": "interrupted",
					"items":  []any{},
				},
			})
		case "thread/turns/list":
			if strings.Contains(params.ThreadID, "missing") {
				send(map[string]any{"id": message.ID, "error": map[string]any{"code": -32602, "message": "thread not found"}})
				continue
			}
			if strings.Contains(params.ThreadID, "empty") {
				reply(map[string]any{"data": []any{}, "nextCursor": nil})
				continue
			}
			status := os.Getenv("WORKYARD_PROTOCOL_HISTORY")
			if status == "" {
				status = "completed"
			}
			for _, candidate := range []string{"interrupted", "failed", "inProgress", "unrecognized"} {
				if strings.Contains(params.ThreadID, candidate) {
					status = candidate
				}
			}
			turns := []any{
				map[string]any{
					"id": params.ThreadID + "-turn", "status": status,
					"items": []any{map[string]any{"id": "recovered-message", "type": "agentMessage", "text": final}},
				},
			}
			if strings.Contains(params.ThreadID, "multiple") {
				turns = append(turns, map[string]any{"id": "older", "status": "completed"})
			}
			reply(map[string]any{
				"data":       turns,
				"nextCursor": nil,
			})
		default:
			if len(message.ID) > 0 && message.Method != "" {
				send(map[string]any{"id": message.ID, "error": map[string]any{"code": -32601, "message": "unsupported"}})
			}
		}
	}
}

func testAdapter(t *testing.T) (*Adapter, string) {
	t.Helper()
	t.Setenv("WORKYARD_CODEX_PROTOCOL_PEER", "1")
	log := t.TempDir() + "/protocol.jsonl"
	t.Setenv("WORKYARD_PROTOCOL_LOG", log)
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	adapter, err := New(ctx, binary, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adapter.Close() })
	return adapter, log
}

func testRequest(id, objective string) worker.StartRequest {
	return worker.StartRequest{
		WorkerID:       core.ID(id),
		ConversationID: core.ID("conversation-" + id),
		TaskID:         core.ID("task-" + id),
		AttemptID:      core.ID("attempt-" + id),
		Instructions:   "worker scope",
		Brief:          core.TaskBrief{Objective: objective},
		WorkingDir:     "/fixture",
		Selection: core.Selection{
			Resource: core.ResourceRef{
				RuntimeID: "codex-local",
				Harness:   "codex",
				Model:     "fixture",
			},
		},
	}
}

func addr(observation worker.Observation) worker.Address {
	return worker.Address{WorkerID: observation.Worker.ID, ConversationID: observation.Conversation.ID, RuntimeID: observation.Worker.RuntimeID}
}

func awaitState(t *testing.T, adapter *Adapter, observation worker.Observation, state core.ExecutionState) worker.Observation {
	t.Helper()
	timeout := time.After(3 * time.Second)
	for {
		if current, err := adapter.Inspect(context.Background(), addr(observation)); err == nil && current.Worker.State == state {
			return current
		}
		select {
		case <-timeout:
			t.Fatalf("worker did not reach %s", state)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestStructuredCompletionHistoryUsageAndPrepareBoundary(t *testing.T) {
	adapter, log := testAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := testRequest("one", "verify fixture")
	request.Selection.ReasoningEffort = "low"
	prepared, err := adapter.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Worker.State != core.StatePending || len(prepared.Handle) == 0 {
		t.Fatal("prepare launched work or lost binding")
	}
	data, _ := os.ReadFile(log)
	if strings.Contains(string(data), `"method":"turn/start"`) {
		t.Fatal("prepare made an inference request")
	}
	if _, err = adapter.Start(ctx, addr(prepared)); err != nil {
		t.Fatal(err)
	}
	done := awaitState(t, adapter, prepared, core.StateCompleted)
	if done.Result == nil || done.Result.Summary != "checked fixture" || len(done.Result.Artifacts) != 1 || len(done.Result.Verification) != 1 {
		t.Fatalf("structured result: %+v", done.Result)
	}
	if len(done.Entries) != 2 || done.Entries[0].Speaker != "user" || done.Entries[1].Speaker != "worker" {
		t.Fatalf("isolated history: %+v", done.Entries)
	}
	if len(done.Usage) != 1 || done.Usage[0].Source != core.MeasurementObserved || *done.Usage[0].InputTokens != 42 || *done.Usage[0].OutputTokens != 17 {
		t.Fatalf("usage: %+v", done.Usage)
	}
	data, _ = os.ReadFile(log)
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var message envelope
		json.Unmarshal([]byte(line), &message)
		if message.Method == "turn/start" {
			var params map[string]any
			json.Unmarshal(message.Params, &params)
			if params["effort"] != "low" {
				t.Fatal("explicit low reasoning effort lost")
			}
		}
		if message.Method == "thread/start" {
			var params map[string]any
			json.Unmarshal(message.Params, &params)
			if params["sandbox"] != "read-only" || params["approvalPolicy"] != "never" || params["config"].(map[string]any)["features.multi_agent"] != false {
				t.Fatal("runtime restrictions missing")
			}
		}
	}
}

func TestSharedRuntimeConversationIsolationAndIndependentCancel(t *testing.T) {
	adapter, _ := testAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	one, err := adapter.Prepare(ctx, testRequest("one", "hold first"))
	if err != nil {
		t.Fatal(err)
	}
	two, err := adapter.Prepare(ctx, testRequest("two", "hold second"))
	if err != nil {
		t.Fatal(err)
	}
	if string(one.Handle) == string(two.Handle) || one.Conversation.ID == two.Conversation.ID {
		t.Fatal("shared worker context")
	}
	if _, err = adapter.Start(ctx, addr(one)); err != nil {
		t.Fatal(err)
	}
	if _, err = adapter.Start(ctx, addr(two)); err != nil {
		t.Fatal(err)
	}
	cancellation, err := adapter.Cancel(ctx, addr(one))
	if err != nil || !cancellation.Requested || cancellation.Confirmed {
		t.Fatalf("cancel: %+v %v", cancellation, err)
	}
	awaitState(t, adapter, one, core.StateCancelled)
	other := awaitState(t, adapter, two, core.StateRunning)
	if other.Worker.ID != two.Worker.ID {
		t.Fatal("cancel crossed worker boundary")
	}
	bad := addr(one)
	bad.ConversationID = two.Conversation.ID
	if _, err = adapter.Inspect(ctx, bad); err == nil {
		t.Fatal("mismatched address accepted")
	}
}

func TestRecoveryReadsSavedThreadWithoutExecuting(t *testing.T) {
	adapter, log := testAdapter(t)
	request := testRequest("one", "verify")
	observation := worker.Observation{
		Worker: core.Worker{
			ID:             "one",
			TaskID:         request.TaskID,
			AttemptID:      request.AttemptID,
			ConversationID: request.ConversationID,
			RuntimeID:      "codex-local",
			State:          core.StateRunning,
		},
		Conversation: core.Conversation{ID: request.ConversationID, WorkerID: "one"},
		Handle:       json.RawMessage(`{"ThreadID":"saved","TurnID":"saved-turn"}`),
	}
	got, err := adapter.Recover(context.Background(), request, observation)
	if err != nil || got.Worker.State != core.StateCompleted || got.Result == nil || got.Result.Summary != "checked fixture" {
		t.Fatalf("recovery: %+v %v", got, err)
	}
	data, _ := os.ReadFile(log)
	if strings.Contains(string(data), `"method":"turn/start"`) || strings.Contains(string(data), `"method":"thread/start"`) {
		t.Fatal("recovery re-executed work")
	}
}

func TestDisconnectRemainsUnknown(t *testing.T) {
	adapter, _ := testAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	observation, err := adapter.Prepare(ctx, testRequest("one", "disconnect"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = adapter.Start(ctx, addr(observation)); err == nil {
		t.Fatal("lost start acknowledgement was accepted")
	}
	got := awaitState(t, adapter, observation, core.StateUnknown)
	if got.Result != nil {
		t.Fatal("transport exit became successful completion")
	}
}

func TestRejectedStartIsDefinite(t *testing.T) {
	adapter, _ := testAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	observation, err := adapter.Prepare(ctx, testRequest("one", "reject model"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.Start(ctx, addr(observation))
	var rpcErr *rpcError
	if !errors.As(err, &rpcErr) || rpcErr.Code != -32602 {
		t.Fatalf("explicit rejection lost: %v", err)
	}
}

func TestNativeResolutionEvidenceWithoutInference(t *testing.T) {
	for _, tc := range []struct {
		thread, turn      string
		want              core.ExecutionState
		noTurn, readError bool
	}{
		{"interrupted", "interrupted-turn", core.StateCancelled, false, false},
		{"failed", "failed-turn", core.StateFailed, false, false},
		{"completed", "completed-turn", core.StateCompleted, false, false},
		{"inProgress", "inProgress-turn", core.StateUnknown, false, false},
		{"unrecognized", "unrecognized-turn", core.StateUnknown, false, false},
		{"empty", "", core.StateUnknown, true, false},
		{"missing", "", core.StateUnknown, false, true},
		{"mismatch", "original-turn", core.StateUnknown, false, false},
		{"multiple", "", core.StateUnknown, false, false},
		{"lost-ack", "", core.StateCompleted, false, false},
	} {
		t.Run(tc.thread, func(t *testing.T) {
			a, log := testAdapter(t)
			request := testRequest("one", "verify")
			prepared := worker.Observation{
				Worker:       core.Worker{ID: request.WorkerID, TaskID: request.TaskID, AttemptID: request.AttemptID, ConversationID: request.ConversationID, RuntimeID: "codex-local", State: core.StateUnknown},
				Conversation: core.Conversation{ID: request.ConversationID, WorkerID: request.WorkerID},
			}
			prepared.Handle, _ = json.Marshal(handle{ThreadID: tc.thread, TurnID: tc.turn})
			got, err := a.Recover(context.Background(), request, prepared)
			if (err != nil) != tc.readError {
				t.Fatal(err)
			}
			if got.Worker.State != tc.want {
				t.Fatalf("native outcome: %+v", got)
			}
			if !tc.readError && (got.Recovery == nil || got.Recovery.NoTurn != tc.noTurn || got.Recovery.Detail == "") {
				t.Fatal("missing reconciliation evidence", got.Recovery)
			}
			data, err := os.ReadFile(log)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(data), `"method":"turn/start"`) || strings.Contains(string(data), `"method":"thread/start"`) {
				t.Fatal("reconciliation executed inference")
			}
		})
	}
}

func TestRecoveryPreservesLiveTerminalEvidence(t *testing.T) {
	a, _ := testAdapter(t)
	ctx := context.Background()
	request := testRequest("one", "verify fixture")
	prepared, err := a.Prepare(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Start(ctx, addr(prepared)); err != nil {
		t.Fatal(err)
	}
	done := awaitState(t, a, prepared, core.StateCompleted)
	// Simulate a durable snapshot predating the completion and usage events.
	prepared.Worker.State = core.StateUnknown
	got, err := a.Recover(ctx, request, prepared)
	if err != nil || got.Worker.State != core.StateCompleted || got.Result == nil || got.Result.Summary != done.Result.Summary || len(got.Usage) != 1 || *got.Usage[0].InputTokens != 42 {
		t.Fatalf("live evidence overwritten: %+v, %v", got, err)
	}
}
