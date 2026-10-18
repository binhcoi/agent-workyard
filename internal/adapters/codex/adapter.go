// Package codex implements independently addressed workers over App Server JSON-RPC.
package codex

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"time"

	"agentworkyard/internal/core"
	"agentworkyard/internal/worker"
)

type handle struct{ ThreadID, TurnID string }
type session struct {
	request     worker.StartRequest
	observation worker.Observation
	handle      handle
}

// Adapter owns one Codex runtime and maps each thread to an independent worker.
type Adapter struct {
	// mu protects sessions and their snapshots from concurrent API/event access.
	rpc      *rpc
	mu       sync.Mutex
	sessions map[core.ID]*session
	threads  map[string]core.ID
	updates  chan worker.Update
	stop     chan struct{}
	once     sync.Once
}

// Compile-time check: *Adapter must implement every method in worker.Adapter.
var _ worker.Adapter = (*Adapter)(nil)

func New(ctx context.Context, binary string, stderr io.Writer) (*Adapter, error) {
	client, err := newRPC(binary, stderr)
	if err != nil {
		return nil, err
	}
	adapter := &Adapter{
		rpc:      client,
		sessions: map[core.ID]*session{},
		threads:  map[string]core.ID{},
		updates:  make(chan worker.Update, 128),
		stop:     make(chan struct{}),
	}
	if err = client.call(ctx, "initialize", map[string]any{"clientInfo": map[string]string{"name": "agent_workyard", "version": "0.2.0"}}, nil); err != nil {
		client.close()
		return nil, err
	}
	if err = client.write(map[string]any{"method": "initialized", "params": map[string]any{}}); err != nil {
		client.close()
		return nil, err
	}
	go adapter.listen()
	return adapter, nil
}

func (a *Adapter) Runtime(_ context.Context, machineID core.ID) (core.Runtime, error) {
	return core.Runtime{
		ID:        "codex-local",
		MachineID: machineID,
		Harness:   "codex",
		Capabilities: core.RuntimeCapabilities{
			IsolatedConversations:       core.SupportAvailable,
			IndependentControl:          core.SupportAvailable,
			ConversationInspection:      core.SupportAvailable,
			Cancellation:                core.SupportAvailable,
			Recovery:                    core.SupportAvailable,
			SharedHosting:               core.SupportAvailable,
			UsageReporting:              core.SupportAvailable,
			Resume:                      core.SupportUnavailable,
			RestrictUnmanagedDelegation: core.SupportAvailable,
			Messages: core.MessageCapabilities{
				Live:         core.SupportUnavailable,
				Continuation: core.SupportUnavailable,
				Queue:        core.SupportUnavailable,
			},
			Limitations: []string{
				"M2 uses read-only sandbox and never approvals; interactive requests are refused.",
				"Messaging and continuation are not implemented in this slice.",
				"Coordinator/runtime crashes may interrupt turns; ambiguous work is never automatically relaunched.",
				"Native multi_agent is disabled; arbitrary external processes are not a comprehensive nested-delegation security boundary.",
			},
		},
	}, nil
}

func (a *Adapter) Prepare(ctx context.Context, request worker.StartRequest) (worker.Observation, error) {
	var response struct{ Thread struct{ ID string } }
	params := map[string]any{
		"cwd":                   request.WorkingDir,
		"model":                 request.Selection.Resource.Model,
		"approvalPolicy":        "never",
		"sandbox":               "read-only",
		"ephemeral":             false,
		"developerInstructions": request.Instructions,
		"config": map[string]any{
			"features.multi_agent": false,
		},
	}
	if request.Selection.Resource.Provider != "" {
		params["modelProvider"] = request.Selection.Resource.Provider
	}
	if err := a.rpc.call(ctx, "thread/start", params, &response); err != nil {
		return worker.Observation{}, err
	}
	if response.Thread.ID == "" {
		return worker.Observation{}, errors.New("Codex returned an empty thread ID")
	}
	observation := worker.Observation{
		Worker: core.Worker{
			ID:             request.WorkerID,
			TaskID:         request.TaskID,
			AttemptID:      request.AttemptID,
			ProfileID:      request.ProfileID,
			ConversationID: request.ConversationID,
			RuntimeID:      request.Selection.Resource.RuntimeID,
			State:          core.StatePending,
			Resource:       request.Selection.Resource,
		},
		Conversation: core.Conversation{
			ID:       request.ConversationID,
			WorkerID: request.WorkerID,
		},
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	workerSession := &session{
		request:     request,
		observation: observation,
		handle:      handle{ThreadID: response.Thread.ID},
	}
	a.sessions[request.WorkerID] = workerSession
	a.threads[response.Thread.ID] = request.WorkerID
	return snapshot(workerSession), nil
}

func (a *Adapter) Start(ctx context.Context, address worker.Address) (worker.Observation, error) {
	a.mu.Lock()
	workerSession, err := a.find(address)
	if err != nil {
		a.mu.Unlock()
		return worker.Observation{}, err
	}
	request := workerSession.request
	threadID := workerSession.handle.ThreadID
	a.mu.Unlock()
	briefJSON, _ := json.Marshal(request.Brief)
	var response struct{ Turn nativeTurn }
	params := map[string]any{
		"threadId": threadID,
		"input": []any{
			map[string]any{
				"type": "text",
				"text": "Execute this task brief within its authorized scope. Do not delegate. " +
					"Return the requested structured result, including verification evidence and any blockers.\n" +
					string(briefJSON),
			},
		},
		"outputSchema": resultSchema,
	}
	if request.Selection.ReasoningEffort != "" {
		params["effort"] = request.Selection.ReasoningEffort
	}
	err = a.rpc.call(ctx, "turn/start", params, &response)
	if err != nil {
		return worker.Observation{}, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	workerSession.handle.TurnID = response.Turn.ID
	// A fast completion can arrive before the turn/start response is consumed.
	if !terminal(workerSession.observation.Worker.State) {
		workerSession.observation.Worker.State = state(response.Turn.Status)
	}
	return snapshot(workerSession), nil
}

func (a *Adapter) Inspect(_ context.Context, address worker.Address) (worker.Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	workerSession, err := a.find(address)
	if err != nil {
		return worker.Observation{}, err
	}
	return snapshot(workerSession), nil
}

func (a *Adapter) History(ctx context.Context, address worker.Address, cursor string) (worker.ConversationPage, error) {
	if cursor != "" {
		return worker.ConversationPage{}, worker.ErrUnsupported
	}
	observation, err := a.Inspect(ctx, address)
	return worker.ConversationPage{ConversationID: observation.Conversation.ID, Entries: observation.Entries}, err
}

func (a *Adapter) Recover(ctx context.Context, request worker.StartRequest, observation worker.Observation) (worker.Observation, error) {
	var nativeHandle handle
	if err := json.Unmarshal(observation.Handle, &nativeHandle); err != nil {
		return observation, err
	}
	if nativeHandle.ThreadID == "" {
		return observation, errors.New("missing saved Codex thread")
	}
	a.mu.Lock()
	workerSession := &session{request: request, observation: observation, handle: nativeHandle}
	a.sessions[observation.Worker.ID] = workerSession
	a.threads[nativeHandle.ThreadID] = observation.Worker.ID
	a.mu.Unlock()
	// Reading history never executes a new turn. Page the latest turn with full items.
	var response struct{ Data []nativeTurn }
	params := map[string]any{
		"threadId":      nativeHandle.ThreadID,
		"limit":         1,
		"sortDirection": "desc",
		"itemsView":     "full",
	}
	if err := a.rpc.call(ctx, "thread/turns/list", params, &response); err != nil {
		return observation, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(response.Data) == 0 {
		workerSession.observation.Worker.State = core.StateUnknown
		return snapshot(workerSession), nil
	}
	turn := response.Data[0]
	if nativeHandle.TurnID != "" && turn.ID != nativeHandle.TurnID {
		workerSession.observation.Worker.State = core.StateUnknown
		return snapshot(workerSession), nil
	}
	workerSession.handle.TurnID = turn.ID
	for _, item := range turn.Items {
		applyItem(workerSession, item, time.Time{})
	}
	workerSession.observation.Worker.State = state(turn.Status)
	if workerSession.observation.Worker.State == core.StateRunning || workerSession.observation.Worker.State == core.StateCancelled {
		workerSession.observation.Worker.State = core.StateUnknown
	}
	if workerSession.observation.Worker.State == core.StateCompleted {
		finishResult(workerSession)
	}
	if turn.Error != nil {
		workerSession.observation.Result = &core.Result{Blockers: []string{turn.Error.Message}}
	}
	return snapshot(workerSession), nil
}

func (a *Adapter) Cancel(ctx context.Context, address worker.Address) (worker.Cancellation, error) {
	a.mu.Lock()
	workerSession, err := a.find(address)
	if err != nil {
		a.mu.Unlock()
		return worker.Cancellation{}, err
	}
	nativeHandle := workerSession.handle
	a.mu.Unlock()
	if nativeHandle.TurnID == "" {
		return worker.Cancellation{}, errors.New("turn identity is unknown; cancellation cannot be confirmed")
	}
	if err = a.rpc.call(ctx, "turn/interrupt", map[string]any{"threadId": nativeHandle.ThreadID, "turnId": nativeHandle.TurnID}, nil); err != nil {
		return worker.Cancellation{}, err
	}
	return worker.Cancellation{Requested: true, Evidence: "turn/interrupt accepted; awaiting turn/completed"}, nil
}

func (a *Adapter) Send(_ context.Context, message worker.Message) (worker.Delivery, error) {
	return worker.Delivery{MessageID: message.ID, State: worker.DeliveryUnsupported}, worker.ErrUnsupported
}

func (a *Adapter) Resume(context.Context, worker.Address) (worker.Observation, error) {
	return worker.Observation{}, worker.ErrUnsupported
}

func (a *Adapter) Updates() <-chan worker.Update { return a.updates }
func (a *Adapter) Close() error {
	// Shutdown can be requested by several cleanup paths; close the channel once.
	a.once.Do(func() {
		close(a.stop)
		a.rpc.close()
	})
	return nil
}

func (a *Adapter) find(address worker.Address) (*session, error) {
	workerSession := a.sessions[address.WorkerID]
	if workerSession == nil ||
		workerSession.observation.Conversation.ID != address.ConversationID ||
		workerSession.observation.Worker.RuntimeID != address.RuntimeID {
		return nil, errors.New("unknown worker address")
	}
	return workerSession, nil
}

// snapshot copies the session's mutable slices and pointers before another
// goroutine receives them. The adapter must hold its mutex while copying.
func snapshot(workerSession *session) worker.Observation {
	workerSession.observation.Handle, _ = json.Marshal(workerSession.handle)
	data, _ := json.Marshal(workerSession.observation)
	var observation worker.Observation
	_ = json.Unmarshal(data, &observation)
	return observation
}

func terminal(state core.ExecutionState) bool {
	return state == core.StateCompleted || state == core.StateFailed || state == core.StateCancelled
}

func state(status string) core.ExecutionState {
	switch status {
	case "inProgress":
		return core.StateRunning
	case "completed":
		return core.StateCompleted
	case "failed":
		return core.StateFailed
	case "interrupted":
		return core.StateCancelled
	default:
		return core.StateUnknown
	}
}

type nativeItem struct {
	ID      string                        `json:"id"`
	Type    string                        `json:"type"`
	Text    string                        `json:"text"`
	Phase   string                        `json:"phase"`
	Content []struct{ Type, Text string } `json:"content"`
}
type nativeTurn struct {
	ID     string       `json:"id"`
	Status string       `json:"status"`
	Items  []nativeItem `json:"items"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func applyItem(workerSession *session, item nativeItem, completedAt time.Time) {
	var speaker, text string
	switch item.Type {
	case "userMessage":
		speaker = "user"
		for _, part := range item.Content {
			if part.Type == "text" {
				text += part.Text
			}
		}
	case "agentMessage":
		speaker = "worker"
		text = item.Text
	default:
		return
	}
	for i, entry := range workerSession.observation.Entries {
		if entry.ID == core.ID(item.ID) {
			workerSession.observation.Entries[i].Text = text
			return
		}
	}
	entry := worker.ConversationEntry{
		ID:      core.ID(item.ID),
		At:      completedAt,
		Speaker: speaker,
		Text:    text,
	}
	workerSession.observation.Entries = append(workerSession.observation.Entries, entry)
}

func finishResult(workerSession *session) {
	for i := len(workerSession.observation.Entries) - 1; i >= 0; i-- {
		entry := workerSession.observation.Entries[i]
		if entry.Speaker != "worker" {
			continue
		}
		var result core.Result
		if err := json.Unmarshal([]byte(entry.Text), &result); err != nil {
			result.Summary = entry.Text
			result.Blockers = []string{"Worker did not return the requested structured result"}
		}
		workerSession.observation.Result = &result
		return
	}
	workerSession.observation.Result = &core.Result{Blockers: []string{"Completed turn had no final worker message"}}
}

// listen is the adapter's event loop. RPC responses are handled separately by
// rpc.call; notifications update the appropriate worker session here.
func (a *Adapter) listen() {
	defer close(a.updates)
	for {
		select {
		case <-a.stop:
			return
		case <-a.rpc.done:
			a.markDisconnected()
			return
		case notification := <-a.rpc.notifications:
			if !a.handleNotification(notification) {
				return
			}
		}
	}
}

func (a *Adapter) markDisconnected() {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, workerSession := range a.sessions {
		if terminal(workerSession.observation.Worker.State) {
			continue
		}
		workerSession.observation.Worker.State = core.StateUnknown
		if !a.emit(workerSession, "runtime.disconnected", "Codex App Server disconnected; reconcile before retrying") {
			return
		}
	}
}

// nativeNotification contains the fields used across the supported Codex events.
// Each event fills only the fields that belong to its method.
type nativeNotification struct {
	ThreadID      string     `json:"threadId"`
	TurnID        string     `json:"turnId"`
	Turn          nativeTurn `json:"turn"`
	Item          nativeItem `json:"item"`
	CompletedAtMs int64      `json:"completedAtMs"`
	TokenUsage    struct {
		Total struct {
			Input  *int64 `json:"inputTokens"`
			Output *int64 `json:"outputTokens"`
			Cached *int64 `json:"cachedInputTokens"`
		} `json:"total"`
	} `json:"tokenUsage"`
}

// handleNotification returns false when shutdown prevents delivery of an update.
func (a *Adapter) handleNotification(notification envelope) bool {
	var params nativeNotification
	if json.Unmarshal(notification.Params, &params) != nil {
		return true
	}

	a.mu.Lock()
	defer a.mu.Unlock()
	workerID := a.threads[params.ThreadID]
	workerSession := a.sessions[workerID]
	if workerSession == nil {
		return true
	}

	switch notification.Method {
	case "turn/started":
		workerSession.handle.TurnID = params.Turn.ID
		if !terminal(workerSession.observation.Worker.State) {
			workerSession.observation.Worker.State = core.StateRunning
		}
	case "item/completed":
		applyItem(workerSession, params.Item, time.UnixMilli(params.CompletedAtMs).UTC())
	case "turn/completed":
		workerSession.handle.TurnID = params.Turn.ID
		for _, item := range params.Turn.Items {
			applyItem(workerSession, item, time.Time{})
		}
		workerSession.observation.Worker.State = state(params.Turn.Status)
		if params.Turn.Status == "completed" {
			finishResult(workerSession)
		}
		if params.Turn.Error != nil {
			workerSession.observation.Result = &core.Result{Blockers: []string{params.Turn.Error.Message}}
		}
	case "thread/tokenUsage/updated":
		totals := params.TokenUsage.Total
		workerSession.observation.Usage = []core.Usage{
			{
				AttemptID:    workerSession.observation.Worker.AttemptID,
				Resource:     workerSession.observation.Worker.Resource,
				Source:       core.MeasurementObserved,
				InputTokens:  totals.Input,
				OutputTokens: totals.Output,
				CachedTokens: totals.Cached,
				ObservedAt:   time.Now().UTC(),
			},
		}
	default:
		return true
	}
	return a.emit(workerSession, notification.Method, string(notification.Params))
}

func (a *Adapter) emit(workerSession *session, kind, evidence string) bool {
	select {
	case a.updates <- worker.Update{Observation: snapshot(workerSession), Kind: kind, Evidence: evidence}:
		return true
	case <-a.stop:
		return false
	}
}

// A constrained result captures artifacts and verification without scraping prose.
var resultSchema = json.RawMessage(`{
  "type": "object",
  "additionalProperties": false,
  "required": [
    "Summary",
    "Artifacts",
    "Verification",
    "Blockers"
  ],
  "properties": {
    "Summary": {
      "type": "string"
    },
    "Artifacts": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "URI",
          "Description"
        ],
        "properties": {
          "URI": {
            "type": "string"
          },
          "Description": {
            "type": "string"
          }
        }
      }
    },
    "Verification": {
      "type": "array",
      "items": {
        "type": "object",
        "additionalProperties": false,
        "required": [
          "Command",
          "Outcome",
          "Evidence"
        ],
        "properties": {
          "Command": {
            "type": "string"
          },
          "Outcome": {
            "type": "string"
          },
          "Evidence": {
            "type": "array",
            "items": {
              "type": "object",
              "additionalProperties": false,
              "required": [
                "URI",
                "Description"
              ],
              "properties": {
                "URI": {
                  "type": "string"
                },
                "Description": {
                  "type": "string"
                }
              }
            }
          }
        }
      }
    },
    "Blockers": {
      "type": "array",
      "items": {
        "type": "string"
      }
    }
  }
}`)
