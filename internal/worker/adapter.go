// Package worker defines the boundary implemented by worker harness adapters.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"agentworkyard/internal/core"
)

var ErrUnsupported = errors.New("operation unsupported by adapter")

// ErrRejected identifies a native refusal proving the requested operation did not start.
// Transport errors and timeouts are ambiguous and must never be classified as rejections.
var ErrRejected = errors.New("operation rejected by adapter")

// Address is coordinator-owned and remains stable independently of native handles.
type Address struct {
	WorkerID       core.ID
	ConversationID core.ID
	RuntimeID      core.ID
}

type StartRequest struct {
	WorkerID       core.ID
	ConversationID core.ID
	AttemptID      core.ID
	TaskID         core.ID
	ProfileID      string
	Instructions   string
	Brief          core.TaskBrief
	Selection      core.Selection
	WorkingDir     string
}

type Message struct {
	ID     core.ID
	To     Address
	Sender string // User and supervisor sources remain distinguishable.
	Text   string
}

type DeliveryState string

const (
	DeliveryQueued      DeliveryState = "queued"
	DeliveryAccepted    DeliveryState = "accepted"
	DeliveryConfirmed   DeliveryState = "confirmed"
	DeliveryUnsupported DeliveryState = "unsupported"
	DeliveryFailed      DeliveryState = "failed"
	DeliveryUnknown     DeliveryState = "unknown"
)

type DeliveryMode string

const (
	DeliveryLive         DeliveryMode = "live"
	DeliveryContinuation DeliveryMode = "continuation"
	DeliveryQueue        DeliveryMode = "queue"
)

// Accepted means the adapter accepted responsibility, not that the worker saw it.
// Confirmed requires native delivery evidence. Ambiguous outcomes remain unknown.
type Delivery struct {
	MessageID core.ID
	State     DeliveryState
	Mode      DeliveryMode
	Evidence  string
}

type Cancellation struct {
	Requested bool
	Confirmed bool // A successful interrupt request alone is not terminal confirmation.
	Evidence  string
}

type Observation struct {
	Worker       core.Worker
	Conversation core.Conversation
	Result       *core.Result
	Usage        []core.Usage
	Limits       []core.RateLimit
	// Handle is an opaque adapter checkpoint. The coordinator persists it without
	// interpreting native thread/turn identifiers.
	Handle  json.RawMessage
	Entries []ConversationEntry
	// Recovery records native history evidence. NoTurn means history was empty,
	// not proof that an unacknowledged start never executed.
	Recovery *RecoveryEvidence
}

type RecoveryEvidence struct {
	NoTurn bool
	Detail string
}

type Update struct {
	Observation Observation
	Kind        string
	Evidence    string
}

type ConversationEntry struct {
	ID      core.ID
	At      time.Time
	Speaker string
	Text    string
}

type ConversationPage struct {
	ConversationID core.ID
	Entries        []ConversationEntry
	NextCursor     string
}

// Adapter methods target logical workers, never the supervisor's native children.
// Capabilities must be checked before optional operations. Unsupported operations
// return ErrUnsupported; request context cancellation stops the API call, not the
// logical worker. Cancel is the explicit lifecycle operation. Observation errors
// must not be converted into worker completion or automatic re-execution.
type Adapter interface {
	Runtime(context.Context, core.ID) (core.Runtime, error)
	// Prepare allocates a conversation without executing the brief. Persist its
	// checkpoint and a start intent before Start can launch a model turn.
	Prepare(context.Context, StartRequest) (Observation, error)
	Start(context.Context, Address) (Observation, error)
	// Recover reconciles saved native state without launching another turn.
	Recover(context.Context, StartRequest, Observation) (Observation, error)
	Inspect(context.Context, Address) (Observation, error)
	History(context.Context, Address, string) (ConversationPage, error)
	Send(context.Context, Message) (Delivery, error)
	Cancel(context.Context, Address) (Cancellation, error)
	Resume(context.Context, Address) (Observation, error)
	// Updates carries full snapshots in order. It must never silently drop state.
	Updates() <-chan Update
	Close() error
}
