// Package worker defines the boundary implemented by worker harness adapters.
// No implementation or worker launch is supplied in M1.
package worker

import (
	"context"
	"errors"
	"time"

	"agentworkyard/internal/core"
)

var ErrUnsupported = errors.New("operation unsupported by adapter")

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
	Start(context.Context, StartRequest) (Observation, error)
	Inspect(context.Context, Address) (Observation, error)
	History(context.Context, Address, string) (ConversationPage, error)
	Send(context.Context, Message) (Delivery, error)
	Cancel(context.Context, Address) (Cancellation, error)
	Resume(context.Context, Address) (Observation, error)
	Subscribe(context.Context, Address) (<-chan core.Event, error)
}
