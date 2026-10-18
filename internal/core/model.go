// Package core defines harness-neutral vocabulary. These records are contracts,
// not a persistence schema or an implemented lifecycle engine.
package core

import "time"

type ID string

type Project struct {
	ID               ID
	Root             string
	DefaultMachineID ID
}

type AssignmentState string

const (
	AssignmentOpen   AssignmentState = "open"
	AssignmentClosed AssignmentState = "closed"
)

// Ownership remains with the coordinator if supervision changes or disconnects.
type Assignment struct {
	ID                   ID
	ProjectID            ID
	OwningCoordinatorID  ID
	ManagingSupervisorID ID
	Objective            string
	State                AssignmentState
}

type TaskBrief struct {
	Objective         string
	Constraints       []string
	Inputs            []Artifact
	Acceptance        []string
	AuthorizedActions []string // Descriptive scope, never an enforcement mechanism.
}

type Task struct {
	ID           ID
	AssignmentID ID
	ProfileID    string
	Brief        TaskBrief
	Dependencies []ID
	State        ExecutionState
	Acceptance   AcceptanceState
}

// A worker-reported result does not imply supervisor acceptance or integration.
type AcceptanceState string

const (
	AcceptancePending  AcceptanceState = "pending"
	AcceptanceAccepted AcceptanceState = "accepted"
	AcceptanceRework   AcceptanceState = "rework"
)

type Attempt struct {
	ID       ID
	TaskID   ID
	WorkerID ID
	Number   int
	State    ExecutionState
}

type ExecutionState string

const (
	StatePending   ExecutionState = "pending"
	StateRunning   ExecutionState = "running"
	StateWaiting   ExecutionState = "waiting"
	StateCompleted ExecutionState = "completed"
	StateFailed    ExecutionState = "failed"
	StateCancelled ExecutionState = "cancelled"
	StateUnknown   ExecutionState = "unknown" // Reconcile ambiguity before retrying.
)

type Worker struct {
	ID             ID
	TaskID         ID
	AttemptID      ID
	ProfileID      string
	ConversationID ID
	RuntimeID      ID
	State          ExecutionState
	Resource       ResourceRef
}

// Each worker owns one separate conversation, even when runtimes are shared.
type Conversation struct {
	ID       ID
	WorkerID ID
}

type Artifact struct {
	URI         string
	Description string
}

type Verification struct {
	Command  string
	Outcome  string
	Evidence []Artifact
}

type Result struct {
	Summary      string
	Artifacts    []Artifact
	Verification []Verification
	Blockers     []string
}

// Event is an observation at the adapter boundary. Durable ordering and storage
// belong to the coordinator, not to a model or the supervisor conversation.
type Event struct {
	ID       ID
	WorkerID ID
	At       time.Time
	Kind     string
	Detail   string
}
