package core

import "time"

// Support's zero value means unknown, never an asserted capability.
type Support string

const (
	SupportUnknown     Support = ""
	SupportAvailable   Support = "available"
	SupportUnavailable Support = "unavailable"
)

type MessageCapabilities struct {
	Live         Support
	Continuation Support
	Queue        Support
}

type RuntimeCapabilities struct {
	IsolatedConversations       Support
	IndependentControl          Support
	ConversationInspection      Support
	Cancellation                Support
	Resume                      Support
	Recovery                    Support
	SharedHosting               Support
	Messages                    MessageCapabilities
	UsageReporting              Support
	RateLimitReporting          Support
	RestrictUnmanagedDelegation Support
	Limitations                 []string
}

// Runtime identity does not imply a process or session layout. An adapter owns
// its native handles; a shared runtime may host several independently managed workers.
type Runtime struct {
	ID           ID
	MachineID    ID
	Harness      string
	Capabilities RuntimeCapabilities
}

// ResourceRef preserves machine -> runtime/harness -> provider/account -> model.
// AccountID is an opaque local reference, never a credential.
type ResourceRef struct {
	MachineID ID
	RuntimeID ID
	Harness   string
	Provider  string
	AccountID ID
	Model     string
}

// Nil capacity or usage values mean unavailable, not zero or unlimited.
type Capacity struct {
	MaxConcurrent *int
	InUse         *int
	ObservedAt    time.Time
}

type ModelOption struct {
	Resource     ResourceRef
	Capabilities []string
	Availability Support
	Capacity     Capacity
}

// Selection is a supervisor decision; M1 does not implement scheduling or fallback.
type Selection struct {
	// Empty inherits the harness default; adapters validate supported values.
	ReasoningEffort string
	Resource        ResourceRef
	Rationale       string
	AllowFallback   bool
}

type MeasurementSource string

const (
	MeasurementUnknown   MeasurementSource = ""
	MeasurementObserved  MeasurementSource = "observed"
	MeasurementEstimated MeasurementSource = "estimated"
)

type Usage struct {
	AttemptID    ID
	Resource     ResourceRef
	Source       MeasurementSource
	InputTokens  *int64
	OutputTokens *int64
	CachedTokens *int64
	ObservedAt   time.Time
}

type RateLimit struct {
	Resource   ResourceRef
	Remaining  *int64
	RetryAfter *time.Time
	ObservedAt time.Time
	Evidence   string
}
