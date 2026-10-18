package coordinator

import (
	"context"

	"agentworkyard/internal/core"
	"agentworkyard/internal/worker"
)

// TaskRequest carries the supervisor's engineering choices, not a request for
// the coordinator to reason about decomposition or model selection.
type TaskRequest struct {
	Task      core.Task
	Selection core.Selection
}

// Control is the initial supervisor-facing contract, not an implemented service.
// Assignment operations are metadata operations and never launch a model.
// Persistence, idempotency, policy enforcement and recovery are M2+ work.
type Control interface {
	PutAssignment(context.Context, core.Assignment) error
	SubmitTask(context.Context, TaskRequest) (core.ID, error)
	InspectWorker(context.Context, worker.Address) (worker.Observation, error)
	SendMessage(context.Context, worker.Message) (worker.Delivery, error)
	CancelWorker(context.Context, worker.Address) (worker.Cancellation, error)
	Resources(context.Context) ([]core.ModelOption, error)
}
