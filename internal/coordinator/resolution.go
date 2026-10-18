package coordinator

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
)

var ErrResolutionBlocked = errors.New("attempt resolution blocked")

// ResolveRequest targets an attempt, so a delayed or repeated command cannot
// accidentally resolve a newer attempt. Repeating a retry returns its successor.
type ResolveRequest struct {
	AttemptID core.ID
	Action    string // reconcile (default), cancel, or retry
	Reason    string
}

type Resolution struct {
	Action         string
	Reason         string
	Evidence       string
	At             time.Time
	RetryAttemptID core.ID
}

// reconcile reads native history only. It preserves the durable launch phase
// when evidence is inconclusive, including the prepared-before-start checkpoint.
// The caller holds s.mu. A read error is never execution failure evidence.
func (s *Service) reconcile(ctx context.Context, record *Record) error {
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	saved := record.Observation
	saved.Recovery = nil
	observation, err := s.adapter.Recover(callCtx, record.Start, saved)
	if err == nil && (observation.Worker.ID != saved.Worker.ID ||
		observation.Worker.AttemptID != saved.Worker.AttemptID ||
		observation.Worker.TaskID != saved.Worker.TaskID ||
		observation.Worker.RuntimeID != saved.Worker.RuntimeID ||
		observation.Worker.ConversationID != saved.Worker.ConversationID ||
		observation.Conversation != saved.Conversation) {
		err = errors.New("adapter recovery identity mismatch")
	}
	if err != nil {
		record.Observation.Recovery = nil
		record.Observation.Worker.State = core.StateUnknown
		record.Diagnostic = "Native reconciliation unavailable: " + err.Error()
		return err
	}
	record.Observation = observation
	record.Diagnostic = "Reconciled saved worker without executing a new turn"
	if observation.Recovery != nil {
		record.Diagnostic = observation.Recovery.Detail
	}
	if isTerminal(observation.Worker.State) {
		record.Phase = "terminal"
	} else {
		record.Observation.Worker.State = core.StateUnknown
	}
	return nil
}

// neverStarted uses the transactional intent journal, including journals written
// before attempt snapshots existed. Empty native history alone is insufficient:
// a turn/start request might have executed without persisting a visible turn.
func (s *Service) neverStarted(record Record) (bool, error) {
	events, err := s.store.Events(record.Attempt.WorkerID)
	if err != nil {
		return false, err
	}
	prepared := record.Phase == "preparing" || record.Phase == "prepared"
	for _, event := range events {
		switch event.Kind {
		case "worker.start_intent", "worker.start_observed", "turn/started", "turn/completed":
			return false, nil
		case "worker.prepare_intent":
			prepared = true
		}
	}
	return prepared, nil
}

// ResolveTask performs deliberate operator recovery. It never calls Start or
// Prepare. Only an explicit, safe retry queues a fresh attempt for the ordinary
// execution loop. The old attempt, its evidence, and the retry link commit with
// the successor in one transaction.
func (s *Service) ResolveTask(ctx context.Context, id core.ID, request ResolveRequest) (Record, error) {
	if request.Action == "" {
		request.Action = "reconcile"
	}
	if request.AttemptID == "" || (request.Action != "reconcile" && request.Action != "retry" && request.Action != "cancel") {
		return Record{}, errors.New("resolution requires an attempt ID and action reconcile, cancel, or retry")
	}
	if request.Action != "reconcile" && strings.TrimSpace(request.Reason) == "" {
		return Record{}, errors.New("retry and cancel resolution require an operator reason")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var record Record
	if err := s.store.Get("task", id, &record); err != nil {
		return record, err
	}
	if record.Attempt.ID != request.AttemptID {
		var previous Record
		if err := s.store.Get("attempt", request.AttemptID, &previous); err == nil &&
			previous.Attempt.TaskID == id && previous.Resolution != nil &&
			previous.Resolution.Action == request.Action && previous.Resolution.Reason == request.Reason &&
			previous.Resolution.RetryAttemptID != "" {
			var successor Record
			err := s.store.Get("attempt", previous.Resolution.RetryAttemptID, &successor)
			return successor, err
		}
		return record, fmt.Errorf("%w: stale attempt ID; current attempt is %s", ErrResolutionBlocked, record.Attempt.ID)
	}
	state := record.Observation.Worker.State
	if state != core.StateUnknown && !isTerminal(state) {
		return record, fmt.Errorf("%w: attempt is %s; only unknown or terminal attempts can be resolved", ErrResolutionBlocked, state)
	}
	if state == core.StateUnknown {
		noStart, err := s.neverStarted(record)
		if err != nil {
			return record, err
		}
		nativeErr := s.reconcile(ctx, &record)
		// The adapter's preparation contract excludes inference. A lost Prepare
		// response may leave an orphan conversation, but no uncertain model turn.
		if noStart && (nativeErr != nil || (record.Observation.Recovery != nil && record.Observation.Recovery.NoTurn)) {
			record.NotStarted = true
			record.Observation.Worker.State = core.StateFailed
			record.Phase = "terminal"
			record.Diagnostic = "Durable preparation checkpoint and journal prove no start was requested. " + record.Diagnostic
		}
		if err := s.save(&record, "worker.reconciled", record.Diagnostic); err != nil {
			return record, err
		}
		s.signal()
	}
	if !isTerminal(record.Observation.Worker.State) {
		record.Diagnostic += "; capacity remains reserved; retry refused until native terminal evidence or proof of no start is available"
		if err := s.save(&record, "worker.resolution_blocked", record.Diagnostic); err != nil {
			return record, err
		}
		return record, fmt.Errorf("%w: %s", ErrResolutionBlocked, record.Diagnostic)
	}
	if request.Action == "retry" {
		if record.Observation.Worker.State == core.StateCompleted {
			return record, fmt.Errorf("%w: native completion recovered; inspect task result instead of retrying", ErrResolutionBlocked)
		}
		if s.unavailable {
			return record, fmt.Errorf("%w: worker runtime unavailable; restart coordinator before retrying", ErrResolutionBlocked)
		}
		if !reflect.DeepEqual(record.Request.Selection.Resource, s.catalog.Resource) {
			return record, fmt.Errorf("%w: restart with the attempt's original catalog resource before retrying", ErrResolutionBlocked)
		}
		var assignment core.Assignment
		if err := s.store.Get("assignment", record.Request.Task.AssignmentID, &assignment); err != nil {
			return record, err
		}
		if assignment.State != core.AssignmentOpen {
			return record, fmt.Errorf("%w: assignment is closed", ErrResolutionBlocked)
		}
		next := newPendingRecord(record.Request)
		next.Attempt.Number = record.Attempt.Number + 1
		record.Resolution = &Resolution{Action: request.Action, Reason: request.Reason, Evidence: record.Diagnostic, At: time.Now().UTC(), RetryAttemptID: next.Attempt.ID}
		err := s.store.Update(func(tx *store.Tx) error {
			if err := saveRecord(tx, &record, "worker.resolved", resolutionDetail(record)); err != nil {
				return err
			}
			return saveRecord(tx, &next, "worker.retry_queued", "Explicit retry of attempt "+string(record.Attempt.ID)+": "+request.Reason)
		})
		if err != nil {
			return record, err
		}
		s.signal()
		return next, nil
	}
	// Cancel closes an unstarted attempt. Existing native terminal outcomes are
	// retained, so an operator cannot turn recovered completion into cancellation.
	if request.Action == "cancel" && record.NotStarted {
		record.Observation.Worker.State = core.StateCancelled
		record.CancelRequested = true
	}
	if record.Resolution == nil || record.Resolution.Action != request.Action || record.Resolution.Reason != request.Reason {
		record.Resolution = &Resolution{Action: request.Action, Reason: request.Reason, Evidence: record.Diagnostic, At: time.Now().UTC()}
		if err := s.save(&record, "worker.resolved", resolutionDetail(record)); err != nil {
			return record, err
		}
	}
	s.signal()
	return record, nil
}

func resolutionDetail(record Record) string {
	return fmt.Sprintf("attempt %s: %s; reason: %s; evidence: %s; retry attempt: %s", record.Attempt.ID,
		record.Resolution.Action, record.Resolution.Reason, record.Resolution.Evidence, record.Resolution.RetryAttemptID)
}
