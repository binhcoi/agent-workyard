package coordinator

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
	"agentworkyard/internal/worker"
)

type Catalog struct {
	Resource      core.ResourceRef
	MaxConcurrent int
}

// Record is the durable snapshot of one task and its execution attempt.
type Record struct {
	Request     TaskRequest
	Start       worker.StartRequest
	Attempt     core.Attempt
	Observation worker.Observation
	// Phase tracks launch checkpoints; Observation.Worker.State tracks execution.
	Phase           string
	CancelRequested bool
	Diagnostic      string
}

// Service owns task lifecycle independently of any submitting client.
type Service struct {
	// mu protects lifecycle changes shared by HTTP handlers and the execution loop.
	mu          sync.Mutex
	store       *store.Store
	adapter     worker.Adapter
	catalog     Catalog
	ID          core.ID
	wake        chan struct{}
	errors      chan error
	unavailable bool
}

func New(stateStore *store.Store, adapter worker.Adapter, catalog Catalog) (*Service, error) {
	if catalog.MaxConcurrent < 1 ||
		catalog.Resource.Model == "" ||
		catalog.Resource.MachineID == "" ||
		catalog.Resource.RuntimeID == "" ||
		catalog.Resource.Harness == "" {
		return nil, errors.New("catalog requires a local machine, runtime, harness, model, and positive concurrency")
	}
	service := &Service{
		store:   stateStore,
		adapter: adapter,
		catalog: catalog,
		wake:    make(chan struct{}, 1),
		errors:  make(chan error, 1),
	}
	if err := stateStore.Get("meta", "coordinator", &service.ID); errors.Is(err, sql.ErrNoRows) {
		service.ID = newID()
		if err = stateStore.Update(func(tx *store.Tx) error { return tx.Put("meta", "coordinator", service.ID) }); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	return service, nil
}

func newID() core.ID {
	var randomBytes [16]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		panic(err)
	}
	return core.ID(hex.EncodeToString(randomBytes[:]))
}

func (s *Service) Errors() <-chan error { return s.errors }
func (s *Service) PutProject(_ context.Context, project core.Project) error {
	if project.ID == "" || !filepath.IsAbs(project.Root) || project.DefaultMachineID != s.catalog.Resource.MachineID {
		return errors.New("project needs ID, absolute root, and the configured local machine")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Update(func(tx *store.Tx) error {
		var existingProject core.Project
		err := tx.Get("project", project.ID, &existingProject)
		if err == nil && !reflect.DeepEqual(existingProject, project) {
			return errors.New("project already exists with different configuration")
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return tx.Put("project", project.ID, project)
	})
}

func (s *Service) PutAssignment(_ context.Context, assignment core.Assignment) error {
	if assignment.ID == "" || assignment.ProjectID == "" || assignment.ManagingSupervisorID == "" || assignment.Objective == "" {
		return errors.New("assignment needs ID, project, managing supervisor, and objective")
	}
	if assignment.OwningCoordinatorID == "" {
		assignment.OwningCoordinatorID = s.ID
	}
	if assignment.OwningCoordinatorID != s.ID {
		return errors.New("assignment belongs to another coordinator")
	}
	if assignment.State == "" {
		assignment.State = core.AssignmentOpen
	}
	if assignment.State != core.AssignmentOpen && assignment.State != core.AssignmentClosed {
		return errors.New("invalid assignment state")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.store.Update(func(tx *store.Tx) error {
		var project core.Project
		if err := tx.Get("project", assignment.ProjectID, &project); err != nil {
			return err
		}
		var existingAssignment core.Assignment
		err := tx.Get("assignment", assignment.ID, &existingAssignment)
		if err == nil {
			if existingAssignment.OwningCoordinatorID != assignment.OwningCoordinatorID || existingAssignment.ProjectID != assignment.ProjectID {
				return errors.New("assignment ownership and project cannot change")
			}
			if existingAssignment.State == core.AssignmentClosed && !reflect.DeepEqual(existingAssignment, assignment) {
				return errors.New("closed assignment cannot be revised")
			}
			if reflect.DeepEqual(existingAssignment, assignment) {
				return nil
			}
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := tx.Put("assignment", assignment.ID, assignment); err != nil {
			return err
		}
		return tx.Event("", "assignment.updated", string(assignment.ID))
	})
}

func (s *Service) SubmitTask(_ context.Context, request TaskRequest) (core.ID, error) {
	if request.Task.ID == "" || request.Task.Brief.Objective == "" || request.Task.ProfileID == "" || request.Instructions == "" {
		return "", errors.New("task needs ID, objective, worker profile, and instructions")
	}
	if len(request.Task.Dependencies) > 0 {
		return "", fmt.Errorf("task dependencies: %w", worker.ErrUnsupported)
	}
	if request.Selection.AllowFallback {
		return "", fmt.Errorf("resource fallback: %w", worker.ErrUnsupported)
	}
	if !reflect.DeepEqual(request.Selection.Resource, s.catalog.Resource) {
		return "", errors.New("selection must match the configured local catalog resource")
	}
	// Execution state is assigned by the coordinator, never by the caller.
	request.Task.State = ""
	request.Task.Acceptance = ""
	s.mu.Lock()
	defer s.mu.Unlock()
	// A retried submission must return the original worker, not launch another.
	var existingRecord Record
	if err := s.store.Get("task", request.Task.ID, &existingRecord); err == nil {
		if !reflect.DeepEqual(existingRecord.Request, request) {
			return "", errors.New("task ID already used with a different request")
		}
		return existingRecord.Observation.Worker.ID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	if s.unavailable {
		return "", errors.New("worker runtime unavailable; restart the coordinator before submitting new work")
	}
	var assignment core.Assignment
	if err := s.store.Get("assignment", request.Task.AssignmentID, &assignment); err != nil {
		return "", err
	}
	if assignment.State != core.AssignmentOpen {
		return "", errors.New("assignment is closed")
	}
	var project core.Project
	if err := s.store.Get("project", assignment.ProjectID, &project); err != nil {
		return "", err
	}
	if filepath.Clean(request.WorkingDir) != project.Root {
		return "", errors.New("task working directory must equal its project root")
	}
	record := newPendingRecord(request)
	if err := s.save(&record, "task.submitted", ""); err != nil {
		return "", err
	}

	// Wake the execution loop only after the submission is durable. The HTTP
	// caller can now disconnect without affecting the worker's lifetime.
	s.signal()
	return record.Observation.Worker.ID, nil
}

// newPendingRecord gives a task stable worker, conversation and attempt identities.
// It only builds data; no database write or harness operation happens here.
func newPendingRecord(request TaskRequest) Record {
	workerID := newID()
	conversationID := newID()
	attemptID := newID()

	startRequest := worker.StartRequest{
		WorkerID:       workerID,
		ConversationID: conversationID,
		AttemptID:      attemptID,
		TaskID:         request.Task.ID,
		ProfileID:      request.Task.ProfileID,
		Instructions:   request.Instructions,
		Brief:          request.Task.Brief,
		Selection:      request.Selection,
		WorkingDir:     request.WorkingDir,
	}
	observation := worker.Observation{
		Worker: core.Worker{
			ID:             workerID,
			TaskID:         request.Task.ID,
			AttemptID:      attemptID,
			ProfileID:      request.Task.ProfileID,
			ConversationID: conversationID,
			RuntimeID:      request.Selection.Resource.RuntimeID,
			Resource:       request.Selection.Resource,
			State:          core.StatePending,
		},
		Conversation: core.Conversation{
			ID:       conversationID,
			WorkerID: workerID,
		},
	}
	return Record{
		Request: request,
		Start:   startRequest,
		Attempt: core.Attempt{
			ID:       attemptID,
			TaskID:   request.Task.ID,
			WorkerID: workerID,
			Number:   1,
			State:    core.StatePending,
		},
		Observation: observation,
		Phase:       "queued",
	}
}

func (s *Service) Get(id core.ID) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var record Record
	err := s.store.Get("task", id, &record)
	return record, err
}

func (s *Service) Assignments() ([]core.Assignment, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.store.List("assignment")
	if err != nil {
		return nil, err
	}
	assignments := []core.Assignment{}
	for _, data := range rows {
		var assignment core.Assignment
		if err = json.Unmarshal(data, &assignment); err != nil {
			return nil, err
		}
		assignments = append(assignments, assignment)
	}
	return assignments, nil
}

func (s *Service) List() ([]Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records()
}

// records reads stored snapshots. The caller must hold s.mu.
func (s *Service) records() ([]Record, error) {
	rows, err := s.store.List("task")
	if err != nil {
		return nil, err
	}
	records := []Record{}
	for _, data := range rows {
		var record Record
		if err = json.Unmarshal(data, &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (s *Service) Events(id core.ID) ([]store.Event, error) {
	record, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	return s.store.Events(record.Observation.Worker.ID)
}

func (s *Service) Resources(ctx context.Context) ([]core.ModelOption, error) {
	records, err := s.List()
	if err != nil {
		return nil, err
	}
	inUse := 0
	for _, record := range records {
		if occupied(record) {
			inUse++
		}
	}
	maxConcurrent := s.catalog.MaxConcurrent
	s.mu.Lock()
	unavailable := s.unavailable
	s.mu.Unlock()
	availability := core.SupportUnknown
	if unavailable {
		availability = core.SupportUnavailable
	}
	return []core.ModelOption{
		{
			Resource:     s.catalog.Resource,
			Availability: availability,
			Capacity: core.Capacity{
				MaxConcurrent: &maxConcurrent,
				InUse:         &inUse,
				ObservedAt:    time.Now().UTC(),
			},
		},
	}, nil
}

func address(observation worker.Observation) worker.Address {
	return worker.Address{
		WorkerID:       observation.Worker.ID,
		ConversationID: observation.Conversation.ID,
		RuntimeID:      observation.Worker.RuntimeID,
	}
}

func isTerminal(state core.ExecutionState) bool {
	return state == core.StateCompleted || state == core.StateCancelled || state == core.StateFailed
}

func occupied(record Record) bool {
	return record.Phase != "queued" && !isTerminal(record.Observation.Worker.State)
}

// save commits the snapshot, task state and event in the same transaction.
func (s *Service) save(record *Record, kind, detail string) error {
	record.Attempt.State = record.Observation.Worker.State
	return s.store.Update(func(tx *store.Tx) error {
		if err := tx.Put("task", record.Request.Task.ID, record); err != nil {
			return err
		}
		task := record.Request.Task
		task.State = record.Observation.Worker.State
		task.Acceptance = core.AcceptancePending
		if err := tx.Put("task_state", task.ID, task); err != nil {
			return err
		}
		return tx.Event(record.Observation.Worker.ID, kind, detail)
	})
}

// signal coalesces wake-ups: one pending signal is enough to check the queue.
func (s *Service) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run belongs to the coordinator lifetime, never an HTTP request or supervisor.
func (s *Service) Run(ctx context.Context) {
	if err := s.recover(ctx); err != nil {
		s.errors <- err
		return
	}
	s.signal()
	updates := s.adapter.Updates()
	for {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-updates:
			if !ok {
				// Receiving from a closed channel would keep this case ready.
				// A nil channel disables it while inspection stays available.
				updates = nil
				if err := s.disconnected(); err != nil {
					s.errors <- err
					return
				}
				continue
			}
			if err := s.apply(update); err != nil {
				s.errors <- err
				return
			}
			s.signal()
		case <-s.wake:
			if err := s.dispatch(ctx); err != nil {
				s.errors <- err
				return
			}
		}
	}
}

func (s *Service) recover(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := s.records()
	if err != nil {
		return err
	}
	for _, record := range records {
		if isTerminal(record.Observation.Worker.State) || record.Phase == "queued" {
			continue
		}
		// Prepared conversations have not launched a turn. No native inference is
		// repeated here, even if a crash occurred after an unacknowledged start.
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		observation, err := s.adapter.Recover(callCtx, record.Start, record.Observation)
		cancel()
		if err != nil {
			record.Observation.Worker.State = core.StateUnknown
			record.Diagnostic = err.Error()
		} else {
			record.Observation = observation
			record.Diagnostic = "Reconciled saved worker without executing a new turn"
		}
		record.Phase = "reconciled"
		if !isTerminal(record.Observation.Worker.State) {
			record.Observation.Worker.State = core.StateUnknown
		}
		if err = s.save(&record, "worker.recovered", record.Diagnostic); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) dispatch(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return nil
	}
	records, err := s.records()
	if err != nil {
		return err
	}
	inUse := 0
	for _, record := range records {
		if occupied(record) {
			inUse++
		}
	}
	for _, record := range records {
		if record.Phase != "queued" || inUse >= s.catalog.MaxConcurrent {
			continue
		}

		inUse++
		if err := s.launchWorker(ctx, &record); err != nil {
			return err
		}
		if record.Phase == "terminal" {
			// A definite rejection did not use the reserved slot. An unknown
			// outcome keeps its slot because the worker may have started.
			inUse--
		}
	}
	return nil
}

// launchWorker is called with s.mu held. Every external operation has a durable
// intent recorded first, so recovery can identify an interrupted launch.
func (s *Service) launchWorker(ctx context.Context, record *Record) error {
	record.Phase = "preparing"
	if err := s.save(record, "worker.prepare_intent", ""); err != nil {
		return err
	}

	// Prepare creates a conversation without starting model inference.
	prepareCtx, cancelPrepare := context.WithTimeout(ctx, 30*time.Second)
	observation, err := s.adapter.Prepare(prepareCtx, record.Start)
	cancelPrepare()
	if err != nil {
		setLaunchError(record, err)
		return s.save(record, "worker.prepare_unknown", err.Error())
	}

	record.Observation = observation
	record.Phase = "prepared"
	if err := s.save(record, "worker.prepared", ""); err != nil {
		return err
	}

	// The native conversation handle is now durable. Record start intent before
	// asking the harness to execute, since its response could be lost.
	record.Phase = "starting"
	if err := s.save(record, "worker.start_intent", ""); err != nil {
		return err
	}

	startCtx, cancelStart := context.WithTimeout(ctx, 30*time.Second)
	observation, err = s.adapter.Start(startCtx, address(record.Observation))
	cancelStart()
	if err != nil {
		setLaunchError(record, err)
	} else {
		record.Observation = observation
		record.Phase = "active"
	}
	return s.save(record, "worker.start_observed", record.Diagnostic)
}

func setLaunchError(record *Record, err error) {
	record.Observation.Worker.State = core.StateUnknown
	record.Phase = "ambiguous"
	record.Diagnostic = err.Error()
	if errors.Is(err, worker.ErrRejected) {
		// Only a definite refusal proves that this operation did not start.
		record.Observation.Worker.State = core.StateFailed
		record.Phase = "terminal"
	}
}

func (s *Service) apply(update worker.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var record Record
	if err := s.store.Get("task", update.Observation.Worker.TaskID, &record); err != nil {
		return err
	}
	if record.Observation.Worker.ID != update.Observation.Worker.ID {
		return errors.New("adapter observation worker mismatch")
	}
	// A late turn/started must not overwrite a completion captured in Start.
	if isTerminal(record.Observation.Worker.State) && !isTerminal(update.Observation.Worker.State) {
		return nil
	}
	record.Observation = update.Observation
	record.Phase = "active"
	return s.save(&record, update.Kind, update.Evidence)
}

func (s *Service) disconnected() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unavailable = true
	records, err := s.records()
	if err != nil {
		return err
	}
	for _, record := range records {
		if occupied(record) {
			record.Observation.Worker.State = core.StateUnknown
			record.Diagnostic = "Adapter disconnected; no automatic retry"
			if err = s.save(&record, "worker.unknown", record.Diagnostic); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) CancelTask(ctx context.Context, id core.ID) (worker.Cancellation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var record Record
	if err := s.store.Get("task", id, &record); err != nil {
		return worker.Cancellation{}, err
	}
	if isTerminal(record.Observation.Worker.State) {
		return worker.Cancellation{Confirmed: record.Observation.Worker.State == core.StateCancelled, Evidence: "Worker is already terminal"}, nil
	}
	record.CancelRequested = true
	if record.Phase == "queued" {
		record.Phase = "terminal"
		record.Observation.Worker.State = core.StateCancelled
		err := s.save(&record, "worker.cancelled", "Cancelled before execution")
		return worker.Cancellation{Requested: true, Confirmed: err == nil, Evidence: "Cancelled before execution"}, err
	}
	if err := s.save(&record, "worker.cancel_intent", ""); err != nil {
		return worker.Cancellation{}, err
	}
	cancellation, err := s.adapter.Cancel(ctx, address(record.Observation))
	if err != nil {
		record.Diagnostic = err.Error()
		if saveErr := s.save(&record, "worker.cancel_unknown", record.Diagnostic); saveErr != nil {
			return cancellation, saveErr
		}
		return cancellation, err
	}
	if cancellation.Confirmed {
		record.Observation.Worker.State = core.StateCancelled
	}
	if err = s.save(&record, "worker.cancel_requested", cancellation.Evidence); err != nil {
		return cancellation, err
	}
	return cancellation, nil
}

var _ Control = (*Service)(nil)

func (s *Service) InspectWorker(_ context.Context, addressToFind worker.Address) (worker.Observation, error) {
	records, err := s.List()
	if err != nil {
		return worker.Observation{}, err
	}
	for _, record := range records {
		if address(record.Observation) == addressToFind {
			return record.Observation, nil
		}
	}
	return worker.Observation{}, sql.ErrNoRows
}

func (s *Service) SendMessage(_ context.Context, message worker.Message) (worker.Delivery, error) {
	return worker.Delivery{MessageID: message.ID, State: worker.DeliveryUnsupported}, worker.ErrUnsupported
}

func (s *Service) CancelWorker(ctx context.Context, addressToFind worker.Address) (worker.Cancellation, error) {
	records, err := s.List()
	if err != nil {
		return worker.Cancellation{}, err
	}
	for _, record := range records {
		if address(record.Observation) == addressToFind {
			return s.CancelTask(ctx, record.Request.Task.ID)
		}
	}
	return worker.Cancellation{}, sql.ErrNoRows
}
