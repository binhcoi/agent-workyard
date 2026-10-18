package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"agentworkyard/internal/coordinator"
	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
	"agentworkyard/internal/worker"
)

func pipeRPC(t *testing.T) (*rpc, *os.File) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close(); reader.Close() })
	return &rpc{stdin: writer, writeGate: make(chan struct{}, 1), pending: map[string]chan envelope{}, done: make(chan struct{})}, reader
}

func awaitRPCError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("RPC remained blocked")
		return nil
	}
}

func readFramePrefix(t *testing.T, reader *os.File) []byte {
	t.Helper()
	if err := reader.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	prefix := make([]byte, 1)
	if _, err := io.ReadFull(reader, prefix); err != nil {
		t.Fatal("writer did not enter pipe I/O:", err)
	}
	return prefix
}

func TestRPCCancelledContextDoesNotWrite(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancelled", true: "expired"}[deadline], func(t *testing.T) {
			r, reader := pipeRPC(t)
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			cancel()
			if err := r.call(ctx, "turn/start", nil, nil); !errors.Is(err, ctx.Err()) {
				t.Fatal("cancelled call did not return its context error:", err)
			}
			if len(r.pending) != 0 {
				t.Fatal("cancelled call retained a pending response")
			}
			if err := r.write(context.Background(), map[string]string{"method": "healthy"}); err != nil {
				t.Fatal("pre-write cancellation damaged the connection:", err)
			}
			r.stdin.Close()
			data, err := io.ReadAll(reader)
			if err != nil || string(data) != "{\"method\":\"healthy\"}\n" {
				t.Fatalf("cancelled request reached the pipe: %q %v", data, err)
			}
		})
	}
}

func TestRPCWriterAcquisitionHonorsContext(t *testing.T) {
	r, reader := pipeRPC(t)
	first := make(chan error, 1)
	go func() { first <- r.call(context.Background(), "turn/start", strings.Repeat("x", 1<<20), nil) }()
	prefix := readFramePrefix(t, reader)
	// The first writer owns the gate and is blocked on an incomplete real pipe
	// frame. Cancelling another caller must not wait for or disturb that writer.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() { second <- r.call(ctx, "thread/read", nil, nil) }()
	if err := awaitRPCError(t, second); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-r.done:
		t.Fatal("a cancelled waiter damaged the active writer")
	default:
	}
	r.mu.Lock()
	pending := len(r.pending)
	r.mu.Unlock()
	if pending != 1 {
		t.Fatalf("cancelled waiter leaked: %d pending calls", pending)
	}
	r.finish(errors.New("test disconnect during active write"))
	if err := awaitRPCError(t, first); err == nil || errors.Is(err, worker.ErrRejected) {
		t.Fatal("disconnect was not an ambiguous write failure:", err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || bytes.Contains(append(prefix, data...), []byte("thread/read")) {
		t.Fatal("cancelled waiter was written later:", err)
	}
}

func TestRPCActiveWriteHonorsDeadlineAndCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel_without_deadline", true: "deadline"}[deadline], func(t *testing.T) {
			r, reader := pipeRPC(t)
			ctx, cancel := context.WithCancel(context.Background())
			if deadline {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
			}
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- r.call(ctx, "turn/start", strings.Repeat("x", 1<<20), nil) }()
			prefix := readFramePrefix(t, reader)
			if !deadline {
				cancel()
			}
			<-ctx.Done()
			err := awaitRPCError(t, result)
			if !errors.Is(err, ctx.Err()) || errors.Is(err, worker.ErrRejected) {
				t.Fatal("write failure lost context or became a definite rejection:", err)
			}
			select {
			case <-r.done:
			default:
				t.Fatal("partial frame left the transport reusable")
			}
			if err := r.call(context.Background(), "must-not-write", nil, nil); err == nil {
				t.Fatal("a subsequent request reused the broken stream")
			}
			data, err := io.ReadAll(reader)
			data = append(prefix, data...)
			if err != nil || len(data) == 0 || bytes.Contains(data, []byte("must-not-write")) || bytes.Contains(data, []byte{'\n'}) {
				t.Fatalf("partial request was completed or followed by another frame: bytes=%d err=%v", len(data), err)
			}
			r.mu.Lock()
			pending := len(r.pending)
			r.mu.Unlock()
			if pending != 0 {
				t.Fatal("write failure leaked pending calls")
			}
		})
	}
}

func TestRPCSuccessfulWriteClearsDeadline(t *testing.T) {
	r, reader := pipeRPC(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := r.write(ctx, map[string]int{"first": 1}); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	if err := r.write(context.Background(), map[string]int{"second": 2}); err != nil {
		t.Fatal("previous deadline or cancellation affected the next writer:", err)
	}
	r.stdin.Close()
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "{\"first\":1}\n{\"second\":2}\n" {
		t.Fatalf("frames interleaved or were lost: %q %v", data, err)
	}
}

func TestRPCZeroByteTimeoutKeepsTransportUsable(t *testing.T) {
	r, reader := pipeRPC(t)
	// Fill the OS pipe with complete frames. A small atomic write cannot send
	// any bytes until the reader drains them.
	if err := r.stdin.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	n, err := r.stdin.Write(bytes.Repeat([]byte("{} \n"), 1<<19))
	if !errors.Is(err, os.ErrDeadlineExceeded) || n == 0 || n%4 != 0 {
		t.Fatal("could not fill the pipe with complete frames:", n, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := r.call(ctx, "must-not-write", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-r.done:
		t.Fatal("a zero-byte timeout aborted other workers' transport")
	default:
	}
	if _, err := io.CopyN(io.Discard, reader, int64(n)); err != nil {
		t.Fatal(err)
	}
	if err := r.write(context.Background(), map[string]string{"method": "healthy"}); err != nil {
		t.Fatal("zero-byte timeout damaged the next write:", err)
	}
	r.stdin.Close()
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "{\"method\":\"healthy\"}\n" {
		t.Fatalf("timed-out frame was sent: %q %v", data, err)
	}
}

func TestRPCConcurrentWritesPreserveFrames(t *testing.T) {
	r, reader := pipeRPC(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	read := make(chan error, 1)
	go func() {
		scanner := bufio.NewScanner(reader)
		scanner.Buffer(make([]byte, 64<<10), 1<<20)
		seen := map[int]bool{}
		for range 8 {
			if !scanner.Scan() {
				read <- fmt.Errorf("missing frame: %v", scanner.Err())
				return
			}
			var message struct {
				ID      int
				Payload string
			}
			if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
				read <- err
				return
			}
			if message.ID < 0 || message.ID >= 8 || seen[message.ID] || message.Payload != strings.Repeat(fmt.Sprint(message.ID), 64<<10) {
				read <- errors.New("concurrent frames interleaved or were duplicated")
				return
			}
			seen[message.ID] = true
		}
		read <- nil
	}()
	written := make(chan error, 8)
	for i := range 8 {
		go func() {
			written <- r.write(ctx, map[string]any{"ID": i, "Payload": strings.Repeat(fmt.Sprint(i), 64<<10)})
		}()
	}
	for range 8 {
		if err := awaitRPCError(t, written); err != nil {
			t.Fatal(err)
		}
	}
	if err := awaitRPCError(t, read); err != nil {
		t.Fatal(err)
	}
}

func TestRPCResponseTimeoutKeepsTransportUsable(t *testing.T) {
	a, log := testAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := a.rpc.call(ctx, "test/no-response", nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	select {
	case <-a.rpc.done:
		t.Fatal("response-only timeout aborted a healthy transport")
	default:
	}
	ctx, cancelNext := context.WithTimeout(context.Background(), time.Second)
	defer cancelNext()
	if _, err := a.Prepare(ctx, testRequest("after-timeout", "verify")); err != nil {
		t.Fatal("later request failed after a response timeout:", err)
	}
	data, err := os.ReadFile(log)
	if err != nil || !bytes.Contains(data, []byte(`"method":"test/no-response"`)) {
		t.Fatal("timeout did not exercise a fully written request", err)
	}
}

func TestRealPipeStartWriteDeadline(t *testing.T) {
	a, _ := testAdapter(t)
	request := testRequest("stalled", strings.Repeat("x", 1<<20))
	prepared, err := a.Prepare(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.rpc.call(context.Background(), "test/stall-stdin", nil, nil); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := a.Start(ctx, addr(prepared)); result <- err }()
	if err := awaitRPCError(t, result); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, worker.ErrRejected) {
		t.Fatal("start write was not bounded and ambiguous:", err)
	}
	awaitState(t, a, prepared, core.StateUnknown)
	closed := make(chan struct{})
	go func() { a.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown failed to reap the non-reading peer")
	}
}

func TestCloseUnblocksRPCWriterAndWaiters(t *testing.T) {
	a, _ := testAdapter(t)
	prepared, err := a.Prepare(context.Background(), testRequest("stalled", strings.Repeat("x", 1<<20)))
	if err != nil {
		t.Fatal(err)
	}
	if err := a.rpc.call(context.Background(), "test/stall-stdin", nil, nil); err != nil {
		t.Fatal(err)
	}
	first, second := make(chan error, 1), make(chan error, 1)
	go func() { _, err := a.Start(context.Background(), addr(prepared)); first <- err }()
	waitForBurst(t, func() bool { return len(a.rpc.writeGate) == 1 })
	go func() { second <- a.rpc.call(context.Background(), "thread/read", nil, nil) }()
	closed := make(chan struct{})
	go func() { a.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown blocked on the writer or a waiting caller")
	}
	for _, result := range []chan error{first, second} {
		if err := awaitRPCError(t, result); err == nil {
			t.Fatal("shutdown incorrectly acknowledged an unreceived request")
		}
	}
	a.rpc.mu.Lock()
	pending := len(a.rpc.pending)
	a.rpc.mu.Unlock()
	if pending != 0 {
		t.Fatal("shutdown leaked pending callers")
	}
}

type shortStartDeadline struct {
	*Adapter
	result chan error
}

func (a shortStartDeadline) Prepare(ctx context.Context, request worker.StartRequest) (worker.Observation, error) {
	observation, err := a.Adapter.Prepare(ctx, request)
	if err == nil {
		err = a.rpc.call(ctx, "test/stall-stdin", nil, nil)
	}
	return observation, err
}

func (a shortStartDeadline) Start(ctx context.Context, address worker.Address) (worker.Observation, error) {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	observation, err := a.Adapter.Start(ctx, address)
	a.result <- err
	return observation, err
}

func TestCoordinatorRetainsUnknownAttemptAfterWriteTimeout(t *testing.T) {
	a, _ := testAdapter(t)
	stateDir := t.TempDir()
	st, err := store.Open(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	catalog := coordinator.Catalog{Resource: core.ResourceRef{MachineID: "local", RuntimeID: "codex-local", Harness: "codex", Model: "fixture"}, MaxConcurrent: 1}
	startResult := make(chan error, 1)
	s, err := coordinator.New(st, shortStartDeadline{Adapter: a, result: startResult}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	if err := s.PutProject(ctx, core.Project{ID: "project", Root: root, DefaultMachineID: "local"}); err != nil {
		t.Fatal(err)
	}
	if err := s.PutAssignment(ctx, core.Assignment{ID: "assignment", ProjectID: "project", ManagingSupervisorID: "supervisor", Objective: "offline write timeout"}); err != nil {
		t.Fatal(err)
	}
	_, err = s.SubmitTask(ctx, coordinator.TaskRequest{Task: core.Task{ID: "stalled", AssignmentID: "assignment", ProfileID: "general-worker", Brief: core.TaskBrief{Objective: strings.Repeat("x", 1<<20)}}, Selection: core.Selection{Resource: catalog.Resource}, WorkingDir: root, Instructions: "stall after prepare"})
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{})
	go func() { s.Run(ctx); close(stopped) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-stopped:
		case <-time.After(3 * time.Second):
			t.Error("coordinator shutdown blocked")
		}
	})
	if err := awaitRPCError(t, startResult); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, worker.ErrRejected) {
		t.Fatal("start write timeout did not remain ambiguous:", err)
	}
	waitForBurst(t, func() bool {
		var current coordinator.Record
		if err := st.Get("task", "stalled", &current); err != nil {
			t.Fatal(err)
		}
		return current.Observation.Worker.State == core.StateUnknown && current.Phase == "ambiguous"
	})
	current, err := s.Get("stalled")
	if err != nil || len(current.Observation.Handle) == 0 || current.Observation.Result != nil {
		t.Fatal("write timeout lost the checkpoint or invented completion", err)
	}
	events, err := s.Events("stalled")
	if err != nil {
		t.Fatal(err)
	}
	starts, failedWrite := 0, false
	for _, event := range events {
		if event.Kind == "worker.start_intent" {
			starts++
		}
		failedWrite = failedWrite || (event.Kind == "worker.start_observed" && strings.Contains(event.Detail, "write failed"))
	}
	if starts != 1 || !failedWrite {
		t.Fatal("write failure evidence was lost or the start was replayed")
	}
	resources, err := s.Resources(ctx)
	if err != nil || *resources[0].Capacity.InUse != 1 {
		t.Fatal("ambiguous attempt released capacity", err)
	}
	if _, err := s.ResolveTask(ctx, "stalled", coordinator.ResolveRequest{AttemptID: current.Attempt.ID, Action: "retry", Reason: "must remain unknown"}); !errors.Is(err, coordinator.ErrResolutionBlocked) {
		t.Fatal("partial write permitted replay", err)
	}
	cancel()
	<-stopped
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = store.Open(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	var recovered coordinator.Record
	if err := st.Get("task", "stalled", &recovered); err != nil || recovered.Attempt.ID != current.Attempt.ID || recovered.Observation.Worker.State != core.StateUnknown {
		t.Fatal("reopen lost ambiguous execution state", err)
	}
}
