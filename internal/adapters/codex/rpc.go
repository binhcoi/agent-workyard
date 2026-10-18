package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"

	"agentworkyard/internal/worker"
)

// envelope is one JSON-RPC message. Responses carry an ID; notifications carry
// a method without an ID. RawMessage postpones decoding method-specific data.
type envelope struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return fmt.Sprintf("Codex RPC %d: %s", e.Code, e.Message) }

// rpc sends requests over stdin and matches stdout responses to waiting callers.
type rpc struct {
	cmd   *exec.Cmd
	stdin *os.File
	// writeGate serializes complete frames; waiting callers can cancel.
	writeGate chan struct{}
	// mu protects pending response channels and the connection error.
	mu            sync.Mutex
	pending       map[string]chan envelope
	next          atomic.Uint64
	done          chan struct{}
	stopped       chan struct{}
	err           error
	notifications chan envelope
}

func newRPC(binary string, stderr io.Writer) (*rpc, error) {
	// Native spawning is disabled at the runtime as well as in thread config.
	cmd := exec.Command(binary, "app-server", "--stdio", "-c", "features.multi_agent=false")
	cmd.Stderr = stderr
	// Retain the write end as *os.File so cancellation can interrupt its I/O.
	stdinRead, stdin, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer stdinRead.Close()
	cmd.Stdin = stdinRead
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		stdin.Close()
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		stdin.Close()
		return nil, err
	}
	client := &rpc{
		cmd:           cmd,
		stdin:         stdin,
		writeGate:     make(chan struct{}, 1),
		pending:       map[string]chan envelope{},
		done:          make(chan struct{}),
		stopped:       make(chan struct{}),
		notifications: make(chan envelope, 128),
	}
	go client.read(stdout)
	return client, nil
}

func (r *rpc) write(ctx context.Context, message any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return r.connectionError()
	case r.writeGate <- struct{}{}:
	}
	defer func() { <-r.writeGate }()
	// Cancellation or disconnect may race acquisition of the available writer.
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-r.done:
		return r.connectionError()
	default:
	}
	data, err := json.Marshal(message)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := r.stdin.SetWriteDeadline(deadline); err != nil {
		err = fmt.Errorf("set Codex RPC write deadline: %w", err)
		r.finish(err)
		return err
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		// Also interrupt contexts cancelled before their deadline, or without one.
		r.stdin.SetWriteDeadline(time.Now())
		close(interrupted)
	})
	n, err := r.stdin.Write(data)
	if !stop() {
		<-interrupted
	}
	// Join cancellation before resetting the deadline or admitting another writer.
	resetErr := r.stdin.SetWriteDeadline(time.Time{})
	if err == nil {
		err = resetErr
	}
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		contextErr := ctx.Err()
		// The file deadline can fire just before the context timer publishes Err.
		if contextErr == nil && !deadline.IsZero() && errors.Is(err, os.ErrDeadlineExceeded) {
			contextErr = context.DeadlineExceeded
		}
		err = fmt.Errorf("Codex RPC write failed (%d/%d bytes): %w", n, len(data), errors.Join(contextErr, err))
		// A zero-byte context interruption leaves framing intact. Partial frames
		// and other I/O failures make the transport unusable; never replay them.
		if n != 0 || contextErr == nil || !errors.Is(err, os.ErrDeadlineExceeded) || resetErr != nil {
			r.finish(err)
		}
		return err
	}
	return ctx.Err()
}

func (r *rpc) connectionError() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

func (r *rpc) call(ctx context.Context, method string, params, result any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	requestID := fmt.Sprint(r.next.Add(1))
	// A buffered reply lets the reader finish delivery even if the caller times out.
	responseCh := make(chan envelope, 1)
	r.mu.Lock()
	r.pending[requestID] = responseCh
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		delete(r.pending, requestID)
		r.mu.Unlock()
	}()
	request := map[string]any{
		"id":     json.RawMessage(requestID),
		"method": method,
		"params": params,
	}
	if err := r.write(ctx, request); err != nil {
		return err
	}
	select {
	case response := <-responseCh:
		if response.Error != nil {
			if response.Error.Code == -32601 || response.Error.Code == -32602 {
				return errors.Join(worker.ErrRejected, response.Error)
			}
			return response.Error
		}
		if result != nil {
			return json.Unmarshal(response.Result, result)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-r.done:
		return r.connectionError()
	}
}

func (r *rpc) read(stdout io.Reader) {
	defer close(r.stopped)
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 64*1024), 32*1024*1024)
	for scanner.Scan() {
		var message envelope
		if err := json.Unmarshal(scanner.Bytes(), &message); err != nil {
			r.finish(fmt.Errorf("invalid Codex JSON: %w", err))
			return
		}
		// Responses go to the caller waiting for this request ID.
		if len(message.ID) > 0 && message.Method == "" {
			r.mu.Lock()
			responseCh := r.pending[string(message.ID)]
			r.mu.Unlock()
			if responseCh != nil {
				responseCh <- message
			}
			continue
		}
		if len(message.ID) > 0 {
			// M2 has no approval/elicitation UI. Never auto-approve a server request.
			rejection := map[string]any{
				"id": message.ID,
				"error": map[string]any{
					"code":    -32601,
					"message": "Workyard M2 does not support interactive requests",
				},
			}
			writeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			err := r.write(writeCtx, rejection)
			cancel()
			if err != nil {
				r.finish(err)
				return
			}
		}
		// Ignore streaming deltas; completed items contain the durable content.
		if len(message.ID) == 0 && !isWorkerNotification(message.Method) {
			continue
		}
		// Backpressure pauses stdout reading without dropping notifications or
		// growing an unbounded queue. Shutdown releases a blocked delivery.
		select {
		case r.notifications <- message:
		case <-r.done:
			return
		}
	}
	err := scanner.Err()
	if err == nil {
		err = errors.New("Codex App Server disconnected")
	}
	r.finish(err)
}

func (r *rpc) finish(err error) {
	r.mu.Lock()
	select {
	case <-r.done:
		r.mu.Unlock()
		return
	default:
		r.err = err
		close(r.done)
	}
	r.mu.Unlock()
	// Disconnect must release an active writer as well as callers waiting for it.
	r.stdin.Close()
}

func (r *rpc) close() error {
	r.finish(errors.New("Codex App Server closed"))
	r.stdin.Close()
	// Kill only this owned runtime. Workers never belong to the supervisor process.
	_ = r.cmd.Process.Kill()
	err := r.cmd.Wait()
	<-r.stopped
	return err
}

func isWorkerNotification(method string) bool {
	switch method {
	case "turn/started", "turn/completed", "item/completed", "thread/tokenUsage/updated":
		return true
	default:
		return false
	}
}
