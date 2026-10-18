package codex

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sync"
	"sync/atomic"

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
	stdin io.WriteCloser
	// writeMu keeps concurrent JSON writes from interleaving on stdin.
	writeMu sync.Mutex
	// mu protects pending response channels and the connection error.
	mu            sync.Mutex
	pending       map[string]chan envelope
	next          atomic.Uint64
	done          chan struct{}
	err           error
	notifications chan envelope
}

func newRPC(binary string, stderr io.Writer) (*rpc, error) {
	// Native spawning is disabled at the runtime as well as in thread config.
	cmd := exec.Command(binary, "app-server", "--stdio", "-c", "features.multi_agent=false")
	cmd.Stderr = stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err = cmd.Start(); err != nil {
		return nil, err
	}
	client := &rpc{
		cmd:           cmd,
		stdin:         stdin,
		pending:       map[string]chan envelope{},
		done:          make(chan struct{}),
		notifications: make(chan envelope, 128),
	}
	go client.read(stdout)
	return client, nil
}

func (r *rpc) write(message any) error {
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return json.NewEncoder(r.stdin).Encode(message)
}

func (r *rpc) call(ctx context.Context, method string, params, result any) error {
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
	if err := r.write(request); err != nil {
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
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.err
	}
}

func (r *rpc) read(stdout io.Reader) {
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
			if err := r.write(rejection); err != nil {
				r.finish(err)
				return
			}
		}
		// Ignore streaming deltas; completed items contain the durable content.
		if len(message.ID) == 0 && !isWorkerNotification(message.Method) {
			continue
		}
		select {
		case r.notifications <- message:
		default:
			r.finish(errors.New("Codex notification buffer exhausted; reconcile native state"))
			return
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
	defer r.mu.Unlock()
	select {
	case <-r.done:
		return
	default:
		r.err = err
		close(r.done)
	}
}

func (r *rpc) close() error {
	r.finish(errors.New("Codex App Server closed"))
	r.stdin.Close()
	// Kill only this owned runtime. Workers never belong to the supervisor process.
	_ = r.cmd.Process.Kill()
	return r.cmd.Wait()
}

func isWorkerNotification(method string) bool {
	switch method {
	case "turn/started", "turn/completed", "item/completed", "thread/tokenUsage/updated":
		return true
	default:
		return false
	}
}
