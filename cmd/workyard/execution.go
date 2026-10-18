package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"time"

	"agentworkyard/internal/adapters/codex"
	"agentworkyard/internal/config"
	"agentworkyard/internal/coordinator"
	"agentworkyard/internal/core"
	"agentworkyard/internal/store"
)

// endpoint is the private connection file used by short-lived CLI clients.
type endpoint struct{ URL, Token string }

// execution dispatches CLI commands. Each command parses its own flags below.
func execution(args []string, out, stderr io.Writer) error {
	if len(args) < 2 {
		return errors.New(usage)
	}
	switch args[0] + " " + args[1] {
	case "coordinator serve":
		return serveCommand(args[2:], out, stderr)
	case "assignment put":
		return putAssignmentCommand(args[2:], out, stderr)
	case "task submit":
		return submitTaskCommand(args[2:], out, stderr)
	case "task status", "task result", "task conversation", "task events", "task cancel":
		return taskCommand(args[1], args[2:], out, stderr)
	case "task list", "assignment list", "coordinator resources", "coordinator runtime":
		return listCommand(args[0], args[1], args[2:], out, stderr)
	default:
		return errors.New(usage)
	}
}

func serveCommand(args []string, out, stderr io.Writer) error {
	flags := flag.NewFlagSet("coordinator serve", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDir := flags.String("state", "", "private coordinator state directory (required)")
	model := flags.String("model", "", "Codex model (required)")
	codexBinary := flags.String("codex", "codex", "Codex executable")
	maxConcurrent := flags.Int("capacity", 1, "maximum active workers in this runtime")
	provider := flags.String("provider", "", "Codex provider ID, empty inherits Codex configuration")
	accountID := flags.String("account", "local", "opaque local account label")
	machineID := flags.String("machine", "local", "local machine ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" || *model == "" || *maxConcurrent < 1 || flags.NArg() != 0 {
		return errors.New("serve requires --state, --model and positive --capacity")
	}
	catalog := coordinator.Catalog{
		Resource: core.ResourceRef{
			MachineID: core.ID(*machineID),
			RuntimeID: "codex-local",
			Harness:   "codex",
			Provider:  *provider,
			AccountID: core.ID(*accountID),
			Model:     *model,
		},
		MaxConcurrent: *maxConcurrent,
	}
	return serve(*stateDir, *codexBinary, catalog, out, stderr)
}

func putAssignmentCommand(args []string, out, stderr io.Writer) error {
	flags := flag.NewFlagSet("assignment put", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDir := flags.String("state", "", "private coordinator state directory (required)")
	id := flags.String("id", "", "stable assignment ID")
	projectDir := flags.String("project", "", "project root")
	supervisor := flags.String("supervisor", "", "managing supervisor ID")
	objective := flags.String("objective", "", "shared objective")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" || *id == "" || *projectDir == "" || *supervisor == "" || *objective == "" || flags.NArg() != 0 {
		return errors.New("assignment put requires --state, --id, --project, --supervisor, --objective")
	}
	root, err := filepath.Abs(*projectDir)
	if err != nil {
		return err
	}
	resources, err := loadCatalog(*stateDir)
	if err != nil {
		return err
	}
	project := core.Project{
		ID:               core.ID(root),
		Root:             root,
		DefaultMachineID: resources[0].Resource.MachineID,
	}
	if _, err = coordinatorRequest(*stateDir, "POST", "/project", project); err != nil {
		return err
	}
	assignment := core.Assignment{
		ID:                   core.ID(*id),
		ProjectID:            project.ID,
		ManagingSupervisorID: core.ID(*supervisor),
		Objective:            *objective,
		State:                core.AssignmentOpen,
	}
	responseData, err := coordinatorRequest(*stateDir, "POST", "/assignment", assignment)
	return writeResponse(out, responseData, err)
}

func submitTaskCommand(args []string, out, stderr io.Writer) error {
	flags := flag.NewFlagSet("task submit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDir := flags.String("state", "", "private coordinator state directory (required)")
	id := flags.String("id", "", "stable task ID, used for idempotency")
	assignment := flags.String("assignment", "", "assignment ID")
	projectDir := flags.String("project", "", "project root")
	briefPath := flags.String("brief", "", "JSON TaskBrief file")
	bundle := flags.String("bundle", "", "shared instruction bundle")
	profile := flags.String("profile", "general-worker", "worker profile")
	effort := flags.String("effort", "", "reasoning effort; empty inherits harness configuration")
	var skills skillFlags
	flags.Var(&skills, "skill", "selected skill (repeatable)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" || *id == "" || *assignment == "" || *projectDir == "" || *briefPath == "" || *bundle == "" || flags.NArg() != 0 {
		return errors.New("task submit requires --state, --id, --assignment, --project, --brief, --bundle")
	}
	root, err := filepath.Abs(*projectDir)
	if err != nil {
		return err
	}
	loaded, err := config.Load(*bundle, root)
	if err != nil {
		return err
	}
	if loaded.Manifest.Profiles[*profile].Kind != "worker" {
		return errors.New("task profile must be a worker")
	}
	instructions, err := loaded.Render(*profile, skills)
	if err != nil {
		return err
	}
	taskBrief, err := loadTaskBrief(*briefPath)
	if err != nil {
		return err
	}
	resources, err := loadCatalog(*stateDir)
	if err != nil {
		return err
	}
	taskRequest := coordinator.TaskRequest{
		Task: core.Task{
			ID:           core.ID(*id),
			AssignmentID: core.ID(*assignment),
			ProfileID:    *profile,
			Brief:        taskBrief,
		},
		Selection: core.Selection{
			Resource:        resources[0].Resource,
			ReasoningEffort: *effort,
			Rationale:       "Explicit supervisor submission to the configured local resource",
		},
		WorkingDir:   root,
		Instructions: instructions,
	}
	responseData, err := coordinatorRequest(*stateDir, "POST", "/task", taskRequest)
	return writeResponse(out, responseData, err)
}

// loadTaskBrief accepts exactly one JSON object and rejects misspelled fields.
func loadTaskBrief(path string) (core.TaskBrief, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return core.TaskBrief{}, err
	}

	var brief core.TaskBrief
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&brief); err != nil {
		return core.TaskBrief{}, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return core.TaskBrief{}, errors.New("brief must contain one JSON object")
	}
	return brief, nil
}

func taskCommand(operation string, args []string, out, stderr io.Writer) error {
	flags := flag.NewFlagSet("task "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDir := flags.String("state", "", "private coordinator state directory (required)")
	id := flags.String("id", "", "task ID")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" || *id == "" || flags.NArg() != 0 {
		return errors.New("task operation requires --state and --id")
	}
	path := "/task/" + url.PathEscape(*id)
	if operation != "status" {
		path += "/" + operation
	}
	method := "GET"
	if operation == "cancel" {
		method = "POST"
	}
	responseData, err := coordinatorRequest(*stateDir, method, path, nil)
	return writeResponse(out, responseData, err)
}

func listCommand(group, operation string, args []string, out, stderr io.Writer) error {
	flags := flag.NewFlagSet(group+" "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	stateDir := flags.String("state", "", "private coordinator state directory (required)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *stateDir == "" || flags.NArg() != 0 {
		return errors.New("--state is required")
	}
	var path string
	switch group {
	case "task":
		path = "/tasks"
	case "assignment":
		path = "/assignments"
	case "coordinator":
		path = "/" + operation
	}
	responseData, err := coordinatorRequest(*stateDir, "GET", path, nil)
	return writeResponse(out, responseData, err)
}

func writeResponse(out io.Writer, responseData []byte, err error) error {
	if err != nil {
		return err
	}
	_, err = out.Write(responseData)
	return err
}

func loadCatalog(stateDir string) ([]core.ModelOption, error) {
	responseData, err := coordinatorRequest(stateDir, "GET", "/resources", nil)
	if err != nil {
		return nil, err
	}
	var resources []core.ModelOption
	if err = json.Unmarshal(responseData, &resources); err != nil {
		return nil, err
	}
	if len(resources) != 1 {
		return nil, errors.New("this slice requires one configured local resource")
	}
	return resources, nil
}

func coordinatorRequest(stateDir, method, path string, payload any) ([]byte, error) {
	data, err := os.ReadFile(filepath.Join(stateDir, "endpoint.json"))
	if err != nil {
		return nil, err
	}
	var connection endpoint
	if err = json.Unmarshal(data, &connection); err != nil {
		return nil, err
	}
	endpointURL, err := url.Parse(connection.URL)
	if err != nil {
		return nil, err
	}
	if endpointURL.Scheme != "http" || endpointURL.Hostname() != "127.0.0.1" || endpointURL.User != nil || connection.Token == "" {
		return nil, errors.New("invalid local coordinator endpoint")
	}
	var body io.Reader
	if payload != nil {
		data, err = json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, connection.URL+path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+connection.Token)
	request.Header.Set("Content-Type", "application/json")
	client := &http.Client{
		Timeout:   40 * time.Second,
		Transport: &http.Transport{Proxy: nil},
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("coordinator redirect refused")
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("local coordinator unavailable: %w", err)
	}
	defer response.Body.Close()
	defer client.CloseIdleConnections()
	data, err = io.ReadAll(io.LimitReader(response.Body, 32<<20))
	if err != nil {
		return nil, err
	}
	if response.StatusCode >= 300 {
		return nil, fmt.Errorf("coordinator %s: %s", response.Status, strings.TrimSpace(string(data)))
	}
	return data, nil
}

// serve owns the store, Codex runtime and local HTTP server until shutdown.
func serve(stateDir, codexBinary string, catalog coordinator.Catalog, out, stderr io.Writer) error {
	stateStore, err := store.Open(stateDir)
	if err != nil {
		return err
	}
	defer stateStore.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	adapter, err := codex.New(initCtx, codexBinary, stderr)
	cancel()
	if err != nil {
		return err
	}
	defer adapter.Close()
	service, err := coordinator.New(stateStore, adapter, catalog)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer listener.Close()
	var secret [32]byte
	if _, err = rand.Read(secret[:]); err != nil {
		return err
	}
	connection := endpoint{URL: "http://" + listener.Addr().String(), Token: hex.EncodeToString(secret[:])}
	if err := writeEndpointFile(stateDir, connection); err != nil {
		return err
	}
	server := &http.Server{
		Handler:           service.Handler(connection.Token),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	serviceStopped := make(chan struct{})
	// The execution loop belongs to this process, not to an HTTP request.
	go func() {
		defer close(serviceStopped)
		service.Run(runCtx)
	}()
	defer func() {
		runCancel()
		adapter.Close()
		// Wait for the execution loop before its deferred store.Close runs.
		<-serviceStopped
	}()
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.Serve(listener) }()
	defer server.Close()
	if _, err = fmt.Fprintf(out, "Coordinator %s ready. Local state: %s\n", service.ID, stateDir); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return nil
	case err = <-service.Errors():
		return err
	case err = <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// writeEndpointFile writes the complete temporary file before publishing the
// connection details for CLI clients.
func writeEndpointFile(stateDir string, connection endpoint) error {
	data, err := json.Marshal(connection)
	if err != nil {
		return err
	}
	tempFile, err := os.CreateTemp(stateDir, "endpoint-*.json")
	if err != nil {
		return err
	}
	tempPath := tempFile.Name()
	defer os.Remove(tempPath)
	if _, err = tempFile.Write(data); err != nil {
		tempFile.Close()
		return err
	}
	if err = tempFile.Close(); err != nil {
		return err
	}
	if err = os.Rename(tempPath, filepath.Join(stateDir, "endpoint.json")); err != nil {
		return err
	}
	return nil
}
