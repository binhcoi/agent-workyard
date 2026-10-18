package store

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"agentworkyard/internal/core"
)

func TestAtomicStateEventsLockAndReopen(t *testing.T) {
	dir := t.TempDir()
	stateStore, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(dir); err == nil {
		other.Close()
		t.Fatal("two coordinators acquired the same state")
	}
	abort := errors.New("simulate persistence failure")
	err = stateStore.Update(func(tx *Tx) error {
		if err := tx.Put("task", "one", core.Task{ID: "one", State: core.StateRunning}); err != nil {
			return err
		}
		if err := tx.Event("worker", "started", ""); err != nil {
			return err
		}
		return abort
	})
	if !errors.Is(err, abort) {
		t.Fatal(err)
	}
	var task core.Task
	if err = stateStore.Get("task", "one", &task); err == nil {
		t.Fatal("rolled-back state survived")
	}
	events, err := stateStore.Events("")
	if err != nil || len(events) != 0 {
		t.Fatalf("rolled-back event survived: %v %v", events, err)
	}
	if err = stateStore.Update(func(tx *Tx) error {
		if err := tx.Put("task", "one", core.Task{ID: "one", State: core.StateCompleted}); err != nil {
			return err
		}
		return tx.Event("worker", "completed", "evidence")
	}); err != nil {
		t.Fatal(err)
	}
	if err = stateStore.Close(); err != nil {
		t.Fatal(err)
	}
	stateStore, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer stateStore.Close()
	if err = stateStore.Get("task", "one", &task); err != nil || task.State != core.StateCompleted {
		t.Fatalf("state: %+v %v", task, err)
	}
	events, err = stateStore.Events("worker")
	if err != nil || len(events) != 1 || events[0].Kind != "completed" || events[0].Sequence != 1 {
		t.Fatalf("events: %+v %v", events, err)
	}
}

// The child exits without running Close or transaction defers, like a crash.
func TestCrashPeer(t *testing.T) {
	dir := os.Getenv("WORKYARD_STORE_CRASH_DIR")
	if dir == "" {
		return
	}
	stateStore, err := Open(dir)
	if err != nil {
		os.Exit(71)
	}
	if err = stateStore.Update(func(tx *Tx) error {
		if err := tx.Put("task", "committed", core.Task{ID: "committed", State: core.StateCompleted}); err != nil {
			return err
		}
		return tx.Event("worker", "completed", "durable")
	}); err != nil {
		os.Exit(72)
	}
	stateStore.Update(func(tx *Tx) error {
		if err := tx.Put("task", "uncommitted", core.Task{ID: "uncommitted"}); err != nil {
			return err
		}
		if err := tx.Event("worker", "uncommitted", ""); err != nil {
			return err
		}
		os.Exit(73)
		return nil
	})
	os.Exit(74)
}

func TestProcessCrashReleasesOwnershipAndRollsBack(t *testing.T) {
	dir := t.TempDir()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary, "-test.run=^TestCrashPeer$")
	cmd.Env = append(os.Environ(), "WORKYARD_STORE_CRASH_DIR="+dir)
	var exit *exec.ExitError
	if err = cmd.Run(); !errors.As(err, &exit) || exit.ExitCode() != 73 {
		t.Fatalf("crash peer: %v", err)
	}
	stateStore, err := Open(dir)
	if err != nil {
		t.Fatalf("crash left ownership locked: %v", err)
	}
	defer stateStore.Close()
	var task core.Task
	if err = stateStore.Get("task", "committed", &task); err != nil || task.State != core.StateCompleted {
		t.Fatalf("lost committed state: %+v %v", task, err)
	}
	if err = stateStore.Get("task", "uncommitted", &task); err == nil {
		t.Fatal("uncommitted state survived crash")
	}
	events, err := stateStore.Events("worker")
	if err != nil || len(events) != 1 || events[0].Kind != "completed" {
		t.Fatalf("crash journal: %+v %v", events, err)
	}
}
