// Package store owns transactional local records and their event journal.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"agentworkyard/internal/core"
	// Register the SQLite driver with database/sql.
	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB // Committed records and their event journal.
	lock *sql.DB // Held open to enforce one owning coordinator per state directory.
}
type Tx struct{ tx *sql.Tx }
type Event struct {
	Sequence int64
	core.Event
}

// Open uses a named error return so deferred cleanup can detect failed setup.
func Open(stateDir string) (_ *Store, err error) {
	if err = os.MkdirAll(stateDir, 0700); err != nil {
		return nil, err
	}
	s := &Store{}
	defer func() {
		if err != nil {
			s.Close()
		}
	}()
	// A separate SQLite transaction is a process-lifetime, crash-released lock.
	// The state database remains free to commit durable transactions.
	s.lock, err = sql.Open("sqlite", filepath.Join(stateDir, "owner.sqlite"))
	if err != nil {
		return nil, err
	}
	s.lock.SetMaxOpenConns(1)
	if _, err = s.lock.Exec("BEGIN EXCLUSIVE"); err != nil {
		return nil, fmt.Errorf("coordinator state already in use: %w", err)
	}
	s.db, err = sql.Open("sqlite", filepath.Join(stateDir, "state.sqlite"))
	if err != nil {
		return nil, err
	}
	s.db.SetMaxOpenConns(1)
	_, err = s.db.Exec(`
		PRAGMA journal_mode=WAL;
		PRAGMA synchronous=FULL;
		CREATE TABLE IF NOT EXISTS records (
			kind TEXT NOT NULL,
			id TEXT NOT NULL,
			data BLOB NOT NULL,
			PRIMARY KEY(kind, id)
		);
		CREATE TABLE IF NOT EXISTS events (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			data BLOB NOT NULL
		);
	`)
	if err != nil {
		return nil, err
	}
	// POSIX modes supplement the containing private directory. Windows inherits ACLs.
	for _, name := range []string{"owner.sqlite", "state.sqlite"} {
		if err = os.Chmod(filepath.Join(stateDir, name), 0600); err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Close() error {
	var err error
	if s.db != nil {
		err = s.db.Close()
	}
	if s.lock != nil {
		s.lock.Exec("ROLLBACK")
		closeErr := s.lock.Close()
		if err == nil {
			err = closeErr
		}
	}
	return err
}

// Update runs the supplied function inside one database transaction.
// Returning an error rolls back every write made by that function.
func (s *Store) Update(update func(*Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	// After Commit succeeds, this deferred rollback has nothing left to undo.
	defer tx.Rollback()
	if err = update(&Tx{tx}); err != nil {
		return err
	}
	return tx.Commit()
}

func (t *Tx) Put(kind string, id core.ID, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec("INSERT INTO records VALUES(?,?,?) ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data", kind, string(id), data)
	return err
}

func (t *Tx) Get(kind string, id core.ID, destination any) error {
	return get(t.tx, kind, id, destination)
}

// Both *sql.DB and *sql.Tx satisfy this interface, so Get works inside or
// outside a transaction without duplicating the query.
type querier interface {
	QueryRow(string, ...any) *sql.Row
}

func get(query querier, kind string, id core.ID, destination any) error {
	var data []byte
	if err := query.QueryRow("SELECT data FROM records WHERE kind=? AND id=?", kind, string(id)).Scan(&data); err != nil {
		return err
	}
	return json.Unmarshal(data, destination)
}

func (s *Store) Get(kind string, id core.ID, destination any) error {
	return get(s.db, kind, id, destination)
}

func (s *Store) List(kind string) ([]json.RawMessage, error) {
	rows, err := s.db.Query("SELECT data FROM records WHERE kind=? ORDER BY id", kind)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := []json.RawMessage{}
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		records = append(records, json.RawMessage(data))
	}
	return records, rows.Err()
}

func (t *Tx) Event(workerID core.ID, kind, detail string) error {
	event := core.Event{WorkerID: workerID, At: time.Now().UTC(), Kind: kind, Detail: detail}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = t.tx.Exec("INSERT INTO events(data) VALUES(?)", data)
	return err
}

func (s *Store) Events(workerID core.ID) ([]Event, error) {
	rows, err := s.db.Query("SELECT seq,data FROM events ORDER BY seq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []Event{}
	for rows.Next() {
		var event Event
		var data []byte
		if err := rows.Scan(&event.Sequence, &data); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(data, &event.Event); err != nil {
			return nil, err
		}
		if workerID == "" || event.WorkerID == workerID {
			events = append(events, event)
		}
	}
	return events, rows.Err()
}
