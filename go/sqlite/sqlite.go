// Package sqlite stores thread journals and the thread directory in one SQLite database.
package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/rdeusser/tardigrade/go/tardigrade"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS threads (
	actor TEXT NOT NULL, instance TEXT NOT NULL, thread TEXT NOT NULL,
	parent_actor TEXT, parent_instance TEXT, parent_thread TEXT, depth INTEGER NOT NULL, created_at INTEGER NOT NULL, active INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (actor, instance, thread)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS events (
	actor TEXT NOT NULL, instance TEXT NOT NULL, thread TEXT NOT NULL, seq INTEGER NOT NULL,
	record TEXT NOT NULL, message_id TEXT,
	PRIMARY KEY (actor, instance, thread, seq)) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS events_message ON events (actor, instance, thread, message_id) WHERE message_id IS NOT NULL;
CREATE TABLE IF NOT EXISTS invocations (
	actor TEXT NOT NULL, instance TEXT NOT NULL, thread TEXT NOT NULL,
	atom TEXT NOT NULL, act TEXT NOT NULL, origin INTEGER NOT NULL, ref TEXT NOT NULL,
	PRIMARY KEY (actor, instance, thread, atom, act, origin)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS inputs (
	actor TEXT NOT NULL, instance TEXT NOT NULL, thread TEXT NOT NULL, ref TEXT NOT NULL, input BLOB NOT NULL,
	PRIMARY KEY (actor, instance, thread, ref)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS checkpoints (
	actor TEXT NOT NULL, instance TEXT NOT NULL, thread TEXT NOT NULL, checkpoint TEXT NOT NULL,
	PRIMARY KEY (actor, instance, thread)) WITHOUT ROWID;
`

// Store is a SQLite database holding every thread of a host.
type Store struct {
	db *sql.DB
}

// Open opens or creates the database at path.
func Open(path string) (*Store, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// CreateThread records a thread unless it exists and reports whether it was created.
func (s *Store) CreateThread(info tardigrade.ThreadInfo) (bool, error) {
	var parent [3]any
	if p := info.Parent; p != nil {
		parent = [3]any{p.Actor, p.Instance, p.Thread}
	}
	result, err := s.db.Exec(`INSERT INTO threads (actor, instance, thread, parent_actor, parent_instance, parent_thread, depth, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
		info.Address.Actor, info.Address.Instance, info.Address.Thread, parent[0], parent[1], parent[2], info.Depth, info.CreatedAt)
	if err != nil {
		return false, err
	}
	created, err := result.RowsAffected()
	return created == 1, err
}

const threadColumns = `actor, instance, thread, parent_actor, parent_instance, parent_thread, depth, created_at, active`

type scanner interface{ Scan(...any) error }

func scanThread(row scanner) (tardigrade.ThreadInfo, error) {
	var info tardigrade.ThreadInfo
	var actor, instance, thread sql.NullString
	err := row.Scan(&info.Address.Actor, &info.Address.Instance, &info.Address.Thread, &actor, &instance, &thread, &info.Depth, &info.CreatedAt, &info.Active)
	if thread.Valid {
		info.Parent = &tardigrade.ThreadCoordinate{Actor: actor.String, Instance: instance.String, Thread: thread.String}
	}
	return info, err
}

// Thread returns a directory entry or nil when the thread does not exist.
func (s *Store) Thread(address tardigrade.ThreadCoordinate) (*tardigrade.ThreadInfo, error) {
	info, err := scanThread(s.db.QueryRow(`SELECT `+threadColumns+` FROM threads WHERE actor = ? AND instance = ? AND thread = ?`, address.Actor, address.Instance, address.Thread))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &info, nil
}

// Threads lists threads in creation order. An empty actor lists every actor and an empty instance lists every instance.
func (s *Store) Threads(actor, instance string) ([]tardigrade.ThreadInfo, error) {
	rows, err := s.db.Query(`SELECT `+threadColumns+` FROM threads WHERE (?1 = '' OR actor = ?1) AND (?2 = '' OR instance = ?2) ORDER BY created_at, actor, instance, thread`, actor, instance)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var threads []tardigrade.ThreadInfo
	for rows.Next() {
		info, err := scanThread(rows)
		if err != nil {
			return nil, err
		}
		threads = append(threads, info)
	}
	return threads, rows.Err()
}

// SetActive records whether a thread holds unfinished work, so a restarted host resumes it.
func (s *Store) SetActive(address tardigrade.ThreadCoordinate, active bool) error {
	_, err := s.db.Exec(`UPDATE threads SET active = ? WHERE actor = ? AND instance = ? AND thread = ?`, active, address.Actor, address.Instance, address.Thread)
	return err
}

// Journal returns the journal of one thread.
func (s *Store) Journal(address tardigrade.ThreadCoordinate) tardigrade.Journal {
	return &journal{db: s.db, key: []any{address.Actor, address.Instance, address.Thread}}
}

type journal struct {
	db  *sql.DB
	key []any
}

const where = ` WHERE actor = ? AND instance = ? AND thread = ?`

func (j *journal) args(extra ...any) []any {
	return append(append([]any(nil), j.key...), extra...)
}

func (j *journal) Length() (int64, error) {
	var length int64
	err := j.db.QueryRow(`SELECT COALESCE(MAX(seq) + 1, 0) FROM events`+where, j.args()...).Scan(&length)
	return length, err
}

func (j *journal) Read(from, to int64) ([]tardigrade.Record, error) {
	rows, err := j.db.Query(`SELECT seq, record FROM events`+where+` AND seq >= ? AND seq < ? ORDER BY seq`, j.args(from, to)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	records := make([]tardigrade.Record, 0, max(to-from, 0))
	for rows.Next() {
		var seq int64
		var data []byte
		if err := rows.Scan(&seq, &data); err != nil {
			return nil, err
		}
		if seq != from+int64(len(records)) {
			return nil, fmt.Errorf("journal: position %d is missing", from+int64(len(records)))
		}
		var record tardigrade.Record
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, fmt.Errorf("journal: position %d: %w", seq, err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if int64(len(records)) != to-from {
		return nil, fmt.Errorf("journal: range [%d, %d) is unavailable", from, to)
	}
	return records, nil
}

func origin(o *int64) int64 {
	if o == nil {
		return -1
	}
	return *o
}

func (j *journal) Append(expected int64, records []tardigrade.Record, changes tardigrade.Changes) (err error) {
	tx, err := j.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()
	var length int64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(seq) + 1, 0) FROM events`+where, j.args()...).Scan(&length); err != nil {
		return err
	}
	if length != expected {
		return tardigrade.ErrConflict
	}
	for i, record := range records {
		data, err := json.Marshal(record)
		if err != nil {
			return err
		}
		var message any
		if record.Message != nil {
			message = record.Message.ID
		}
		position := expected + int64(i)
		if _, err := tx.Exec(`INSERT INTO events (actor, instance, thread, seq, record, message_id) VALUES (?, ?, ?, ?, ?, ?)`, j.args(position, string(data), message)...); err != nil {
			return err
		}
		if request, ok := record.Event.(tardigrade.EffectRequested); ok {
			ref, _ := json.Marshal(request.Ref)
			if _, err := tx.Exec(`INSERT INTO invocations (actor, instance, thread, atom, act, origin, ref) VALUES (?, ?, ?, ?, ?, ?, ?)`, j.args(request.Ref.Atom, request.Ref.Act, origin(request.Origin), string(ref))...); err != nil {
				return err
			}
		}
	}
	for ref, input := range changes.PutInputs {
		if _, err := tx.Exec(`INSERT OR REPLACE INTO inputs (actor, instance, thread, ref, input) VALUES (?, ?, ?, ?, ?)`, j.args(ref, []byte(input))...); err != nil {
			return err
		}
	}
	for _, ref := range changes.RemoveInputs {
		if _, err := tx.Exec(`DELETE FROM inputs`+where+` AND ref = ?`, j.args(ref)...); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (j *journal) Message(id string) (*tardigrade.Record, int64, error) {
	var seq int64
	var data []byte
	err := j.db.QueryRow(`SELECT seq, record FROM events`+where+` AND message_id = ?`, j.args(id)...).Scan(&seq, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	var record tardigrade.Record
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, 0, err
	}
	return &record, seq, nil
}

func (j *journal) Invocation(identity tardigrade.Identity) (*tardigrade.Ref, error) {
	var data []byte
	err := j.db.QueryRow(`SELECT ref FROM invocations`+where+` AND atom = ? AND act = ? AND origin = ?`, j.args(identity.Atom, identity.Act, origin(identity.Origin))...).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var ref tardigrade.Ref
	return &ref, json.Unmarshal(data, &ref)
}

func (j *journal) Inputs() (map[string]json.RawMessage, error) {
	rows, err := j.db.Query(`SELECT ref, input FROM inputs`+where, j.args()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	inputs := map[string]json.RawMessage{}
	for rows.Next() {
		var ref string
		var input []byte
		if err := rows.Scan(&ref, &input); err != nil {
			return nil, err
		}
		inputs[ref] = input
	}
	return inputs, rows.Err()
}

func (j *journal) Checkpoint() (*tardigrade.Checkpoint, error) {
	var data []byte
	err := j.db.QueryRow(`SELECT checkpoint FROM checkpoints`+where, j.args()...).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var checkpoint tardigrade.Checkpoint
	return &checkpoint, json.Unmarshal(data, &checkpoint)
}

func (j *journal) SaveCheckpoint(checkpoint tardigrade.Checkpoint) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	_, err = j.db.Exec(`INSERT OR REPLACE INTO checkpoints (actor, instance, thread, checkpoint) VALUES (?, ?, ?, ?)`, j.args(string(data))...)
	return err
}
