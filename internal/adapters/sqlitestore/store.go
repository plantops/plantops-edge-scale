package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

const schemaVersion = 2

type Store struct {
	path string
	db   *sql.DB
}

type Status struct {
	Path            string `json:"path"`
	Schema          int    `json:"schema_version"`
	Integrity       string `json:"integrity"`
	Tickets         int64  `json:"tickets"`
	Overrides       int64  `json:"overrides"`
	PendingSync     int64  `json:"pending_sync"`
	QueuedCycles    int64  `json:"queued_cycles"`
	CalledCycles    int64  `json:"called_cycles"`
	CompletedCycles int64  `json:"completed_cycles"`
	OrphanCycles    int64  `json:"orphan_cycles"`
}

type SyncItem struct {
	ID           int64      `json:"id"`
	Kind         string     `json:"kind"`
	EntityID     string     `json:"entity_id"`
	PayloadJSON  string     `json:"payload_json"`
	CreatedAt    time.Time  `json:"created_at"`
	AttemptCount int        `json:"attempt_count"`
	LastAttempt  *time.Time `json:"last_attempt_at,omitempty"`
	LastError    string     `json:"last_error,omitempty"`
	NextAttempt  time.Time  `json:"next_attempt_at"`
}

func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("sqlite path is empty")
	}
	dir := filepath.Dir(path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// One writer/connection is enough for this edge workload and guarantees all
	// connection-scoped PRAGMAs apply consistently without CGO/native services.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	s := &Store{path: path, db: db}
	if err := s.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) init(ctx context.Context) error {
	for _, stmt := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=FULL",
		"PRAGMA foreign_keys=ON",
		"PRAGMA busy_timeout=5000",
		"PRAGMA wal_autocheckpoint=1000",
	} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite %s: %w", stmt, err)
		}
	}
	if err := s.migrate(ctx); err != nil {
		return err
	}
	return s.IntegrityCheck(ctx)
}

func (s *Store) migrate(ctx context.Context) error {
	var v int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&v); err != nil {
		return err
	}
	if v > schemaVersion {
		return fmt.Errorf("sqlite schema %d is newer than supported %d", v, schemaVersion)
	}
	if v == schemaVersion {
		return nil
	}
	if v == 1 {
		return s.migrateV2(ctx)
	}
	if v != 0 {
		return fmt.Errorf("unsupported sqlite migration from schema %d", v)
	}

	// Build the original v1 schema first, then apply the v1 -> v2 migration.
	// This keeps one tested migration path for both new and existing databases.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmts := []string{
		`CREATE TABLE tickets (
			id TEXT PRIMARY KEY,
			station_id TEXT NOT NULL,
			transaction_id TEXT NOT NULL UNIQUE,
			plate TEXT NOT NULL DEFAULT '',
			rfid TEXT NOT NULL DEFAULT '',
			weight_kg INTEGER NOT NULL,
			weight_observed_at TEXT NOT NULL,
			weight_raw_seq INTEGER NOT NULL,
			weight_raw_hash TEXT NOT NULL,
			mode TEXT NOT NULL,
			overrides_json TEXT NOT NULL DEFAULT '[]',
			ticket_json TEXT NOT NULL,
			committed_at TEXT NOT NULL,
			synced_at TEXT
		)`,
		`CREATE INDEX idx_tickets_committed_at ON tickets(committed_at)`,
		`CREATE TABLE overrides (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			ticket_id TEXT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
			transaction_id TEXT NOT NULL,
			device TEXT NOT NULL,
			reason TEXT NOT NULL,
			requested_by TEXT NOT NULL,
			authorized_by TEXT NOT NULL,
			authorized_as TEXT NOT NULL,
			authorized_at TEXT NOT NULL,
			evidence_json TEXT NOT NULL,
			expired_at TEXT
		)`,
		`CREATE INDEX idx_overrides_transaction ON overrides(transaction_id)`,
		`CREATE TABLE sync_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			entity_id TEXT NOT NULL,
			payload_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			attempt_count INTEGER NOT NULL DEFAULT 0,
			last_attempt_at TEXT,
			last_error TEXT NOT NULL DEFAULT '',
			next_attempt_at TEXT NOT NULL,
			acked_at TEXT,
			UNIQUE(kind, entity_id)
		)`,
		`CREATE INDEX idx_sync_pending ON sync_queue(acked_at, next_attempt_at, id)`,
		`CREATE TABLE station_state (
			key TEXT PRIMARY KEY,
			value_json TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version=1`); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	return s.migrateV2(ctx)
}

// Commit is the legacy one-pass durability boundary retained only while the
// old physical-pass engine is being replaced. The authoritative two-pass
// business path uses OpenCycle + CompleteCycle; first pass must not call this.
func (s *Store) Commit(ctx context.Context, ticket domain.Ticket) error {
	payload, err := json.Marshal(ticket)
	if err != nil {
		return err
	}
	overridesJSON, err := json.Marshal(ticket.Overrides)
	if err != nil {
		return err
	}
	now := ticket.CommittedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO tickets
		(id,station_id,transaction_id,plate,rfid,weight_kg,weight_observed_at,weight_raw_seq,weight_raw_hash,mode,overrides_json,ticket_json,committed_at,synced_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		ticket.ID, ticket.StationID, ticket.TransactionID, ticket.Plate, ticket.RFID, ticket.WeightKG,
		ticket.WeightObservedAt.UTC().Format(time.RFC3339Nano), ticket.WeightRawRef.Seq, ticket.WeightRawRef.Hash, string(ticket.Mode),
		string(overridesJSON), string(payload), now.Format(time.RFC3339Nano), nullableTime(ticket.SyncedAt))
	if err != nil {
		return err
	}
	for _, o := range ticket.Overrides {
		evidence, err := json.Marshal(o.Evidence)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO overrides
			(ticket_id,transaction_id,device,reason,requested_by,authorized_by,authorized_as,authorized_at,evidence_json,expired_at)
			VALUES(?,?,?,?,?,?,?,?,?,?)`, ticket.ID, ticket.TransactionID, string(o.Device), o.Reason, o.RequestedBy, o.AuthorizedBy, string(o.AuthorizedAs), o.AuthorizedAt.UTC().Format(time.RFC3339Nano), string(evidence), nullableTime(o.ExpiredAt))
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sync_queue(kind,entity_id,payload_json,created_at,next_attempt_at) VALUES('TICKET',?,?,?,?)`,
		ticket.ID, string(payload), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	stateJSON, _ := json.Marshal(map[string]any{"ticket_id": ticket.ID, "transaction_id": ticket.TransactionID, "committed_at": now})
	_, err = tx.ExecContext(ctx, `INSERT INTO station_state(key,value_json,updated_at) VALUES('last_committed_ticket',?,?)
		ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at`, string(stateJSON), now.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) IntegrityCheck(ctx context.Context) error {
	var result string
	if err := s.db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("sqlite integrity_check: %s", result)
	}
	return nil
}

func (s *Store) Status(ctx context.Context) (Status, error) {
	st := Status{Path: s.path, Schema: schemaVersion, Integrity: "ok"}
	if err := s.IntegrityCheck(ctx); err != nil {
		st.Integrity = err.Error()
		return st, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM tickets").Scan(&st.Tickets); err != nil {
		return st, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM overrides").Scan(&st.Overrides); err != nil {
		return st, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sync_queue WHERE acked_at IS NULL").Scan(&st.PendingSync); err != nil {
		return st, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM weigh_cycles WHERE status=?", string(domain.CycleQueued)).Scan(&st.QueuedCycles); err != nil {
		return st, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM weigh_cycles WHERE status=?", string(domain.CycleCalled)).Scan(&st.CalledCycles); err != nil {
		return st, err
	}
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM weigh_cycles WHERE status=?", string(domain.CycleComplete)).Scan(&st.CompletedCycles); err != nil {
		return st, err
	}
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM weigh_cycles WHERE status IN (?,?,?,?,?)`,
		string(domain.CycleOrphanedFirstPass), string(domain.CycleUnpairedReturn), string(domain.CyclePairTimeInvalid), string(domain.CyclePairWeightInvalid), string(domain.CycleWrongTruck)).Scan(&st.OrphanCycles); err != nil {
		return st, err
	}
	return st, nil
}

func (s *Store) LastTicket(ctx context.Context) (domain.Ticket, bool, error) {
	var raw string
	err := s.db.QueryRowContext(ctx, "SELECT ticket_json FROM tickets ORDER BY committed_at DESC, rowid DESC LIMIT 1").Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Ticket{}, false, nil
	}
	if err != nil {
		return domain.Ticket{}, false, err
	}
	var t domain.Ticket
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return domain.Ticket{}, false, err
	}
	return t, true, nil
}

// RecentTickets returns up to limit completed tickets, newest first.
func (s *Store) RecentTickets(ctx context.Context, limit int) ([]domain.Ticket, error) {
	if limit <= 0 || limit > 200 {
		limit = 20
	}
	rows, err := s.db.QueryContext(ctx, "SELECT ticket_json FROM tickets ORDER BY committed_at DESC, rowid DESC LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Ticket
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var t domain.Ticket
		if err := json.Unmarshal([]byte(raw), &t); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) PendingSync(ctx context.Context, limit int) ([]SyncItem, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `SELECT id,kind,entity_id,payload_json,created_at,attempt_count,last_attempt_at,last_error,next_attempt_at
		FROM sync_queue WHERE acked_at IS NULL ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SyncItem{}
	for rows.Next() {
		var x SyncItem
		var created, next string
		var last sql.NullString
		if err := rows.Scan(&x.ID, &x.Kind, &x.EntityID, &x.PayloadJSON, &created, &x.AttemptCount, &last, &x.LastError, &next); err != nil {
			return nil, err
		}
		x.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		x.NextAttempt, _ = time.Parse(time.RFC3339Nano, next)
		if last.Valid {
			if v, e := time.Parse(time.RFC3339Nano, last.String); e == nil {
				x.LastAttempt = &v
			}
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (s *Store) MarkSyncAttempt(ctx context.Context, id int64, errText string, next time.Time) error {
	if next.IsZero() {
		next = time.Now().UTC()
	}
	_, err := s.db.ExecContext(ctx, `UPDATE sync_queue SET attempt_count=attempt_count+1,last_attempt_at=?,last_error=?,next_attempt_at=? WHERE id=? AND acked_at IS NULL`, time.Now().UTC().Format(time.RFC3339Nano), errText, next.UTC().Format(time.RFC3339Nano), id)
	return err
}

func (s *Store) AckSync(ctx context.Context, id int64) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var entity string
	if err := tx.QueryRowContext(ctx, "SELECT entity_id FROM sync_queue WHERE id=? AND kind='TICKET'", id).Scan(&entity); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE sync_queue SET acked_at=?,last_error='' WHERE id=?", now, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE tickets SET synced_at=? WHERE id=?", now, entity); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Checkpoint(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return err
}
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
func (s *Store) Path() string { return s.path }

func nullableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}
