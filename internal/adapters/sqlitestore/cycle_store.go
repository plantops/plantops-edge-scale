package sqlitestore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

func (s *Store) migrateV2(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmts := []string{
		`ALTER TABLE tickets ADD COLUMN cycle_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE tickets ADD COLUMN first_pass_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE tickets ADD COLUMN second_pass_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE tickets ADD COLUMN gross_kg INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tickets ADD COLUMN tare_kg INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tickets ADD COLUMN net_kg INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tickets ADD COLUMN first_weight_observed_at TEXT`,
		`ALTER TABLE tickets ADD COLUMN second_weight_observed_at TEXT`,
		`ALTER TABLE tickets ADD COLUMN first_weight_raw_seq INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tickets ADD COLUMN first_weight_raw_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE tickets ADD COLUMN second_weight_raw_seq INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tickets ADD COLUMN second_weight_raw_hash TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE tickets ADD COLUMN pair_elapsed_seconds INTEGER NOT NULL DEFAULT 0`,
		`CREATE UNIQUE INDEX idx_tickets_cycle_id ON tickets(cycle_id) WHERE cycle_id <> ''`,
		`CREATE TABLE weigh_passes (
			id TEXT PRIMARY KEY,
			cycle_id TEXT NOT NULL,
			pass_number INTEGER NOT NULL,
			direction TEXT NOT NULL,
			station_id TEXT NOT NULL,
			plate TEXT NOT NULL,
			plate_key TEXT NOT NULL,
			rfid TEXT NOT NULL DEFAULT '',
			weight_kg INTEGER NOT NULL,
			weight_observed_at TEXT NOT NULL,
			weight_raw_seq INTEGER NOT NULL,
			weight_raw_hash TEXT NOT NULL,
			mode TEXT NOT NULL,
			overrides_json TEXT NOT NULL DEFAULT '[]',
			evidence_json TEXT NOT NULL DEFAULT '[]',
			pass_json TEXT NOT NULL,
			committed_at TEXT NOT NULL,
			UNIQUE(cycle_id, pass_number)
		)`,
		`CREATE INDEX idx_weigh_passes_cycle ON weigh_passes(cycle_id, pass_number)`,
		`CREATE TABLE weigh_cycles (
			id TEXT PRIMARY KEY,
			station_id TEXT NOT NULL,
			plate TEXT NOT NULL,
			plate_key TEXT NOT NULL,
			rfid TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL,
			first_pass_id TEXT NOT NULL UNIQUE,
			second_pass_id TEXT,
			queued_at TEXT NOT NULL,
			called_at TEXT,
			completed_at TEXT,
			pair_elapsed_ns INTEGER NOT NULL DEFAULT 0,
			gross_kg INTEGER NOT NULL DEFAULT 0,
			tare_kg INTEGER NOT NULL DEFAULT 0,
			net_kg INTEGER NOT NULL DEFAULT 0,
			last_block_reason TEXT NOT NULL DEFAULT '',
			cycle_json TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX idx_weigh_cycles_queue ON weigh_cycles(status, queued_at, id)`,
		`CREATE INDEX idx_weigh_cycles_identity ON weigh_cycles(status, plate_key, rfid)`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite v2 migration: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `PRAGMA user_version=2`); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) OpenCycle(ctx context.Context, cycle domain.WeighCycle) error {
	if cycle.ID == "" {
		return errors.New("cycle id is required")
	}
	if cycle.Status == "" {
		cycle.Status = domain.CycleQueued
	}
	if cycle.Status != domain.CycleQueued {
		return fmt.Errorf("new cycle must start QUEUED, got %s", cycle.Status)
	}
	cycle.FirstPass.CycleID = cycle.ID
	cycle.FirstPass.Number = domain.PassFirst
	if err := domain.ValidateFirstPass(cycle.FirstPass); err != nil {
		return err
	}
	if cycle.Plate == "" {
		cycle.Plate = cycle.FirstPass.Plate
	}
	if cycle.RFID == "" {
		cycle.RFID = cycle.FirstPass.RFID
	}
	if cycle.StationID == "" {
		cycle.StationID = cycle.FirstPass.StationID
	}
	if cycle.FirstPass.ID == "" {
		return errors.New("first pass id is required")
	}
	if cycle.QueuedAt.IsZero() {
		cycle.QueuedAt = cycle.FirstPass.CommittedAt
	}
	if cycle.QueuedAt.IsZero() {
		cycle.QueuedAt = time.Now().UTC()
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := insertPassTx(ctx, tx, cycle.FirstPass); err != nil {
		return err
	}
	if err := insertCycleTx(ctx, tx, cycle); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CallCycle(ctx context.Context, cycleID string, at time.Time) error {
	if cycleID == "" {
		return errors.New("cycle id is required")
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	cycle, found, err := getCycleTx(ctx, tx, cycleID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("cycle %s not found", cycleID)
	}
	if cycle.Status == domain.CycleCalled {
		return tx.Commit()
	}
	if cycle.Status != domain.CycleQueued {
		return fmt.Errorf("cycle %s cannot be called from status %s", cycleID, cycle.Status)
	}
	cycle.Status = domain.CycleCalled
	t := at.UTC()
	cycle.CalledAt = &t
	cycle.LastBlockReason = ""
	return updateCycleTx(ctx, tx, cycle, t)
}

func (s *Store) GetCycle(ctx context.Context, cycleID string) (domain.WeighCycle, bool, error) {
	row := s.db.QueryRowContext(ctx, `SELECT cycle_json FROM weigh_cycles WHERE id=?`, cycleID)
	var raw string
	if err := row.Scan(&raw); errors.Is(err, sql.ErrNoRows) {
		return domain.WeighCycle{}, false, nil
	} else if err != nil {
		return domain.WeighCycle{}, false, err
	}
	var cycle domain.WeighCycle
	if err := json.Unmarshal([]byte(raw), &cycle); err != nil {
		return domain.WeighCycle{}, false, err
	}
	return cycle, true, nil
}

func (s *Store) FindCalledCycle(ctx context.Context, plate, rfid string) (domain.WeighCycle, bool, error) {
	plateKey := normalizeVehicleKey(plate)
	if plateKey == "" {
		return domain.WeighCycle{}, false, nil
	}
	rows, err := s.db.QueryContext(ctx, `SELECT cycle_json FROM weigh_cycles WHERE status=? AND plate_key=? ORDER BY called_at, queued_at, id`, string(domain.CycleCalled), plateKey)
	if err != nil {
		return domain.WeighCycle{}, false, err
	}
	defer rows.Close()

	var match *domain.WeighCycle
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return domain.WeighCycle{}, false, err
		}
		var cycle domain.WeighCycle
		if err := json.Unmarshal([]byte(raw), &cycle); err != nil {
			return domain.WeighCycle{}, false, err
		}
		if rfid != "" && cycle.RFID != "" && cycle.RFID != rfid {
			continue
		}
		if match != nil {
			return domain.WeighCycle{}, false, fmt.Errorf("multiple CALLED cycles match plate %s; explicit cycle selection required", plate)
		}
		copy := cycle
		match = &copy
	}
	if err := rows.Err(); err != nil {
		return domain.WeighCycle{}, false, err
	}
	if match == nil {
		return domain.WeighCycle{}, false, nil
	}
	return *match, true, nil
}

func (s *Store) ListQueue(ctx context.Context) ([]domain.WeighCycle, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT cycle_json FROM weigh_cycles WHERE status IN (?,?) ORDER BY CASE status WHEN ? THEN 0 ELSE 1 END, COALESCE(called_at,queued_at), queued_at, id`,
		string(domain.CycleQueued), string(domain.CycleCalled), string(domain.CycleCalled))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []domain.WeighCycle{}
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			return nil, err
		}
		var cycle domain.WeighCycle
		if err := json.Unmarshal([]byte(raw), &cycle); err != nil {
			return nil, err
		}
		out = append(out, cycle)
	}
	return out, rows.Err()
}

func (s *Store) CompleteCycle(ctx context.Context, cycle domain.WeighCycle, ticket domain.Ticket) error {
	if cycle.SecondPass == nil {
		return errors.New("second pass is required to complete cycle")
	}
	second := *cycle.SecondPass
	second.CycleID = cycle.ID
	second.Number = domain.PassSecond
	validation := domain.ValidatePair(cycle.FirstPass, second, domain.PairPolicy{})
	if !validation.Valid {
		return fmt.Errorf("cycle pair is not intrinsically valid: %s", validation.Reason)
	}
	if cycle.Status != domain.CycleCalled && cycle.Status != domain.CycleComplete {
		return fmt.Errorf("cycle must be CALLED before completion, got %s", cycle.Status)
	}

	now := time.Now().UTC()
	if cycle.CompletedAt != nil && !cycle.CompletedAt.IsZero() {
		now = cycle.CompletedAt.UTC()
	}
	cycle.Status = domain.CycleComplete
	cycle.SecondPass = &second
	cycle.PairElapsed = validation.Elapsed
	cycle.GrossKG = validation.GrossKG
	cycle.TareKG = validation.TareKG
	cycle.NetKG = validation.NetKG
	cycle.Flow = validation.Flow
	cycle.LastBlockReason = ""
	cycle.CompletedAt = &now

	ticket = finalTicketFromCycle(ticket, cycle, now)
	if ticket.ID == "" {
		return errors.New("final ticket id is required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var status string
	if err := tx.QueryRowContext(ctx, `SELECT status FROM weigh_cycles WHERE id=?`, cycle.ID).Scan(&status); err != nil {
		return err
	}
	if domain.CycleStatus(status) != domain.CycleCalled {
		return fmt.Errorf("durable cycle %s is %s, not CALLED; completion forbidden", cycle.ID, status)
	}
	if err := insertPassTx(ctx, tx, second); err != nil {
		return err
	}
	if err := updateCycleTxNoCommit(ctx, tx, cycle, now); err != nil {
		return err
	}
	if err := insertFinalTicketTx(ctx, tx, ticket); err != nil {
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
	payload, err := json.Marshal(ticket)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO sync_queue(kind,entity_id,payload_json,created_at,next_attempt_at) VALUES('TICKET',?,?,?,?)`,
		ticket.ID, string(payload), now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	stateJSON, _ := json.Marshal(map[string]any{
		"ticket_id": ticket.ID,
		"cycle_id": cycle.ID,
		"completed_at": now,
		"gross_kg": cycle.GrossKG,
		"tare_kg": cycle.TareKG,
		"net_kg": cycle.NetKG,
	})
	_, err = tx.ExecContext(ctx, `INSERT INTO station_state(key,value_json,updated_at) VALUES('last_completed_cycle',?,?)
		ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at`, string(stateJSON), now.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MarkCycleStatus(ctx context.Context, cycleID string, status domain.CycleStatus, reason string, at time.Time) error {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	cycle, found, err := getCycleTx(ctx, tx, cycleID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("cycle %s not found", cycleID)
	}
	if cycle.Status == domain.CycleComplete {
		return errors.New("completed cycle status is immutable")
	}
	cycle.Status = status
	cycle.LastBlockReason = reason
	return updateCycleTx(ctx, tx, cycle, at.UTC())
}

func insertPassTx(ctx context.Context, tx *sql.Tx, pass domain.WeighPass) error {
	if pass.ID == "" || pass.CycleID == "" {
		return errors.New("pass id and cycle id are required")
	}
	payload, err := json.Marshal(pass)
	if err != nil {
		return err
	}
	overrides, err := json.Marshal(pass.Overrides)
	if err != nil {
		return err
	}
	evidence, err := json.Marshal(pass.CameraEvidence)
	if err != nil {
		return err
	}
	at := pass.CommittedAt
	if at.IsZero() {
		at = pass.Weight.ObservedAt
	}
	if at.IsZero() {
		at = time.Now().UTC()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO weigh_passes
		(id,cycle_id,pass_number,direction,station_id,plate,plate_key,rfid,weight_kg,weight_observed_at,weight_raw_seq,weight_raw_hash,mode,overrides_json,evidence_json,pass_json,committed_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		pass.ID, pass.CycleID, int(pass.Number), string(pass.Direction), pass.StationID, pass.Plate, normalizeVehicleKey(pass.Plate), pass.RFID,
		pass.Weight.WeightKG, pass.Weight.ObservedAt.UTC().Format(time.RFC3339Nano), pass.Weight.RawRef.Seq, pass.Weight.RawRef.Hash, string(pass.Mode),
		string(overrides), string(evidence), string(payload), at.UTC().Format(time.RFC3339Nano))
	return err
}

func insertCycleTx(ctx context.Context, tx *sql.Tx, cycle domain.WeighCycle) error {
	payload, err := json.Marshal(cycle)
	if err != nil {
		return err
	}
	updated := cycle.QueuedAt
	if updated.IsZero() {
		updated = time.Now().UTC()
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO weigh_cycles
		(id,station_id,plate,plate_key,rfid,status,first_pass_id,second_pass_id,queued_at,called_at,completed_at,pair_elapsed_ns,gross_kg,tare_kg,net_kg,last_block_reason,cycle_json,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		cycle.ID, cycle.StationID, cycle.Plate, normalizeVehicleKey(cycle.Plate), cycle.RFID, string(cycle.Status), cycle.FirstPass.ID, nullablePassID(cycle.SecondPass),
		cycle.QueuedAt.UTC().Format(time.RFC3339Nano), nullableTime(cycle.CalledAt), nullableTime(cycle.CompletedAt), int64(cycle.PairElapsed), cycle.GrossKG, cycle.TareKG, cycle.NetKG,
		cycle.LastBlockReason, string(payload), updated.UTC().Format(time.RFC3339Nano))
	return err
}

func getCycleTx(ctx context.Context, tx *sql.Tx, cycleID string) (domain.WeighCycle, bool, error) {
	var raw string
	err := tx.QueryRowContext(ctx, `SELECT cycle_json FROM weigh_cycles WHERE id=?`, cycleID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.WeighCycle{}, false, nil
	}
	if err != nil {
		return domain.WeighCycle{}, false, err
	}
	var cycle domain.WeighCycle
	if err := json.Unmarshal([]byte(raw), &cycle); err != nil {
		return domain.WeighCycle{}, false, err
	}
	return cycle, true, nil
}

func updateCycleTx(ctx context.Context, tx *sql.Tx, cycle domain.WeighCycle, updated time.Time) error {
	if err := updateCycleTxNoCommit(ctx, tx, cycle, updated); err != nil {
		return err
	}
	return tx.Commit()
}

func updateCycleTxNoCommit(ctx context.Context, tx *sql.Tx, cycle domain.WeighCycle, updated time.Time) error {
	payload, err := json.Marshal(cycle)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE weigh_cycles SET status=?,second_pass_id=?,called_at=?,completed_at=?,pair_elapsed_ns=?,gross_kg=?,tare_kg=?,net_kg=?,last_block_reason=?,cycle_json=?,updated_at=? WHERE id=?`,
		string(cycle.Status), nullablePassID(cycle.SecondPass), nullableTime(cycle.CalledAt), nullableTime(cycle.CompletedAt), int64(cycle.PairElapsed), cycle.GrossKG, cycle.TareKG, cycle.NetKG,
		cycle.LastBlockReason, string(payload), updated.UTC().Format(time.RFC3339Nano), cycle.ID)
	return err
}

func insertFinalTicketTx(ctx context.Context, tx *sql.Tx, ticket domain.Ticket) error {
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
	_, err = tx.ExecContext(ctx, `INSERT INTO tickets
		(id,station_id,transaction_id,plate,rfid,weight_kg,weight_observed_at,weight_raw_seq,weight_raw_hash,mode,overrides_json,ticket_json,committed_at,synced_at,
		 cycle_id,first_pass_id,second_pass_id,gross_kg,tare_kg,net_kg,first_weight_observed_at,second_weight_observed_at,first_weight_raw_seq,first_weight_raw_hash,second_weight_raw_seq,second_weight_raw_hash,pair_elapsed_seconds)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		ticket.ID, ticket.StationID, ticket.TransactionID, ticket.Plate, ticket.RFID,
		ticket.WeightKG, ticket.WeightObservedAt.UTC().Format(time.RFC3339Nano), ticket.WeightRawRef.Seq, ticket.WeightRawRef.Hash, string(ticket.Mode),
		string(overridesJSON), string(payload), now.Format(time.RFC3339Nano), nullableTime(ticket.SyncedAt),
		ticket.CycleID, ticket.FirstPassID, ticket.SecondPassID, ticket.GrossKG, ticket.TareKG, ticket.NetKG,
		ticket.FirstWeightObservedAt.UTC().Format(time.RFC3339Nano), ticket.SecondWeightObservedAt.UTC().Format(time.RFC3339Nano),
		ticket.FirstWeightRawRef.Seq, ticket.FirstWeightRawRef.Hash, ticket.SecondWeightRawRef.Seq, ticket.SecondWeightRawRef.Hash, ticket.PairElapsedSeconds)
	return err
}

func finalTicketFromCycle(ticket domain.Ticket, cycle domain.WeighCycle, now time.Time) domain.Ticket {
	second := *cycle.SecondPass
	ticket.CycleID = cycle.ID
	if ticket.TransactionID == "" {
		ticket.TransactionID = cycle.ID
	}
	if ticket.StationID == "" {
		ticket.StationID = cycle.StationID
	}
	if ticket.Plate == "" {
		ticket.Plate = cycle.Plate
	}
	if ticket.RFID == "" {
		ticket.RFID = cycle.RFID
	}
	ticket.FirstPassID = cycle.FirstPass.ID
	ticket.SecondPassID = second.ID
	ticket.GrossKG = cycle.GrossKG
	ticket.TareKG = cycle.TareKG
	ticket.NetKG = cycle.NetKG
	ticket.Flow = cycle.Flow
	ticket.FirstWeightObservedAt = cycle.FirstPass.Weight.ObservedAt
	ticket.SecondWeightObservedAt = second.Weight.ObservedAt
	ticket.FirstWeightRawRef = cycle.FirstPass.Weight.RawRef
	ticket.SecondWeightRawRef = second.Weight.RawRef
	ticket.PairElapsedSeconds = int64(cycle.PairElapsed / time.Second)
	if ticket.Mode == "" {
		ticket.Mode = combinedMode(cycle.FirstPass.Mode, second.Mode)
	}
	if len(ticket.Overrides) == 0 {
		ticket.Overrides = append(append([]domain.Override{}, cycle.FirstPass.Overrides...), second.Overrides...)
	}
	ticket.CommittedAt = now
	// Compatibility columns for schema-v1 readers. The authoritative completed
	// quantity is NetKG; raw pair evidence is in the explicit first/second refs.
	ticket.WeightKG = ticket.NetKG
	ticket.WeightObservedAt = ticket.SecondWeightObservedAt
	ticket.WeightRawRef = ticket.SecondWeightRawRef
	return ticket
}

func combinedMode(a, b domain.Mode) domain.Mode {
	rank := func(m domain.Mode) int {
		switch m {
		case domain.ModeLockout:
			return 4
		case domain.ModeManual:
			return 3
		case domain.ModeDegraded:
			return 2
		default:
			return 1
		}
	}
	if rank(b) > rank(a) {
		return b
	}
	if a == "" {
		return domain.ModeNormal
	}
	return a
}

func nullablePassID(p *domain.WeighPass) any {
	if p == nil || p.ID == "" {
		return nil
	}
	return p.ID
}

func normalizeVehicleKey(s string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
