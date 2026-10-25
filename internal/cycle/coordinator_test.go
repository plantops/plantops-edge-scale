package cycle

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/sqlitestore"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

func TestBusinessCycleFirstQueueCallSecondNetComplete(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer store.Close()
	c := New(store, domain.PairPolicy{MaxElapsed: 4*time.Hour, MinNetKG: 1000})

	t0 := time.Date(2026,8,21,1,0,0,0,time.UTC)
	first := pass(domain.DirectionAToB, "15C-123.45", "RFID-1", 28460, t0, 11)
	open, err := c.RecordFirstPass(ctx, first)
	if err != nil { t.Fatal(err) }
	st, _ := store.Status(ctx)
	if st.QueuedCycles != 1 || st.Tickets != 0 || st.PendingSync != 0 {
		t.Fatalf("first pass incorrectly completed business tx: %+v", st)
	}

	if err := c.Call(ctx, open.ID); err != nil { t.Fatal(err) }
	second := pass(domain.DirectionBToA, "15C12345", "RFID-1", 11820, t0.Add(45*time.Minute), 22)
	completed, ticket, err := c.RecordSecondPass(ctx, second)
	if err != nil { t.Fatal(err) }
	if completed.Status != domain.CycleComplete || ticket.NetKG != 16640 || ticket.GrossKG != 28460 || ticket.TareKG != 11820 {
		t.Fatalf("bad completion cycle=%+v ticket=%+v", completed, ticket)
	}
	st, _ = store.Status(ctx)
	if st.QueuedCycles != 0 || st.CalledCycles != 0 || st.CompletedCycles != 1 || st.Tickets != 1 || st.PendingSync != 1 {
		t.Fatalf("final completion not atomic: %+v", st)
	}
}

func TestTareFirstOutboundCycleCompletesWithFlow(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer store.Close()
	c := New(store, domain.PairPolicy{MaxElapsed: 4*time.Hour, MinNetKG: 1000})

	t0 := time.Date(2026,8,21,1,0,0,0,time.UTC)
	open, err := c.RecordFirstPass(ctx, pass(domain.DirectionAToB, "61C-123.45", "RFID-9", 9100, t0, 11))
	if err != nil { t.Fatal(err) }
	if err := c.Call(ctx, open.ID); err != nil { t.Fatal(err) }
	done, ticket, err := c.RecordSecondPass(ctx, pass(domain.DirectionBToA, "61C12345", "RFID-9", 27000, t0.Add(18*time.Minute), 22))
	if err != nil { t.Fatalf("tare-first outbound trip must complete: %v", err) }
	if done.Flow != domain.FlowMaterialOut || ticket.Flow != domain.FlowMaterialOut {
		t.Fatalf("flow not MATERIAL_OUT: cycle=%q ticket=%q", done.Flow, ticket.Flow)
	}
	if ticket.GrossKG != 27000 || ticket.TareKG != 9100 || ticket.NetKG != 17900 {
		t.Fatalf("gross/tare roles wrong: %+v", ticket)
	}
}

func TestRequireGrossFirstPolicyRejectsTareFirstAndKeepsCycleCalled(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer store.Close()
	c := New(store, domain.PairPolicy{RequireGrossFirst: true})

	t0 := time.Now().UTC()
	open, err := c.RecordFirstPass(ctx, pass(domain.DirectionAToB, "61C-1", "R1", 9100, t0, 1))
	if err != nil { t.Fatal(err) }
	if err := c.Call(ctx, open.ID); err != nil { t.Fatal(err) }
	if _, _, err := c.RecordSecondPass(ctx, pass(domain.DirectionBToA, "61C-1", "R1", 27000, t0.Add(time.Minute), 2)); !errors.Is(err, ErrInvalidPair) {
		t.Fatalf("legacy policy must reject tare-first with ErrInvalidPair, got %v", err)
	}
	st, _ := store.Status(ctx)
	if st.Tickets != 0 || st.CalledCycles != 1 { t.Fatalf("rejected pair must leave cycle CALLED and create no ticket: %+v", st) }
}

func TestReturnTruckMustMatchCalledCycle(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer store.Close()
	c := New(store, domain.PairPolicy{MaxElapsed: 4*time.Hour})

	t0 := time.Now().UTC()
	open, err := c.RecordFirstPass(ctx, pass(domain.DirectionAToB, "15C-111.11", "RFID-1", 25000, t0, 1))
	if err != nil { t.Fatal(err) }
	if err := c.Call(ctx, open.ID); err != nil { t.Fatal(err) }

	_, _, err = c.RecordSecondPass(ctx, pass(domain.DirectionBToA, "15C-999.99", "RFID-X", 12000, t0.Add(time.Hour), 2))
	if !errors.Is(err, ErrNoCalledCycle) { t.Fatalf("expected no-called-cycle, got %v", err) }
	q, err := c.Queue(ctx)
	if err != nil || len(q) != 1 || q[0].Status != domain.CycleCalled {
		t.Fatalf("wrong truck must not consume correct called cycle: q=%+v err=%v", q, err)
	}
	st, _ := store.Status(ctx)
	if st.Tickets != 0 || st.PendingSync != 0 { t.Fatalf("wrong truck created business completion: %+v", st) }
}

func TestExpiredPairBecomesIncompleteEvidenceNotTicket(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer store.Close()
	c := New(store, domain.PairPolicy{MaxElapsed: 2*time.Hour})

	t0 := time.Now().UTC().Add(-3*time.Hour)
	open, err := c.RecordFirstPass(ctx, pass(domain.DirectionAToB, "15C-1", "R1", 25000, t0, 1))
	if err != nil { t.Fatal(err) }
	if err := c.Call(ctx, open.ID); err != nil { t.Fatal(err) }
	_, _, err = c.RecordSecondPass(ctx, pass(domain.DirectionBToA, "15C-1", "R1", 12000, t0.Add(3*time.Hour), 2))
	if !errors.Is(err, ErrPairWindowOver) { t.Fatalf("expected pair-window error, got %v", err) }
	st, _ := store.Status(ctx)
	if st.Tickets != 0 || st.PendingSync != 0 || st.OrphanCycles != 1 {
		t.Fatalf("expired pair became completion or lost evidence: %+v", st)
	}
}

func TestExpireOrphansClearsOnlyActiveQueue(t *testing.T) {
	ctx := context.Background()
	store, err := sqlitestore.Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer store.Close()
	c := New(store, domain.PairPolicy{MaxElapsed: time.Hour})

	t0 := time.Now().UTC().Add(-2*time.Hour)
	if _, err := c.RecordFirstPass(ctx, pass(domain.DirectionAToB, "15C-2", "R2", 22000, t0, 1)); err != nil { t.Fatal(err) }
	n, err := c.ExpireOrphans(ctx, time.Now().UTC())
	if err != nil || n != 1 { t.Fatalf("expired=%d err=%v", n, err) }
	q, _ := c.Queue(ctx)
	if len(q) != 0 { t.Fatalf("expired orphan remained active queue: %+v", q) }
	st, _ := store.Status(ctx)
	if st.OrphanCycles != 1 || st.Tickets != 0 || st.PendingSync != 0 { t.Fatalf("orphan state=%+v", st) }
}

func pass(direction domain.Direction, plate, rfid string, weight int64, at time.Time, seq uint64) domain.WeighPass {
	return domain.WeighPass{
		Direction: direction,
		StationID: "WHD-NC",
		Plate: plate,
		RFID: rfid,
		Mode: domain.ModeNormal,
		Weight: domain.WeightAcceptance{WeightKG:weight,ObservedAt:at,RawRef:domain.RawWeightRef{Seq:seq,Hash:"hash"}},
		CommittedAt: at.Add(time.Second),
	}
}
