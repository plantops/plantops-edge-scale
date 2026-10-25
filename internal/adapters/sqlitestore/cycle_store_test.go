package sqlitestore

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

func TestFirstPassCreatesQueueWithoutFinalTicketOrSync(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer s.Close()

	cycle := sampleCycle()
	if err := s.OpenCycle(ctx, cycle); err != nil { t.Fatal(err) }
	st, err := s.Status(ctx)
	if err != nil { t.Fatal(err) }
	if st.Schema != 2 || st.QueuedCycles != 1 || st.CalledCycles != 0 || st.CompletedCycles != 0 {
		t.Fatalf("unexpected cycle status: %+v", st)
	}
	if st.Tickets != 0 || st.PendingSync != 0 {
		t.Fatalf("first pass must not create final ticket/sync: %+v", st)
	}
	q, err := s.ListQueue(ctx)
	if err != nil || len(q) != 1 { t.Fatalf("queue=%+v err=%v", q, err) }
	if q[0].ID != cycle.ID || q[0].Status != domain.CycleQueued || q[0].FirstPass.Weight.WeightKG != 28460 {
		t.Fatalf("bad queue cycle: %+v", q[0])
	}
}

func TestSecondPassRequiresCalledCycleAndCompletesAtomically(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer s.Close()

	cycle := sampleCycle()
	if err := s.OpenCycle(ctx, cycle); err != nil { t.Fatal(err) }
	second := sampleSecondPass(cycle)
	cycle.SecondPass = &second
	cycle.Status = domain.CycleCalled

	if err := s.CompleteCycle(ctx, cycle, domain.Ticket{ID:"T-FINAL-1"}); err == nil {
		t.Fatal("completion without durable CALL must fail")
	}
	st, _ := s.Status(ctx)
	if st.Tickets != 0 || st.PendingSync != 0 || st.QueuedCycles != 1 {
		t.Fatalf("failed completion changed durable state: %+v", st)
	}

	calledAt := cycle.FirstPass.Weight.ObservedAt.Add(20*time.Minute)
	if err := s.CallCycle(ctx, cycle.ID, calledAt); err != nil { t.Fatal(err) }
	called, ok, err := s.FindCalledCycle(ctx, "15C12345", "RFID-1")
	if err != nil || !ok { t.Fatalf("called ok=%v err=%v", ok, err) }
	if called.Status != domain.CycleCalled || called.CalledAt == nil { t.Fatalf("called=%+v", called) }

	called.SecondPass = &second
	called.Status = domain.CycleCalled
	completedAt := second.Weight.ObservedAt.Add(time.Second)
	called.CompletedAt = &completedAt
	if err := s.CompleteCycle(ctx, called, domain.Ticket{ID:"T-FINAL-1"}); err != nil { t.Fatal(err) }

	st, err = s.Status(ctx)
	if err != nil { t.Fatal(err) }
	if st.QueuedCycles != 0 || st.CalledCycles != 0 || st.CompletedCycles != 1 || st.Tickets != 1 || st.PendingSync != 1 {
		t.Fatalf("completion not atomic: %+v", st)
	}
	last, ok, err := s.LastTicket(ctx)
	if err != nil || !ok { t.Fatalf("last ok=%v err=%v", ok, err) }
	if last.CycleID != cycle.ID || last.GrossKG != 28460 || last.TareKG != 11820 || last.NetKG != 16640 {
		t.Fatalf("bad paired ticket: %+v", last)
	}
	if last.FirstWeightRawRef.Seq != 101 || last.SecondWeightRawRef.Seq != 202 {
		t.Fatalf("raw pair refs missing: %+v", last)
	}
	if last.PairElapsedSeconds != int64((45*time.Minute)/time.Second) {
		t.Fatalf("bad elapsed seconds: %d", last.PairElapsedSeconds)
	}
}

func TestInvalidPairNeverCompletesOrClearsQueue(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer s.Close()

	cycle := sampleCycle()
	if err := s.OpenCycle(ctx, cycle); err != nil { t.Fatal(err) }
	if err := s.CallCycle(ctx, cycle.ID, time.Now().UTC()); err != nil { t.Fatal(err) }
	second := sampleSecondPass(cycle)
	second.Weight.WeightKG = cycle.FirstPass.Weight.WeightKG // equal weights: no net, never a valid pair
	cycle2, ok, err := s.GetCycle(ctx, cycle.ID)
	if err != nil || !ok { t.Fatal(err) }
	cycle2.SecondPass = &second
	if err := s.CompleteCycle(ctx, cycle2, domain.Ticket{ID:"BAD"}); err == nil {
		t.Fatal("invalid weight pair must not complete")
	}
	st, _ := s.Status(ctx)
	if st.CalledCycles != 1 || st.Tickets != 0 || st.PendingSync != 0 {
		t.Fatalf("invalid pair cleared/corrupted queue: %+v", st)
	}
}

// Outbound material: pass 1 is the empty truck (tare), pass 2 the loaded truck
// (gross). The durable ticket must carry gross/tare by role and the flow.
func TestTareFirstPairCompletesAsMaterialOut(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer s.Close()

	cycle := sampleCycle()
	cycle.FirstPass.Weight.WeightKG = 9100
	if err := s.OpenCycle(ctx, cycle); err != nil { t.Fatal(err) }
	if err := s.CallCycle(ctx, cycle.ID, time.Now().UTC()); err != nil { t.Fatal(err) }
	second := sampleSecondPass(cycle)
	second.Weight.WeightKG = 27000
	called, ok, err := s.GetCycle(ctx, cycle.ID)
	if err != nil || !ok { t.Fatal(err) }
	called.SecondPass = &second
	if err := s.CompleteCycle(ctx, called, domain.Ticket{ID: "T-OUT"}); err != nil {
		t.Fatalf("tare-first pair must complete: %v", err)
	}
	last, ok, err := s.LastTicket(ctx)
	if err != nil || !ok { t.Fatalf("last ok=%v err=%v", ok, err) }
	if last.Flow != domain.FlowMaterialOut || last.GrossKG != 27000 || last.TareKG != 9100 || last.NetKG != 17900 {
		t.Fatalf("tare-first ticket wrong: %+v", last)
	}
}

func TestOrphanIsRetainedButNeverBecomesTicket(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil { t.Fatal(err) }
	defer s.Close()

	cycle := sampleCycle()
	if err := s.OpenCycle(ctx, cycle); err != nil { t.Fatal(err) }
	if err := s.MarkCycleStatus(ctx, cycle.ID, domain.CycleOrphanedFirstPass, "pair window expired", cycle.QueuedAt.Add(8*time.Hour)); err != nil { t.Fatal(err) }
	st, err := s.Status(ctx)
	if err != nil { t.Fatal(err) }
	if st.OrphanCycles != 1 || st.QueuedCycles != 0 || st.Tickets != 0 || st.PendingSync != 0 {
		t.Fatalf("orphan semantics wrong: %+v", st)
	}
	q, err := s.ListQueue(ctx)
	if err != nil || len(q) != 0 { t.Fatalf("orphan must leave active queue q=%+v err=%v", q, err) }
}

func TestQueuedCycleSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "edge.db")
	s, err := Open(path)
	if err != nil { t.Fatal(err) }
	cycle := sampleCycle()
	if err := s.OpenCycle(ctx, cycle); err != nil { t.Fatal(err) }
	if err := s.Checkpoint(ctx); err != nil { t.Fatal(err) }
	if err := s.Close(); err != nil { t.Fatal(err) }

	s2, err := Open(path)
	if err != nil { t.Fatal(err) }
	defer s2.Close()
	q, err := s2.ListQueue(ctx)
	if err != nil || len(q) != 1 { t.Fatalf("reopen queue=%+v err=%v", q, err) }
	if q[0].ID != cycle.ID || q[0].Status != domain.CycleQueued { t.Fatalf("bad recovered cycle: %+v", q[0]) }
}

func sampleCycle() domain.WeighCycle {
	t0 := time.Date(2026,8,21,1,0,0,0,time.UTC)
	first := domain.WeighPass{
		ID:"P1",Number:domain.PassFirst,Direction:domain.DirectionAToB,StationID:"S01",Plate:"15C-123.45",RFID:"RFID-1",Mode:domain.ModeNormal,
		Weight:domain.WeightAcceptance{WeightKG:28460,ObservedAt:t0,RawRef:domain.RawWeightRef{Seq:101,Hash:"gross-hash"}},CommittedAt:t0.Add(time.Second),
	}
	return domain.WeighCycle{ID:"CYCLE-1",StationID:"S01",Plate:first.Plate,RFID:first.RFID,Status:domain.CycleQueued,FirstPass:first,QueuedAt:t0.Add(time.Second)}
}

func sampleSecondPass(cycle domain.WeighCycle) domain.WeighPass {
	return domain.WeighPass{
		ID:"P2",CycleID:cycle.ID,Number:domain.PassSecond,Direction:domain.DirectionBToA,StationID:cycle.StationID,Plate:"15C12345",RFID:cycle.RFID,Mode:domain.ModeNormal,
		Weight:domain.WeightAcceptance{WeightKG:11820,ObservedAt:cycle.FirstPass.Weight.ObservedAt.Add(45*time.Minute),RawRef:domain.RawWeightRef{Seq:202,Hash:"tare-hash"}},
		CommittedAt:cycle.FirstPass.Weight.ObservedAt.Add(45*time.Minute+time.Second),
	}
}
