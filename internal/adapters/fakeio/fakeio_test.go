package fakeio

import (
	"context"
	"testing"
	"time"
)

func tp(v bool) *bool { return &v }

func TestStartsInSafeStateAndPatchChangesOnlyGivenInputs(t *testing.T) {
	f := New(0, true)
	st := f.State()
	if st.EntryBarrier != "CLOSED" || st.ExitBarrier != "CLOSED" || st.Outputs.EntryGreen || st.Outputs.ExitGreen || !st.Inputs.SafetyClear {
		t.Fatalf("initial state: %+v", st)
	}
	f.Patch(InputsPatch{EntryPresent: tp(true), SafetyClear: tp(false)})
	f.Patch(InputsPatch{FrontPresent: tp(true)}) // must not touch the others
	p, _ := f.ReadInputs(context.Background())
	if !p.EntryPresent || !p.FrontPresent || p.RearPresent || p.SafetyClear {
		t.Fatalf("patch semantics wrong: %+v", p)
	}
	if p.Observed.IsZero() {
		t.Fatal("inputs must carry an observation time")
	}
}

func TestBarrierIsNeitherOpenNorClosedWhileMoving(t *testing.T) {
	f := New(80*time.Millisecond, true)
	ctx := context.Background()
	_ = f.RequestEntryBarrier(ctx, true)
	p, _ := f.ReadInputs(ctx)
	if p.EntryBarrierOpen || p.EntryBarrierClosed {
		t.Fatalf("a moving barrier must report neither feedback: %+v", p)
	}
	if f.State().EntryBarrier != "MOVING" || !f.State().Outputs.EntryBarrierOpen {
		t.Fatalf("state: %+v", f.State())
	}
	time.Sleep(160 * time.Millisecond)
	p, _ = f.ReadInputs(ctx)
	if !p.EntryBarrierOpen || p.EntryBarrierClosed {
		t.Fatalf("barrier should be OPEN after its travel time: %+v", p)
	}
}

func TestNewBarrierRequestSupersedesAPendingMove(t *testing.T) {
	f := New(100*time.Millisecond, true)
	ctx := context.Background()
	_ = f.RequestExitBarrier(ctx, true)
	time.Sleep(30 * time.Millisecond)
	_ = f.RequestExitBarrier(ctx, false) // changed our mind before it arrived
	time.Sleep(200 * time.Millisecond)
	p, _ := f.ReadInputs(ctx)
	if p.ExitBarrierOpen || !p.ExitBarrierClosed {
		t.Fatalf("the later close request must win over the stale open move: %+v", p)
	}
}

func TestSafeStateDropsEveryPermissiveOutput(t *testing.T) {
	f := New(0, true)
	ctx := context.Background()
	_ = f.SetEntryLight(ctx, true)
	_ = f.SetExitLight(ctx, true)
	_ = f.SetBuzzer(ctx, true)
	_ = f.RequestEntryBarrier(ctx, true)
	_ = f.RequestExitBarrier(ctx, true)
	if err := f.SafeState(ctx); err != nil {
		t.Fatal(err)
	}
	o := f.State().Outputs
	if o.EntryGreen || o.ExitGreen || o.Buzzer || o.EntryBarrierOpen || o.ExitBarrierOpen {
		t.Fatalf("safe state left outputs on: %+v", o)
	}
	if f.State().EntryBarrier != "CLOSED" || f.State().ExitBarrier != "CLOSED" {
		t.Fatalf("barriers not closed: %+v", f.State())
	}
}
