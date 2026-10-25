// Package fakeio is a simulated remote I/O module that lives inside the edge
// process. It implements the same Inputs/Outputs the Modbus adapter does, so the
// real runtime I/O controller (safe state, audited outputs, barrier-feedback
// timeouts, contradiction checks) runs unchanged on top of it. Presence sensors
// are set from the bench panel; lamps, buzzer and barrier requests are the
// outputs the workflow actually drove.
package fakeio

import (
	"context"
	"sync"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

// Inputs are the sensor states a person (or the bench panel) sets.
type Inputs struct {
	EntryPresent bool `json:"entry_present"`
	FrontPresent bool `json:"front_present"`
	RearPresent  bool `json:"rear_present"`
	ExitPresent  bool `json:"exit_present"`
	SafetyClear  bool `json:"safety_clear"`
}

// InputsPatch changes only the fields that are present.
type InputsPatch struct {
	EntryPresent *bool `json:"entry_present,omitempty"`
	FrontPresent *bool `json:"front_present,omitempty"`
	RearPresent  *bool `json:"rear_present,omitempty"`
	ExitPresent  *bool `json:"exit_present,omitempty"`
	SafetyClear  *bool `json:"safety_clear,omitempty"`
}

// Outputs are what the workflow has driven. A lamp is red when it is not green.
type Outputs struct {
	EntryGreen       bool `json:"entry_green"`
	ExitGreen        bool `json:"exit_green"`
	Buzzer           bool `json:"buzzer"`
	EntryBarrierOpen bool `json:"entry_barrier_open_request"`
	ExitBarrierOpen  bool `json:"exit_barrier_open_request"`
}

type State struct {
	Inputs       Inputs  `json:"inputs"`
	Outputs      Outputs `json:"outputs"`
	EntryBarrier string  `json:"entry_barrier"` // OPEN | CLOSED | MOVING
	ExitBarrier  string  `json:"exit_barrier"`
}

type IO struct {
	// BarrierDelay is the time a barrier takes to travel; during it neither the
	// OPEN nor the CLOSED feedback is true.
	BarrierDelay time.Duration

	mu                         sync.Mutex
	in                         Inputs
	out                        Outputs
	entryOpenFB, entryClosedFB bool
	exitOpenFB, exitClosedFB   bool
	entryGen, exitGen          int // a new request invalidates a pending barrier move
}

func New(barrierDelay time.Duration, safetyClear bool) *IO {
	return &IO{BarrierDelay: barrierDelay, in: Inputs{SafetyClear: safetyClear}, entryClosedFB: true, exitClosedFB: true}
}

func (f *IO) ReadInputs(context.Context) (domain.PositionSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return domain.PositionSnapshot{
		EntryPresent: f.in.EntryPresent, FrontPresent: f.in.FrontPresent, RearPresent: f.in.RearPresent, ExitPresent: f.in.ExitPresent,
		EntryBarrierOpen: f.entryOpenFB, EntryBarrierClosed: f.entryClosedFB,
		ExitBarrierOpen: f.exitOpenFB, ExitBarrierClosed: f.exitClosedFB,
		SafetyClear: f.in.SafetyClear, Observed: time.Now().UTC(),
	}, nil
}

func (f *IO) SetEntryLight(_ context.Context, green bool) error {
	f.mu.Lock()
	f.out.EntryGreen = green
	f.mu.Unlock()
	return nil
}

func (f *IO) SetExitLight(_ context.Context, green bool) error {
	f.mu.Lock()
	f.out.ExitGreen = green
	f.mu.Unlock()
	return nil
}

func (f *IO) SetBuzzer(_ context.Context, on bool) error {
	f.mu.Lock()
	f.out.Buzzer = on
	f.mu.Unlock()
	return nil
}

func (f *IO) RequestEntryBarrier(_ context.Context, open bool) error {
	f.mu.Lock()
	f.out.EntryBarrierOpen = open
	f.entryGen++
	gen := f.entryGen
	f.entryOpenFB, f.entryClosedFB = false, false // moving
	f.mu.Unlock()
	f.settle(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.entryGen == gen {
			f.entryOpenFB, f.entryClosedFB = open, !open
		}
	})
	return nil
}

func (f *IO) RequestExitBarrier(_ context.Context, open bool) error {
	f.mu.Lock()
	f.out.ExitBarrierOpen = open
	f.exitGen++
	gen := f.exitGen
	f.exitOpenFB, f.exitClosedFB = false, false
	f.mu.Unlock()
	f.settle(func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.exitGen == gen {
			f.exitOpenFB, f.exitClosedFB = open, !open
		}
	})
	return nil
}

func (f *IO) settle(apply func()) {
	if f.BarrierDelay <= 0 {
		apply()
		return
	}
	time.AfterFunc(f.BarrierDelay, apply)
}

// SafeState drops every permissive output and asks both barriers to close.
func (f *IO) SafeState(ctx context.Context) error {
	_ = f.SetBuzzer(ctx, false)
	_ = f.SetEntryLight(ctx, false)
	_ = f.SetExitLight(ctx, false)
	_ = f.RequestEntryBarrier(ctx, false)
	return f.RequestExitBarrier(ctx, false)
}

// Patch applies sensor changes from the bench panel.
func (f *IO) Patch(p InputsPatch) {
	f.mu.Lock()
	defer f.mu.Unlock()
	set := func(dst, src *bool) {
		if src != nil {
			*dst = *src
		}
	}
	set(&f.in.EntryPresent, p.EntryPresent)
	set(&f.in.FrontPresent, p.FrontPresent)
	set(&f.in.RearPresent, p.RearPresent)
	set(&f.in.ExitPresent, p.ExitPresent)
	set(&f.in.SafetyClear, p.SafetyClear)
}

func (f *IO) State() State {
	f.mu.Lock()
	defer f.mu.Unlock()
	pos := func(open, closed bool) string {
		switch {
		case open && !closed:
			return "OPEN"
		case closed && !open:
			return "CLOSED"
		default:
			return "MOVING"
		}
	}
	return State{Inputs: f.in, Outputs: f.out, EntryBarrier: pos(f.entryOpenFB, f.entryClosedFB), ExitBarrier: pos(f.exitOpenFB, f.exitClosedFB)}
}
