// Package fakescale is a simulated weight indicator inside the edge process. It
// behaves like the real thing where it matters to the workflow: after a weight
// change it reads NOT stable until it has settled, and every frame goes through
// the raw-weight audit journal before it becomes a reading (source "FAKE-SCALE"),
// so the audit chain stays intact and simulated frames are recognisable in it.
package fakescale

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

type RawWeightRecordJournal interface {
	AppendRecord(context.Context, domain.RawWeightEvent) (domain.RawWeightRef, error)
}

type State struct {
	WeightKG int64  `json:"weight_kg"`
	Stable   bool   `json:"stable"` // what the indicator shows right now
	Moving   bool   `json:"moving"` // operator holds the display unstable
	Overload bool   `json:"overload"`
	Fault    string `json:"fault,omitempty"`
	Frames   uint64 `json:"frames"`
}

// Patch changes only the fields that are present.
type Patch struct {
	WeightKG *int64  `json:"weight_kg,omitempty"`
	Moving   *bool   `json:"moving,omitempty"`
	Overload *bool   `json:"overload,omitempty"`
	Fault    *string `json:"fault,omitempty"`
}

type Scale struct {
	Poll   time.Duration // default 100 ms
	Settle time.Duration // time after a weight change before STABLE (default 700 ms)

	StationID     string
	TransactionID func() string
	Journal       RawWeightRecordJournal
	OnReading     func(domain.AuditedScaleReading)

	mu       sync.Mutex
	weight   int64
	moving   bool
	overload bool
	fault    string
	settleAt time.Time
	frames   uint64
}

// Apply changes the simulated indicator. A weight change makes the display
// unstable until it has settled.
func (s *Scale) Apply(p Patch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.WeightKG != nil && *p.WeightKG != s.weight {
		s.weight = *p.WeightKG
		s.settleAt = time.Now().Add(s.settle())
	}
	if p.Moving != nil {
		s.moving = *p.Moving
		if !s.moving {
			s.settleAt = time.Now().Add(s.settle())
		}
	}
	if p.Overload != nil {
		s.overload = *p.Overload
	}
	if p.Fault != nil {
		s.fault = *p.Fault
	}
}

func (s *Scale) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	stable := !s.moving && !time.Now().Before(s.settleAt)
	return State{WeightKG: s.weight, Stable: stable, Moving: s.moving, Overload: s.overload, Fault: s.fault, Frames: s.frames}
}

func (s *Scale) settle() time.Duration {
	if s.Settle <= 0 {
		return 700 * time.Millisecond
	}
	return s.Settle
}

func (s *Scale) poll() time.Duration {
	if s.Poll <= 0 {
		return 100 * time.Millisecond
	}
	return s.Poll
}

// Run produces one audited frame per poll until ctx ends or the journal fails.
func (s *Scale) Run(ctx context.Context) error {
	if s.Journal == nil {
		return fmt.Errorf("fakescale: raw weight journal is required")
	}
	t := time.NewTicker(s.poll())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		st := s.State()
		now := time.Now().UTC()
		reading := domain.ScaleReading{WeightKG: st.WeightKG, Stable: st.Stable, Overload: st.Overload, Fault: st.Fault, Health: domain.HealthHealthy, Observed: now}
		if st.Fault != "" {
			reading.Health = domain.HealthFault
		}
		text := fmt.Sprintf("FAKE WT=%d;ST=%d;OVERLOAD=%d;FAULT=%s", st.WeightKG, b2i(st.Stable), b2i(st.Overload), st.Fault)
		w, stb, ov := reading.WeightKG, reading.Stable, reading.Overload
		ref, err := s.Journal.AppendRecord(ctx, domain.RawWeightEvent{
			StationID: s.StationID, TransactionID: s.txID(), Kind: domain.RawWeightFrame, ReceivedAtUTC: now,
			Source: "FAKE-SCALE", RawBase64: base64.StdEncoding.EncodeToString([]byte(text)), RawText: text,
			WeightKG: &w, Stable: &stb, Overload: &ov, Fault: reading.Fault, Health: reading.Health, ParseOK: true,
		})
		if err != nil {
			return fmt.Errorf("raw weight audit append: %w", err)
		}
		s.mu.Lock()
		s.frames++
		s.mu.Unlock()
		if s.OnReading != nil {
			s.OnReading(domain.AuditedScaleReading{Reading: reading, RawRef: ref})
		}
	}
}

func (s *Scale) txID() string {
	if s.TransactionID == nil {
		return ""
	}
	return s.TransactionID()
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
