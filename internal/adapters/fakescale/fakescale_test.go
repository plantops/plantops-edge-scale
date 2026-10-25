package fakescale

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

type journal struct {
	mu     sync.Mutex
	events []domain.RawWeightEvent
	fail   error
}

func (j *journal) AppendRecord(_ context.Context, e domain.RawWeightEvent) (domain.RawWeightRef, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.fail != nil {
		return domain.RawWeightRef{}, j.fail
	}
	j.events = append(j.events, e)
	return domain.RawWeightRef{Seq: uint64(len(j.events)), Hash: "h"}, nil
}

func TestWeightChangeIsUnstableUntilSettled(t *testing.T) {
	s := &Scale{Settle: 120 * time.Millisecond}
	w := int64(27000)
	s.Apply(Patch{WeightKG: &w})
	if st := s.State(); st.WeightKG != 27000 || st.Stable {
		t.Fatalf("right after a change the display must read unstable: %+v", st)
	}
	time.Sleep(200 * time.Millisecond)
	if !s.State().Stable {
		t.Fatal("display should be stable after the settle time")
	}
	moving := true
	s.Apply(Patch{Moving: &moving})
	if s.State().Stable {
		t.Fatal("operator-forced motion must read unstable")
	}
}

func TestEveryFrameIsJournaledBeforeItBecomesAReading(t *testing.T) {
	j := &journal{}
	var mu sync.Mutex
	var got []domain.AuditedScaleReading
	s := &Scale{Poll: 5 * time.Millisecond, Settle: 10 * time.Millisecond, StationID: "T", Journal: j,
		OnReading: func(a domain.AuditedScaleReading) { mu.Lock(); got = append(got, a); mu.Unlock() }}
	w := int64(9100)
	s.Apply(Patch{WeightKG: &w})
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := s.Run(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) < 5 || len(j.events) < len(got) {
		t.Fatalf("readings=%d journaled=%d", len(got), len(j.events))
	}
	last := got[len(got)-1]
	if last.Reading.WeightKG != 9100 || !last.Reading.Stable || last.RawRef.Seq == 0 {
		t.Fatalf("last reading: %+v", last)
	}
	e := j.events[len(j.events)-1]
	if e.Source != "FAKE-SCALE" || e.RawText == "" || e.RawBase64 == "" || !e.ParseOK {
		t.Fatalf("simulated frames must be recognisable and complete in the raw journal: %+v", e)
	}
}

func TestJournalFailureStopsTheFakeScaleToo(t *testing.T) {
	j := &journal{fail: errors.New("disk full")}
	delivered := 0
	s := &Scale{Poll: 5 * time.Millisecond, Journal: j, OnReading: func(domain.AuditedScaleReading) { delivered++ }}
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("Run must fail when the journal cannot be written")
	}
	if delivered != 0 {
		t.Fatal("a reading must never be delivered without its journal record, even for a simulated scale")
	}
}
