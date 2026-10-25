package laumasrtu_test

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/laumasrtu"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/sim/laumassim"
)

// memJournal is an in-memory raw-weight journal with a failure switch.
type memJournal struct {
	mu     sync.Mutex
	events []domain.RawWeightEvent
	fail   error
}

func (j *memJournal) AppendRecord(_ context.Context, e domain.RawWeightEvent) (domain.RawWeightRef, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.fail != nil {
		return domain.RawWeightRef{}, j.fail
	}
	j.events = append(j.events, e)
	return domain.RawWeightRef{Seq: uint64(len(j.events)), Hash: "h" + hex.EncodeToString([]byte{byte(len(j.events))})}, nil
}

func (j *memJournal) snapshot() []domain.RawWeightEvent {
	j.mu.Lock()
	defer j.mu.Unlock()
	return append([]domain.RawWeightEvent(nil), j.events...)
}

func (j *memJournal) count(kind domain.RawWeightEventKind, parseOK bool) int {
	n := 0
	for _, e := range j.snapshot() {
		if e.Kind == kind && e.ParseOK == parseOK {
			n++
		}
	}
	return n
}

type rig struct {
	ind      *laumassim.Indicator
	journal  *memJournal
	col      *laumasrtu.Collector
	mu       sync.Mutex
	readings []domain.AuditedScaleReading
	faults   []error
	done     chan error
	cancel   context.CancelFunc
}

func newRig(t *testing.T, mutate func(*laumasrtu.Collector)) *rig {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &rig{ind: laumassim.New(1), journal: &memJournal{}, done: make(chan error, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go func() { _ = r.ind.Serve(ctx, ln) }()
	r.col = &laumasrtu.Collector{
		Source: ln.Addr().String(), Dial: laumasrtu.TCPDialer(ln.Addr().String(), time.Second), Slave: 1,
		PollInterval: 4 * time.Millisecond, ResponseTimeout: 60 * time.Millisecond, ReconnectDelay: 15 * time.Millisecond, MaxMissedPolls: 3,
		StationID: "TEST", Journal: r.journal,
		OnReading: func(a domain.AuditedScaleReading) { r.mu.Lock(); r.readings = append(r.readings, a); r.mu.Unlock() },
		OnFault:   func(e error) { r.mu.Lock(); r.faults = append(r.faults, e); r.mu.Unlock() },
	}
	if mutate != nil {
		mutate(r.col)
	}
	go func() { r.done <- r.col.Run(ctx) }()
	t.Cleanup(func() { cancel() })
	return r
}

func (r *rig) readingCount() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.readings) }
func (r *rig) faultCount() int   { r.mu.Lock(); defer r.mu.Unlock(); return len(r.faults) }
func (r *rig) last() domain.AuditedScaleReading {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readings[len(r.readings)-1]
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(3 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestCollectorReadsAndJournalsExactGoldenBytes(t *testing.T) {
	r := newRig(t, nil)
	r.ind.SetWeight(28460, true)
	waitFor(t, "stable 28460 reading", func() bool { return r.readingCount() > 0 && r.last().Reading.WeightKG == 28460 })

	a := r.last()
	if !a.Reading.Stable || a.Reading.Health != domain.HealthHealthy || a.RawRef.Seq == 0 || a.RawRef.Hash == "" {
		t.Fatalf("bad audited reading: %+v", a)
	}
	// The journaled bytes are the exact controller frame: byte-for-byte the golden vector.
	var found bool
	for _, e := range r.journal.snapshot() {
		if e.Kind == domain.RawWeightFrame && e.WeightKG != nil && *e.WeightKG == 28460 {
			raw, _ := base64.StdEncoding.DecodeString(e.RawBase64)
			if hex.EncodeToString(raw) != "01030a080000006f2c000000003dc5" || !e.ParseOK || e.Source == "" {
				t.Fatalf("journal frame wrong: %x ok=%v", raw, e.ParseOK)
			}
			found = true
			break
		}
	}
	if !found {
		t.Fatal("no journaled FRAME for 28460 kg")
	}
}

func TestCollectorFollowsWeightAndStableBit(t *testing.T) {
	r := newRig(t, nil)
	r.ind.SetWeight(0, true)
	waitFor(t, "empty", func() bool { return r.readingCount() > 0 })
	r.ind.SetWeight(12500, false)
	waitFor(t, "moving", func() bool { a := r.last(); return a.Reading.WeightKG == 12500 && !a.Reading.Stable })
	r.ind.SetWeight(12500, true)
	waitFor(t, "stable", func() bool { a := r.last(); return a.Reading.WeightKG == 12500 && a.Reading.Stable })
}

func TestIsolatedDroppedPollsAreJournaledButNeverBecomeReadingsOrFaults(t *testing.T) {
	r := newRig(t, nil)
	r.ind.SetWeight(9100, true)
	waitFor(t, "first reading", func() bool { return r.readingCount() > 2 })

	for k := 0; k < 4; k++ { // four separate single drops, never 3 in a row
		before := r.journal.count(domain.RawWeightTransportError, false)
		r.ind.DropNext(1)
		waitFor(t, "drop journaled", func() bool { return r.journal.count(domain.RawWeightTransportError, false) > before })
		n := r.readingCount()
		waitFor(t, "recovery", func() bool { return r.readingCount() > n+1 })
	}
	if r.faultCount() != 0 {
		t.Fatalf("isolated drops must not raise a fault: %v", r.faults)
	}
}

func TestSilentIndicatorRaisesFaultThenRecoversByReconnect(t *testing.T) {
	r := newRig(t, nil)
	r.ind.SetWeight(5000, true)
	waitFor(t, "reading", func() bool { return r.readingCount() > 0 })

	r.ind.SetSilent(true)
	waitFor(t, "fault", func() bool { return r.faultCount() > 0 })
	r.mu.Lock()
	first := r.faults[0]
	r.mu.Unlock()
	if !errors.Is(first, laumasrtu.ErrIndicatorSilent) {
		t.Fatalf("fault should wrap ErrIndicatorSilent: %v", first)
	}
	n := r.readingCount()
	time.Sleep(60 * time.Millisecond)
	if r.readingCount() != n {
		t.Fatal("no readings may be produced while the indicator is silent (no synthetic/held-over values)")
	}

	r.ind.SetSilent(false)
	waitFor(t, "recovery after reconnect", func() bool { return r.readingCount() > n })
}

func TestCorruptCRCFramesAreJournaledAsFaultsAndNeverDecoded(t *testing.T) {
	r := newRig(t, nil)
	r.ind.SetWeight(7000, true)
	waitFor(t, "reading", func() bool { return r.readingCount() > 0 })

	n := r.readingCount()
	r.ind.CorruptNext(1)
	waitFor(t, "corrupt frame journaled", func() bool { return r.journal.count(domain.RawWeightFrame, false) > 0 })
	for _, e := range r.journal.snapshot() {
		if e.Kind == domain.RawWeightFrame && !e.ParseOK {
			if e.Error == "" || e.Health != domain.HealthFault || e.WeightKG != nil || e.RawBase64 == "" {
				t.Fatalf("corrupt frame must be journaled raw, flagged, with no decoded weight: %+v", e)
			}
		}
	}
	waitFor(t, "recovery", func() bool { return r.readingCount() > n+1 })
	if r.faultCount() != 0 {
		t.Fatalf("a single corrupt frame must not fault: %v", r.faults)
	}

	r.ind.CorruptNext(3)
	waitFor(t, "fault after 3 damaged frames", func() bool { return r.faultCount() > 0 })
}

func TestOverloadMaskRaisesOverloadFlag(t *testing.T) {
	r := newRig(t, func(c *laumasrtu.Collector) { c.Decode = laumasrtu.DecodeConfig{OverloadMask: 0x0100} })
	r.ind.SetWeight(41000, true)
	r.ind.SetStatusBits(0x0100, true)
	waitFor(t, "overload reading", func() bool { return r.readingCount() > 0 && r.last().Reading.Overload })
}

func TestJournalFailureStopsCollectorAndEmitsNothingUnaudited(t *testing.T) {
	r := newRig(t, nil)
	r.ind.SetWeight(1000, true)
	waitFor(t, "reading", func() bool { return r.readingCount() > 0 })

	r.journal.mu.Lock()
	r.journal.fail = errors.New("disk full")
	r.journal.mu.Unlock()
	select {
	case err := <-r.done:
		if err == nil {
			t.Fatal("Run must return an error when the raw journal cannot be written")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("collector kept running without a durable raw journal")
	}
	n := r.readingCount()
	time.Sleep(40 * time.Millisecond)
	if r.readingCount() != n {
		t.Fatal("a reading was delivered that is not in the journal")
	}
}

func TestWrongSlaveNeverAnswersSoCollectorFaults(t *testing.T) {
	r := newRig(t, func(c *laumasrtu.Collector) { c.Slave = 9 }) // the simulator is slave 1
	waitFor(t, "fault", func() bool { return r.faultCount() > 0 })
	if r.readingCount() != 0 {
		t.Fatal("no reading may come from a slave that never answered")
	}
}
