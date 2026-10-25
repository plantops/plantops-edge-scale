package w100

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

func TestCRCKnownVector(t *testing.T) {
	// Modbus reference: 01 03 00 00 00 0A -> CRC C5 CD on the wire.
	got := ReadRequest(1, 0, 10)
	if got[6] != 0xC5 || got[7] != 0xCD {
		t.Fatalf("crc % X", got[6:])
	}
}

func TestDecode(t *testing.T) {
	cases := []struct {
		name string
		regs []uint16
		kg   int64
		st   bool
		ov   bool
		flt  string
	}{
		{"28460 kg stable", []uint16{BitStable, 0, 28460, 0, 28460, 0, 0, 0<<8 | 4}, 28460, true, false, ""},
		{"negative gross", []uint16{BitGrossNegative, 0, 20, 0, 0, 0, 0, 4}, -20, false, false, ""},
		{"tonnes 2 decimals", []uint16{BitStable, 0, 2846, 0, 0, 0, 0, UnitT<<8 | 11}, 28460, true, false, ""},
		{"grams", []uint16{0, 0x0001, 0x86A0, 0, 0, 0, 0, UnitG<<8 | 4}, 100, false, false, ""},
		{"over 110% FS", []uint16{BitOver110FS, 1, 4464, 0, 0, 0, 0, 4}, 70000, false, true, ""},
		{"cell error", []uint16{BitCellError, 0, 0, 0, 0, 0, 0, 4}, 0, false, false, "CELL_ERROR"},
		{"keypad tare ignored: ticket uses gross", []uint16{BitStable | BitNetDisplay, 0, 28460, 0, 460, 0, 0, 4}, 28460, true, false, ""},
	}
	for _, c := range cases {
		b, err := Decode(c.regs)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if b.GrossKG != c.kg || b.Stable != c.st || b.Overload != c.ov || b.Fault != c.flt {
			t.Errorf("%s: got %+v", c.name, b)
		}
	}
	if _, err := Decode([]uint16{0, 0, 0, 0, 0, 0, 0, 5<<8 | 4}); err == nil {
		t.Error("pounds accepted")
	}
	if _, err := Decode([]uint16{0, 0, 0, 0, 0, 0, 0, 19}); err == nil {
		t.Error("unknown division accepted")
	}
}

func TestParseRejectsBadFrames(t *testing.T) {
	ok := appendCRC([]byte{1, 3, 4, 0, 1, 0, 2})
	if r, err := ParseReadResponse(1, 2, ok); err != nil || r[1] != 2 {
		t.Fatal(r, err)
	}
	bad := append([]byte(nil), ok...)
	bad[4] ^= 1
	if _, err := ParseReadResponse(1, 2, bad); err == nil {
		t.Error("bad CRC accepted")
	}
	if _, err := ParseReadResponse(2, 2, ok); err == nil {
		t.Error("wrong unit accepted")
	}
	var ex ExceptionError
	if _, err := ParseReadResponse(1, 2, appendCRC([]byte{1, 0x83, 2})); !errors.As(err, &ex) || ex.Code != 2 {
		t.Error("exception not reported", err)
	}
}

type memJournal struct {
	mu   sync.Mutex
	ev   []domain.RawWeightEvent
	fail bool
}

func (m *memJournal) AppendRecord(_ context.Context, e domain.RawWeightEvent) (domain.RawWeightRef, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return domain.RawWeightRef{}, errors.New("disk full")
	}
	m.ev = append(m.ev, e)
	return domain.RawWeightRef{Seq: uint64(len(m.ev))}, nil
}

func (m *memJournal) kinds() (frames, transport int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.ev {
		if e.Kind == domain.RawWeightFrame {
			frames++
		} else {
			transport++
		}
	}
	return
}

func startSim(t *testing.T) (*Sim, string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	s := &Sim{Unit: 1}
	go s.ListenAndServe(ln)
	return s, "tcp:" + ln.Addr().String()
}

func waitFor(t *testing.T, what string, f func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout waiting for " + what)
}

func TestCollectorJournalsThenDelivers(t *testing.T) {
	sim, port := startSim(t)
	sim.Set(SimState{GrossKG: 0, Stable: true})
	j := &memJournal{}
	var mu sync.Mutex
	var got []domain.AuditedScaleReading
	var faults int
	c := &Collector{Port: port, Unit: 1, Poll: 10 * time.Millisecond, Timeout: 50 * time.Millisecond,
		RepeatEvery: time.Hour, ReconnectDelay: 20 * time.Millisecond, Journal: j,
		OnReading: func(a domain.AuditedScaleReading) { mu.Lock(); got = append(got, a); mu.Unlock() },
		OnFault:   func(error) { mu.Lock(); faults++; mu.Unlock() }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	count := func() int { mu.Lock(); defer mu.Unlock(); return len(got) }
	waitFor(t, "first reading", func() bool { return count() == 1 })
	time.Sleep(100 * time.Millisecond) // ~10 identical polls
	if n := count(); n != 1 {
		t.Fatalf("identical answers delivered %d times; want once per RepeatEvery", n)
	}
	sim.Set(SimState{GrossKG: 28460, Stable: true})
	waitFor(t, "changed weight", func() bool { return count() == 2 })
	mu.Lock()
	last := got[1]
	mu.Unlock()
	if last.Reading.WeightKG != 28460 || !last.Reading.Stable || last.RawRef.Seq == 0 {
		t.Fatalf("reading %+v", last)
	}
	if f, _ := j.kinds(); f != count() {
		t.Fatalf("journaled %d frames, delivered %d: every delivery must be journaled first", f, count())
	}

	// Cable pulled: three missed answers -> one transport record, fault, reopen.
	sim.SetMute(true)
	waitFor(t, "transport fault", func() bool { _, tr := j.kinds(); mu.Lock(); defer mu.Unlock(); return tr >= 1 && faults >= 1 })
	sim.SetMute(false)
	sim.Set(SimState{GrossKG: 11820, Stable: true})
	waitFor(t, "recovery", func() bool { mu.Lock(); defer mu.Unlock(); return got[len(got)-1].Reading.WeightKG == 11820 })

	// Journal failure stops the collector instead of letting weight through.
	j.mu.Lock()
	j.fail = true
	j.mu.Unlock()
	sim.Set(SimState{GrossKG: 500, Stable: false})
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("collector kept running without its journal")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not stop on journal failure")
	}
	mu.Lock()
	defer mu.Unlock()
	if got[len(got)-1].Reading.WeightKG == 500 {
		t.Fatal("unjournaled weight delivered")
	}
}
