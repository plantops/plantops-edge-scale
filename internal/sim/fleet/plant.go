package fleet

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/sim/laumassim"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/sim/modbusio"
)

// Expectation is what the sim knows to be true about one completed turn.
type Expectation struct {
	Plate    string
	Flow     Flow
	W1, W2   int64
	NetKG    int64
	Truck    int
	Sequence int // n-th completed turn of this truck
}

type Stats struct {
	PassesRun, TurnsStarted, TurnsDone                  int
	NoTruckAvailable                                    int
	CallFailures                                        int
	LampConflicts, BarrierConflicts                     int
	InjectedRFIDMiss, InjectedLPRBad, InjectedScaleDrop int
	ScaleBusy                                           time.Duration // real time a truck occupied the scale zone
	QueueWaitA, StoreWait                               []float64     // simulated seconds
	MaxQueueA, MaxReady                                 int
}

// Plant is the physical site. Exactly one truck is in the scale zone at a time
// (the lane is single and the edge runs one physical transaction at a time);
// the dispatcher decides who goes next.
type Plant struct {
	cfg  Config
	edge *EdgeClient
	io   *modbusio.Slave
	ind  *laumassim.Indicator
	logf func(string, ...any)

	mu       sync.Mutex
	rng      *rand.Rand
	trucks   []*Truck
	queueA   []*Truck
	ready    []*Truck
	started  time.Time
	arrivals bool
	stats    Stats
	expected []Expectation
	done     map[int]int
	stopped  bool
}

func NewPlant(cfg Config, edge *EdgeClient, io *modbusio.Slave, ind *laumassim.Indicator, logf func(string, ...any)) *Plant {
	cfg = cfg.withDefaults()
	if logf == nil {
		logf = func(string, ...any) {}
	}
	rng := rand.New(rand.NewSource(cfg.Seed))
	return &Plant{cfg: cfg, edge: edge, io: io, ind: ind, logf: logf, rng: rng,
		trucks: newFleet(cfg.Fleet, rng, cfg), arrivals: true, done: map[int]int{}}
}

func (p *Plant) Config() Config  { return p.cfg }
func (p *Plant) Fleet() []*Truck { return p.trucks }
func (p *Plant) Stats() Stats    { p.mu.Lock(); defer p.mu.Unlock(); return p.stats }
func (p *Plant) Expected() []Expectation {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Expectation(nil), p.expected...)
}
func (p *Plant) Elapsed() time.Duration { return time.Since(p.started) }

func (p *Plant) simNow() float64 { return time.Since(p.started).Seconds() * p.cfg.Speed }

func (p *Plant) chance(prob float64) bool {
	if prob <= 0 {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rng.Float64() < prob
}

func (p *Plant) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Run executes the site until MaxTurns turns have completed (or ctx ends).
func (p *Plant) Run(ctx context.Context) error {
	p.started = time.Now()
	p.ind.SetWeight(0, true)
	go p.monitor(ctx)
	go p.arrivalLoop(ctx)
	return p.schedule(ctx)
}

// arrivalLoop brings new turns to the A-side queue as a Poisson process.
func (p *Plant) arrivalLoop(ctx context.Context) {
	rate := p.cfg.TurnsPerDay / (p.cfg.WorkHours * 3600) // per simulated second
	for ctx.Err() == nil {
		p.mu.Lock()
		gap := p.rng.ExpFloat64() / rate
		p.mu.Unlock()
		if p.sleep(ctx, time.Duration(gap/p.cfg.Speed*float64(time.Second))) != nil {
			return
		}
		p.mu.Lock()
		if p.cfg.MaxTurns > 0 && p.stats.TurnsStarted >= p.cfg.MaxTurns {
			p.arrivals = false
			p.mu.Unlock()
			return
		}
		var idle []*Truck
		for _, t := range p.trucks {
			if t.Phase == PhaseIdle {
				idle = append(idle, t)
			}
		}
		if len(idle) == 0 {
			p.stats.NoTruckAvailable++
			p.mu.Unlock()
			continue
		}
		t := idle[p.rng.Intn(len(idle))]
		t.Task = TaskOut
		if p.rng.Float64() < p.cfg.InboundShare {
			t.Task = TaskIn
		}
		t.PayloadKG = (p.cfg.PayloadMinKG + p.rng.Int63n(p.cfg.PayloadMaxKG-p.cfg.PayloadMinKG+1)) / 10 * 10
		t.Residual1 = 0
		if p.rng.Float64() < p.cfg.ResidualShare {
			t.Residual1 = (1 + p.rng.Int63n(p.cfg.ResidualMaxKG)) / 10 * 10
		}
		t.Phase, t.Arrived, t.Called, t.Cycle = PhaseWaitingA, p.simNow(), false, ""
		p.queueA = append(p.queueA, t)
		p.stats.TurnsStarted++
		if len(p.queueA) > p.stats.MaxQueueA {
			p.stats.MaxQueueA = len(p.queueA)
		}
		p.mu.Unlock()
	}
}

// monitor watches the outputs for conditions that must never happen.
func (p *Plant) monitor(ctx context.Context) {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	var lampBad, barBad bool
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		l := p.io.Coil(modbusio.CoilEntryGreen) && p.io.Coil(modbusio.CoilExitGreen)
		b := p.io.Coil(modbusio.CoilEntryBarrierOpen) && p.io.Coil(modbusio.CoilExitBarrierOpen)
		p.mu.Lock()
		if l && !lampBad {
			p.stats.LampConflicts++
		}
		if b && !barBad {
			p.stats.BarrierConflicts++
		}
		p.mu.Unlock()
		lampBad, barBad = l, b
	}
}

func (p *Plant) finished() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.arrivals {
		return false
	}
	for _, t := range p.trucks {
		if t.Phase != PhaseIdle {
			return false
		}
	}
	return true
}

// pick chooses the next truck for the scale: the one that has waited longest on
// either side. Returns pass number 1 (from the A queue) or 2 (returning).
func (p *Plant) pick() (*Truck, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var a, b *Truck
	if len(p.queueA) > 0 {
		a = p.queueA[0]
	}
	bi := -1
	for i, t := range p.ready {
		if b == nil || t.Ready < b.Ready {
			b, bi = t, i
		}
	}
	switch {
	case a == nil && b == nil:
		return nil, 0
	case b == nil || (a != nil && a.Arrived <= b.Ready):
		p.queueA = p.queueA[1:]
		p.stats.QueueWaitA = append(p.stats.QueueWaitA, p.simNow()-a.Arrived)
		a.Phase = PhasePass1
		return a, 1
	default:
		p.ready = append(p.ready[:bi], p.ready[bi+1:]...)
		p.stats.StoreWait = append(p.stats.StoreWait, p.simNow()-b.Ready)
		b.Phase = PhasePass2
		return b, 2
	}
}

func (p *Plant) schedule(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, pass := p.pick()
		if t == nil {
			if p.finished() {
				return nil
			}
			if err := p.sleep(ctx, 20*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		if pass == 2 {
			if err := p.callCycle(ctx, t); err != nil {
				p.mu.Lock()
				p.stats.CallFailures++
				t.Phase = PhaseReadyAtStore
				p.ready = append(p.ready, t)
				p.mu.Unlock()
				p.logf("call failed for %s: %v", t.Plate, err)
				if err := p.sleep(ctx, 300*time.Millisecond); err != nil {
					return err
				}
				continue
			}
		}
		if err := p.runPass(ctx, t, pass); err != nil {
			return fmt.Errorf("truck %s pass %d: %w", t.Plate, pass, err)
		}
	}
}

// callCycle is the dispatcher: it CALLs the queued cycle of a truck that is
// ready at the store, exactly as an operator or an upstream system would.
func (p *Plant) callCycle(ctx context.Context, t *Truck) error {
	items, err := p.edge.Queue(ctx)
	if err != nil {
		return err
	}
	for _, it := range items {
		if it.RFID == t.RFID && it.Status == "QUEUED" {
			if err := p.edge.Call(ctx, it.ID); err != nil {
				return err
			}
			t.Cycle, t.Called = it.ID, true
			return nil
		}
	}
	return fmt.Errorf("no QUEUED cycle for RFID %s", t.RFID)
}

func (p *Plant) waitCoils(ctx context.Context, what string, a, b int) error {
	deadline := time.Now().Add(p.cfg.StepTimeout)
	for {
		if p.io.Coil(a) && p.io.Coil(b) {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s (coils %d and %d); edge says: %s", what, a, b, p.edgeStatus(ctx))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *Plant) edgeStatus(ctx context.Context) string {
	v, err := p.edge.Workflow(ctx)
	if err != nil {
		return "workflow unavailable: " + err.Error()
	}
	if v.Transaction == nil {
		return "idle (no transaction)"
	}
	return fmt.Sprintf("state=%s identity=%s dir=%s block=%q", v.Transaction.State, v.Transaction.Identity, v.Transaction.Direction, v.Transaction.LastBlockReason)
}

func (p *Plant) ramp(ctx context.Context, from, to int64, d time.Duration) error {
	step := p.cfg.ScalePoll / 2
	if step < 10*time.Millisecond {
		step = 10 * time.Millisecond
	}
	n := int(d / step)
	if n < 1 {
		n = 1
	}
	for i := 1; i <= n; i++ {
		p.ind.SetWeight(int32(from+(to-from)*int64(i)/int64(n)), false)
		if err := p.sleep(ctx, step); err != nil {
			return err
		}
	}
	return nil
}

func (p *Plant) identify(ctx context.Context, t *Truck) error {
	rfidMiss, lprBad := p.chance(p.cfg.RFIDMissRate), p.chance(p.cfg.LPRMisreadRate)
	img := fmt.Sprintf("sim/%s/%d.jpg", t.Plate, time.Now().UnixNano())
	if !rfidMiss {
		if err := p.edge.PostRFID(ctx, t.RFID, 0.97); err != nil {
			return err
		}
	}
	plate, conf := t.Plate, 0.95
	if lprBad {
		p.mu.Lock()
		plate = misread(t.Plate, p.rng)
		p.stats.InjectedLPRBad++
		p.mu.Unlock()
		conf = 0.55
	}
	if err := p.edge.PostLPR(ctx, plate, conf, img); err != nil {
		return err
	}
	if lprBad { // the camera re-reads the next frame and gets it right
		if err := p.sleep(ctx, p.cfg.scaled(p.cfg.IdentityS)); err != nil {
			return err
		}
		if err := p.edge.PostLPR(ctx, t.Plate, 0.96, img); err != nil {
			return err
		}
	}
	if rfidMiss { // the reader retries while the truck waits at the line
		p.mu.Lock()
		p.stats.InjectedRFIDMiss++
		p.mu.Unlock()
		if err := p.sleep(ctx, p.cfg.scaled(p.cfg.IdentityS)); err != nil {
			return err
		}
		return p.edge.PostRFID(ctx, t.RFID, 0.9)
	}
	return nil
}

// runPass choreographs one physical crossing. Pass 1 enters at side A (entry
// sensor, entry lamp/barrier) and leaves at side B; pass 2 is the mirror image.
func (p *Plant) runPass(ctx context.Context, t *Truck, pass int) error {
	cfg := p.cfg
	approach, exit := modbusio.DIEntryPresent, modbusio.DIExitPresent
	inGreen, inBar := modbusio.CoilEntryGreen, modbusio.CoilEntryBarrierOpen
	outGreen, outBar := modbusio.CoilExitGreen, modbusio.CoilExitBarrierOpen
	if pass == 2 {
		approach, exit = exit, approach
		inGreen, inBar, outGreen, outBar = outGreen, outBar, inGreen, inBar
	}
	front, rear := modbusio.DIFrontPresent, modbusio.DIRearPresent
	w := t.WeightKG(pass)
	zoneStart := time.Now()
	defer func() {
		p.mu.Lock()
		p.stats.ScaleBusy += time.Since(zoneStart)
		p.stats.PassesRun++
		p.mu.Unlock()
	}()
	step := func(simS float64) error { return p.sleep(ctx, cfg.scaled(simS)) }

	p.ind.SetWeight(0, true)
	if err := step(cfg.ApproachS); err != nil {
		return err
	}
	p.io.SetDI(approach, true)
	if err := step(cfg.IdentityS); err != nil {
		return err
	}
	if err := p.identify(ctx, t); err != nil {
		return fmt.Errorf("identify: %w", err)
	}

	if err := p.waitCoils(ctx, "entry permission", inGreen, inBar); err != nil {
		return err
	}
	if err := p.sleep(ctx, cfg.minHold()); err != nil {
		return err
	}

	// Drive onto the deck.
	p.io.SetDI(approach, false)
	if err := step(cfg.DriveOnS / 3); err != nil {
		return err
	}
	p.io.SetDI(front, true)
	if err := p.ramp(ctx, 0, w, cfg.scaled(cfg.DriveOnS*2/3)); err != nil {
		return err
	}
	p.io.SetDI(rear, true)
	if p.chance(cfg.ScaleDropRate) {
		p.ind.DropNext(1)
		p.mu.Lock()
		p.stats.InjectedScaleDrop++
		p.mu.Unlock()
	}
	if err := step(cfg.SettleS); err != nil {
		return err
	}
	p.ind.SetWeight(int32(w), true)

	// The edge accepts the stable weight and releases the other side.
	if err := p.waitCoils(ctx, "release", outGreen, outBar); err != nil {
		return err
	}
	if err := p.sleep(ctx, cfg.minHold()); err != nil {
		return err
	}

	// Leave.
	p.io.SetDI(front, false)
	p.io.SetDI(rear, false)
	p.io.SetDI(exit, true)
	if err := p.ramp(ctx, w, 0, cfg.scaled(cfg.LeaveS*0.4)); err != nil {
		return err
	}
	p.ind.SetWeight(0, true)
	if err := step(cfg.LeaveS * 0.6); err != nil {
		return err
	}
	p.io.SetDI(exit, false)

	// The edge must finish the physical transaction before the next truck.
	if err := p.waitIdle(ctx); err != nil {
		return err
	}
	return p.afterPass(t, pass)
}

func (p *Plant) waitIdle(ctx context.Context) error {
	deadline := time.Now().Add(p.cfg.StepTimeout)
	for {
		v, err := p.edge.Workflow(ctx)
		if err == nil && v.Transaction == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("edge did not return to idle: %s", p.edgeStatus(ctx))
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func (p *Plant) afterPass(t *Truck, pass int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if pass == 1 {
		t.Phase = PhaseInLoop
		span := p.cfg.TripMax - p.cfg.TripMin
		trip := p.cfg.TripMin + time.Duration(p.rng.Float64()*float64(span))
		trip -= 90 * time.Second // the two weighings themselves take about this long
		if trip < 30*time.Second {
			trip = 30 * time.Second
		}
		time.AfterFunc(time.Duration(float64(trip)/p.cfg.Speed), func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.stopped {
				return
			}
			t.Phase, t.Ready = PhaseReadyAtStore, p.simNow()
			p.ready = append(p.ready, t)
			if len(p.ready) > p.stats.MaxReady {
				p.stats.MaxReady = len(p.ready)
			}
		})
		return nil
	}
	p.done[t.ID]++
	p.expected = append(p.expected, Expectation{Plate: t.Plate, Flow: t.Task, W1: t.WeightKG(1), W2: t.WeightKG(2),
		NetKG: t.ExpectedNetKG(), Truck: t.ID, Sequence: p.done[t.ID]})
	t.Phase, t.Turns = PhaseIdle, t.Turns+1
	p.stats.TurnsDone++
	return nil
}

// Stop prevents trip timers from firing after the run.
func (p *Plant) Stop() { p.mu.Lock(); p.stopped = true; p.mu.Unlock() }

var ErrNoTickets = errors.New("no tickets received")
