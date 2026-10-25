package fleet

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

// Report is the outcome of a run: efficiency figures in the same terms the
// plant uses (tons per hour over effective time) and a list of correctness
// problems found by comparing every ticket with the sim's ground truth.
type Report struct {
	Cfg  Config
	Real time.Duration
	Sim  time.Duration

	Stats Stats

	Tickets, Duplicates           int
	FlowIn, FlowOut               int
	TonsIn, TonsOut               float64
	CycleMeanSim, CycleP95        time.Duration // first weighing -> second weighing
	QueueWaitMeanA, QueueWaitMaxA time.Duration
	StoreWaitMean                 time.Duration
	ScaleUtilisation              float64
	TPH                           float64 // total net tons per simulated hour of the run
	TurnsPerSimDay                float64 // extrapolated to a 16 h working day

	AuditWeightsOK, AuditEventsOK bool
	Problems                      []string
}

func (r *Report) OK() bool { return len(r.Problems) == 0 }

func (r *Report) problem(format string, a ...any) {
	r.Problems = append(r.Problems, fmt.Sprintf(format, a...))
}

func secDur(s float64) time.Duration { return time.Duration(s * float64(time.Second)) }

func meanMax(v []float64) (mean, max float64) {
	for _, x := range v {
		mean += x
		max = math.Max(max, x)
	}
	if len(v) > 0 {
		mean /= float64(len(v))
	}
	return
}

// BuildReport checks the tickets Central received against what really happened.
func BuildReport(cfg Config, plant *Plant, tickets []domain.Ticket, duplicates int, real time.Duration) *Report {
	r := &Report{Cfg: cfg, Real: real, Sim: cfg.simDuration(real), Stats: plant.Stats(), Tickets: len(tickets), Duplicates: duplicates}
	exp := plant.Expected()

	if duplicates > 0 {
		r.problem("%d duplicate ticket delivery(ies) reached Central", duplicates)
	}
	if len(tickets) != len(exp) {
		r.problem("Central holds %d ticket(s) but %d turn(s) completed", len(tickets), len(exp))
	}

	// Pair each truck's tickets with its expectations in completion order.
	byPlate := map[string][]domain.Ticket{}
	for _, t := range tickets {
		byPlate[t.Plate] = append(byPlate[t.Plate], t)
	}
	for _, ts := range byPlate {
		sort.Slice(ts, func(i, j int) bool { return ts[i].SecondWeightObservedAt.Before(ts[j].SecondWeightObservedAt) })
	}
	expByPlate := map[string][]Expectation{}
	for _, e := range exp {
		expByPlate[e.Plate] = append(expByPlate[e.Plate], e)
	}
	var cycles []float64
	for plate, es := range expByPlate {
		ts := byPlate[plate]
		for i, e := range es {
			if i >= len(ts) {
				r.problem("%s turn %d: no ticket", plate, e.Sequence)
				continue
			}
			t := ts[i]
			hi, lo := e.W1, e.W2
			if lo > hi {
				hi, lo = lo, hi
			}
			switch {
			case t.NetKG != e.NetKG:
				r.problem("%s turn %d: net %d kg, truth %d kg", plate, e.Sequence, t.NetKG, e.NetKG)
			case t.GrossKG != hi || t.TareKG != lo:
				r.problem("%s turn %d: gross/tare %d/%d, truth %d/%d", plate, e.Sequence, t.GrossKG, t.TareKG, hi, lo)
			case string(t.Flow) != string(e.Flow):
				r.problem("%s turn %d: flow %q, truth %q", plate, e.Sequence, t.Flow, e.Flow)
			case !t.FirstWeightObservedAt.Before(t.SecondWeightObservedAt):
				r.problem("%s turn %d: second weighing not after first", plate, e.Sequence)
			case t.FirstWeightRawRef.Seq == 0 || t.SecondWeightRawRef.Seq == 0:
				r.problem("%s turn %d: ticket lacks raw-weight audit references", plate, e.Sequence)
			}
		}
		if len(ts) > len(es) {
			r.problem("%s: %d surplus ticket(s) with no completed turn", plate, len(ts)-len(es))
		}
	}
	for _, t := range tickets {
		cycles = append(cycles, float64(t.PairElapsedSeconds)*cfg.Speed)
		tons := float64(t.NetKG) / 1000
		if t.Flow == domain.FlowMaterialOut {
			r.FlowOut++
			r.TonsOut += tons
		} else {
			r.FlowIn++
			r.TonsIn += tons
		}
	}

	if len(cycles) > 0 {
		sort.Float64s(cycles)
		m, _ := meanMax(cycles)
		r.CycleMeanSim = secDur(m)
		r.CycleP95 = secDur(cycles[int(math.Min(float64(len(cycles)-1), math.Ceil(0.95*float64(len(cycles)))-1))])
	}
	m, mx := meanMax(r.Stats.QueueWaitA)
	r.QueueWaitMeanA, r.QueueWaitMaxA = secDur(m), secDur(mx)
	m, _ = meanMax(r.Stats.StoreWait)
	r.StoreWaitMean = secDur(m)
	if real > 0 {
		r.ScaleUtilisation = r.Stats.ScaleBusy.Seconds() / real.Seconds()
	}
	if h := r.Sim.Hours(); h > 0 {
		r.TPH = (r.TonsIn + r.TonsOut) / h
		r.TurnsPerSimDay = float64(r.Stats.TurnsDone) / h * cfg.WorkHours
	}
	if r.Stats.LampConflicts > 0 {
		r.problem("both lamps were green at the same time %d time(s)", r.Stats.LampConflicts)
	}
	if r.Stats.BarrierConflicts > 0 {
		r.problem("both barriers were requested open at the same time %d time(s)", r.Stats.BarrierConflicts)
	}
	return r
}

func (r *Report) String() string {
	var b strings.Builder
	w := func(f string, a ...any) { fmt.Fprintf(&b, f+"\n", a...) }
	st := r.Stats
	w("FLEET SIMULATION  speed x%.0f  fleet %d trucks  seed %d", r.Cfg.Speed, r.Cfg.Fleet, r.Cfg.Seed)
	w("  run time        %s real  =  %s simulated", r.Real.Round(time.Second), fmtSim(r.Sim))
	w("  turns           %d started, %d completed, %d tickets at Central (%d passes through the scale)", st.TurnsStarted, st.TurnsDone, r.Tickets, st.PassesRun)
	w("  material        IN %d turns / %.1f t     OUT %d turns / %.1f t", r.FlowIn, r.TonsIn, r.FlowOut, r.TonsOut)
	w("  throughput      %.1f t per simulated hour   (= %.0f turns per 16 h day at this pace)", r.TPH, r.TurnsPerSimDay)
	w("  cycle           first->second weighing: mean %s, p95 %s", fmtSim(r.CycleMeanSim), fmtSim(r.CycleP95))
	w("  waiting         A-side queue: mean %s, max %s (peak %d trucks)   store: mean %s (peak %d ready)", fmtSim(r.QueueWaitMeanA), fmtSim(r.QueueWaitMaxA), st.MaxQueueA, fmtSim(r.StoreWaitMean), st.MaxReady)
	w("  scale zone      occupied %.0f%% of the time", r.ScaleUtilisation*100)
	w("  injected        RFID misses %d, LPR misreads %d, scale drops %d   |  dispatcher call retries %d, no-truck arrivals %d", st.InjectedRFIDMiss, st.InjectedLPRBad, st.InjectedScaleDrop, st.CallFailures, st.NoTruckAvailable)
	w("  audit chains    raw weights verified=%v   events verified=%v", r.AuditWeightsOK, r.AuditEventsOK)
	if r.OK() {
		w("  RESULT          PASS - every ticket matches the simulated truth; lamps/barriers never conflicted")
	} else {
		w("  RESULT          FAIL - %d problem(s):", len(r.Problems))
		for i, p := range r.Problems {
			if i >= 15 {
				w("    ... and %d more", len(r.Problems)-i)
				break
			}
			w("    - %s", p)
		}
	}
	return b.String()
}
