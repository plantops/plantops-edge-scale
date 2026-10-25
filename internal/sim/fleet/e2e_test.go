package fleet

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

var (
	buildOnce sync.Once
	edgeBin   string
	buildErr  error
)

// edgeBinary builds the real edge executable once for all end-to-end tests.
func edgeBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "edge-e2e-")
		if err != nil {
			buildErr = err
			return
		}
		name := "edge"
		if runtime.GOOS == "windows" {
			name += ".exe"
		}
		edgeBin = filepath.Join(dir, name)
		cmd := exec.Command("go", "build", "-o", edgeBin, "../../../cmd/edge")
		if out, err := cmd.CombinedOutput(); err != nil {
			buildErr = &buildFailure{err: err, out: string(out)}
		}
	})
	if buildErr != nil {
		t.Fatalf("build edge: %v", buildErr)
	}
	return edgeBin
}

type buildFailure struct {
	err error
	out string
}

func (b *buildFailure) Error() string { return b.err.Error() + "\n" + b.out }

func runScenario(t *testing.T, cfg Config, timeout time.Duration) *Report {
	t.Helper()
	rep, err := RunSite(context.Background(), SiteOptions{Config: cfg, EdgeBinary: edgeBinary(t), Timeout: timeout})
	if err != nil {
		t.Fatalf("RunSite: %v", err)
	}
	t.Log("\n" + rep.String())
	if !rep.OK() {
		t.Fatalf("simulation found %d problem(s)", len(rep.Problems))
	}
	if rep.Stats.TurnsDone != cfg.MaxTurns || rep.Tickets != cfg.MaxTurns {
		t.Fatalf("expected %d completed turns and tickets, got %d / %d", cfg.MaxTurns, rep.Stats.TurnsDone, rep.Tickets)
	}
	return rep
}

func baseCfg(turns int) Config {
	return Config{
		Speed: 20, Seed: 7, Fleet: turns + 1, MaxTurns: turns, TurnsPerDay: 4000,
		TripMin: time.Minute, TripMax: 2 * time.Minute,
	}
}

func TestEndToEndMaterialInGrossFirst(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end simulation")
	}
	t.Parallel()
	cfg := baseCfg(3)
	cfg.InboundShare = 0.999
	rep := runScenario(t, cfg, 3*time.Minute)
	if rep.FlowIn != 3 || rep.FlowOut != 0 {
		t.Fatalf("flows in/out = %d/%d, want 3/0", rep.FlowIn, rep.FlowOut)
	}
}

// The case the legacy rule refused: empty on A->B (tare first), loaded on B->A.
func TestEndToEndMaterialOutTareFirst(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end simulation")
	}
	t.Parallel()
	cfg := baseCfg(3)
	cfg.InboundShare = 0.001
	rep := runScenario(t, cfg, 3*time.Minute)
	if rep.FlowOut != 3 || rep.FlowIn != 0 {
		t.Fatalf("flows in/out = %d/%d, want 0/3", rep.FlowIn, rep.FlowOut)
	}
}

// Missed RFID reads, misread plates (identity mismatch then re-read) and dropped
// scale polls on a mixed queue of trucks sharing one lane.
func TestEndToEndMixedTrafficWithFaultInjection(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end simulation")
	}
	t.Parallel()
	cfg := baseCfg(8)
	cfg.Fleet = 6
	cfg.RFIDMissRate, cfg.LPRMisreadRate, cfg.ScaleDropRate = 0.35, 0.35, 0.35
	rep := runScenario(t, cfg, 5*time.Minute)
	if rep.Stats.InjectedRFIDMiss+rep.Stats.InjectedLPRBad+rep.Stats.InjectedScaleDrop == 0 {
		t.Fatal("fault injection did not fire; the scenario proves nothing")
	}
}
