package fleet

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/sim/laumassim"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/sim/modbusio"
)

// SiteOptions describes one self-contained simulated site.
type SiteOptions struct {
	Config     Config
	EdgeBinary string // path to the built edge executable; required
	WorkDir    string // data directory for the edge (db, journals); temp dir if empty
	EdgeLog    io.Writer
	Logf       func(string, ...any)
	Timeout    time.Duration // overall limit for the run (real time)
	ExtraEdge  []string      // extra edge flags, e.g. "-pair-require-gross-first"
	KeepEdge   bool          // leave the edge process running at the end
	// Bench runs the edge with every component set to adapter "fake" and drives it
	// through its HTTP bench API instead of Modbus / Laumas simulators.
	Bench bool
}

// EdgeArgs builds the edge flags that connect it to the simulated devices.
func EdgeArgs(cfg Config, listen, scaleAddr, ioAddr, ticketURL, heartbeatURL, dir, vehicleMap string) []string {
	cfg = cfg.withDefaults()
	return edgeArgs(cfg, false, listen, scaleAddr, ioAddr, ticketURL, heartbeatURL, dir, vehicleMap)
}

func edgeArgs(cfg Config, bench bool, listen, scaleAddr, ioAddr, ticketURL, heartbeatURL, dir, vehicleMap string) []string {
	devices := []string{
		"-scale-protocol", "laumas-rtu", "-scale-addr", scaleAddr, "-scale-slave", "1",
		"-scale-response-timeout", "300ms", "-scale-reconnect-delay", "300ms",
		"-io-addr", ioAddr, "-io-map", "safety_clear=8", "-io-buzzer-pulse", "50ms",
	}
	if bench {
		devices = []string{
			"-scale-protocol", "fake", "-io-adapter", "fake", "-rfid-adapter", "fake", "-lpr-adapter", "fake", "-camera-adapter", "fake",
			"-fake-settle", "350ms", "-fake-barrier-delay", "150ms", "-io-buzzer-pulse", "50ms",
			"-central-allow-simulated", // a simulator run may deliver to the mock Central
		}
	}
	args := []string{
		"-listen", listen,
		"-station-id", "SIM-01",
		"-db", filepath.Join(dir, "edge.db"),
		"-raw-weight-journal", filepath.Join(dir, "raw-weight.jsonl"),
		"-event-journal", filepath.Join(dir, "events.jsonl"),
		"-scale-poll", cfg.ScalePoll.String(), "-io-poll", cfg.IOPoll.String(),
		"-vehicle-map", vehicleMap,
		"-min-stable-weight-kg", "1000", "-stable-confirmations", "2", "-stable-tolerance-kg", "20",
		"-central-ticket-url", ticketURL, "-central-heartbeat-url", heartbeatURL, "-central-sync-interval", "500ms",
	}
	return append(args, devices...)
}

func freeAddr() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	return ln.Addr().String(), nil
}

// RunSite starts the simulated devices and a Central, spawns the real edge
// binary against them, runs the plant to completion and returns the report.
func RunSite(ctx context.Context, o SiteOptions) (*Report, error) {
	logf := o.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	if o.EdgeBinary == "" {
		return nil, fmt.Errorf("edge binary path is required")
	}
	if o.Timeout <= 0 {
		o.Timeout = 30 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, o.Timeout)
	defer cancel()

	dir := o.WorkDir
	if dir == "" {
		d, err := os.MkdirTemp("", "fleetsim-")
		if err != nil {
			return nil, err
		}
		dir = d
		defer os.RemoveAll(d)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	// Devices. In bench mode there are none: the edge simulates them itself.
	var (
		ind      *laumassim.Indicator
		slave    *modbusio.Slave
		scaleLn  net.Listener
		ioLn     net.Listener
		err      error
		scaleDev ScaleBackend
		ioDev    IOBackend
	)
	if !o.Bench {
		ind = laumassim.New(1)
		if scaleLn, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			return nil, err
		}
		go func() { _ = ind.Serve(ctx, scaleLn) }()
		slave = modbusio.New()
		if ioLn, err = net.Listen("tcp", "127.0.0.1:0"); err != nil {
			return nil, err
		}
		go func() { _ = slave.Serve(ctx, ioLn) }()
		scaleDev, ioDev = ind, slave
	}
	central, err := StartMockCentral(ctx, "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	edgeAddr, err := freeAddr()
	if err != nil {
		return nil, err
	}

	cfg := o.Config.withDefaults()
	edge := NewEdgeClient("http://" + edgeAddr)
	if o.Bench {
		scaleDev, ioDev = NewBenchScale(edge), NewBenchIO(edge)
	}
	plant := NewPlant(cfg, edge, ioDev, scaleDev, logf)

	scaleAddr, ioAddr := "", ""
	if !o.Bench {
		scaleAddr, ioAddr = scaleLn.Addr().String(), ioLn.Addr().String()
	}
	args := edgeArgs(cfg, o.Bench, edgeAddr, scaleAddr, ioAddr, central.TicketURL(), central.HeartbeatURL(), dir, VehicleMap(plant.Fleet()))
	args = append(args, o.ExtraEdge...)
	cmd := exec.CommandContext(ctx, o.EdgeBinary, args...)
	cmd.Stdout, cmd.Stderr = o.EdgeLog, o.EdgeLog
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start edge: %w", err)
	}
	if !o.KeepEdge {
		defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()
	}
	logf("edge pid %d on %s (bench=%v scale %q io %q central %s)", cmd.Process.Pid, edgeAddr, o.Bench, scaleAddr, ioAddr, central.Addr)

	if err := waitHealthy(ctx, edge); err != nil {
		return nil, err
	}
	if err := waitScaleLive(ctx, edge); err != nil {
		return nil, err
	}

	runErr := plant.Run(ctx)
	plant.Stop()
	real := plant.Elapsed()

	// Let Central receive the last tickets.
	want := plant.Stats().TurnsDone
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && len(central.Tickets()) < want {
		time.Sleep(100 * time.Millisecond)
	}
	central.mu.Lock()
	dups := central.Duplicates
	central.mu.Unlock()

	rep := BuildReport(cfg, plant, central.Tickets(), dups, real)
	vctx, vcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer vcancel()
	if w, e, verr := edge.Verify(vctx); verr != nil {
		rep.problem("audit verification call failed: %v", verr)
	} else {
		rep.AuditWeightsOK, rep.AuditEventsOK = w, e
		if !w {
			rep.problem("raw weight audit chain failed verification")
		}
		if !e {
			rep.problem("operational event audit chain failed verification")
		}
	}
	if runErr != nil && ctx.Err() == nil {
		rep.problem("plant stopped early: %v", runErr)
	} else if runErr != nil {
		rep.problem("run ended by the time limit before all turns completed: %v", runErr)
	}
	return rep, nil
}

func waitHealthy(ctx context.Context, edge *EdgeClient) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if edge.Healthy(ctx) {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("edge did not become healthy at %s", edge.Base)
}

// waitScaleLive waits until the edge is reading the simulated indicator, so the
// first truck is never sent in front of a blind scale.
func waitScaleLive(ctx context.Context, edge *EdgeClient) error {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		var st struct {
			Connected bool `json:"connected"`
		}
		if _, err := edge.do(ctx, "GET", "/api/scale/status", nil, &st); err == nil && st.Connected {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("edge never connected to the simulated Laumas indicator")
}
