// fleetsim runs a whole simulated weighbridge site against the real edge
// binary: no scale, RFID reader, camera, I/O module or barrier hardware needed.
//
//	go build -o edge.exe ./cmd/edge
//	go run ./cmd/fleetsim -edge ./edge.exe -turns 40 -speed 20
//
// It prints an efficiency report and checks every completed ticket against the
// simulated truth; the exit status is non-zero on any discrepancy.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/sim/fleet"
)

func main() {
	var (
		edgeBin  = flag.String("edge", "edge.exe", "path to the built edge executable")
		workDir  = flag.String("data-dir", "", "edge data directory (default: a temp dir that is removed afterwards)")
		edgeLog  = flag.String("edge-log", "", "write the edge process output to this file")
		timeout  = flag.Duration("timeout", 30*time.Minute, "real-time limit for the whole run")
		keepEdge = flag.Bool("keep-edge", false, "leave the edge process running afterwards")
		verbose  = flag.Bool("v", false, "log simulator events")
		cfg      fleet.Config
		faults   = flag.Bool("faults", true, "inject RFID misses, LPR misreads and dropped scale polls")
	)
	flag.Float64Var(&cfg.Speed, "speed", 10, "simulated seconds per real second")
	flag.Int64Var(&cfg.Seed, "seed", 1, "random seed (same seed = same run)")
	flag.IntVar(&cfg.Fleet, "fleet", 16, "number of trucks")
	flag.IntVar(&cfg.MaxTurns, "turns", 40, "turns to run (a turn = two weighings); 0 = until -timeout")
	flag.Float64Var(&cfg.TurnsPerDay, "turns-per-day", 450, "site load used for the arrival rate")
	flag.Float64Var(&cfg.WorkHours, "work-hours", 16, "working hours the load is spread over (06:00-22:00 = 16)")
	flag.Float64Var(&cfg.InboundShare, "inbound-share", 0.6, "share of turns that are MATERIAL_IN (gross first)")
	flag.DurationVar(&cfg.TripMin, "trip-min", 6*time.Minute, "shortest simulated time between first and second weighing")
	flag.DurationVar(&cfg.TripMax, "trip-max", 25*time.Minute, "longest simulated time between first and second weighing")
	flag.Int64Var(&cfg.PayloadMinKG, "payload-min-kg", 14000, "smallest load per turn")
	flag.Int64Var(&cfg.PayloadMaxKG, "payload-max-kg", 28000, "largest load per turn")
	flag.DurationVar(&cfg.IOPoll, "io-poll", 100*time.Millisecond, "edge I/O poll interval (the edge is started with this value)")
	flag.DurationVar(&cfg.ScalePoll, "scale-poll", 100*time.Millisecond, "edge scale poll interval (the edge is started with this value)")
	flag.Parse()

	if *faults {
		cfg.RFIDMissRate, cfg.LPRMisreadRate, cfg.ScaleDropRate = 0.08, 0.08, 0.10
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	opts := fleet.SiteOptions{Config: cfg, EdgeBinary: *edgeBin, WorkDir: *workDir, Timeout: *timeout, KeepEdge: *keepEdge}
	if *verbose {
		opts.Logf = func(f string, a ...any) { fmt.Fprintf(os.Stderr, "[sim] "+f+"\n", a...) }
	}
	if *edgeLog != "" {
		f, err := os.Create(*edgeLog)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		defer f.Close()
		opts.EdgeLog = f
	}
	rep, err := fleet.RunSite(ctx, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fleetsim:", err)
		os.Exit(2)
	}
	fmt.Print(rep.String())
	if !rep.OK() {
		os.Exit(1)
	}
}
