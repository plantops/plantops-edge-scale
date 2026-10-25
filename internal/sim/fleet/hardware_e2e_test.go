package fleet

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The edge started from the all-fake preset must say so everywhere, refuse to
// deliver simulated tickets to Central, and expose a strict bench API.
func TestAllFakeProfileIsDeclaredGuardedAndAudited(t *testing.T) {
	if testing.Short() {
		t.Skip("starts the edge binary")
	}
	bin := edgeBinary(t)
	dir := t.TempDir()
	addr, err := freeAddr()
	if err != nil {
		t.Fatal(err)
	}
	central, err := StartMockCentral(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, "-config", "../../../config/all-fake.json", "-listen", addr,
		"-db", filepath.Join(dir, "edge.db"), "-raw-weight-journal", filepath.Join(dir, "raw.jsonl"), "-event-journal", filepath.Join(dir, "ev.jsonl"),
		"-central-ticket-url", central.TicketURL(), "-central-heartbeat-url", central.HeartbeatURL(), "-central-sync-interval", "300ms")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _, _ = cmd.Process.Wait() }()

	edge := NewEdgeClient("http://" + addr)
	ctx := context.Background()
	if err := waitHealthy(ctx, edge); err != nil {
		t.Fatal(err)
	}
	if err := waitScaleLive(ctx, edge); err != nil {
		t.Fatal(err)
	}

	var hw struct {
		Mode      string   `json:"mode"`
		Simulated []string `json:"simulated_components"`
	}
	if _, err := edge.do(ctx, http.MethodGet, "/api/hardware", nil, &hw); err != nil {
		t.Fatal(err)
	}
	if hw.Mode != "SIMULATED" || strings.Join(hw.Simulated, ",") != "cameras,io,lpr,rfid,scale" {
		t.Fatalf("hardware profile = %+v", hw)
	}

	// Bench API: strict about unknown fields.
	if _, err := edge.do(ctx, http.MethodPost, "/api/fake/scale", map[string]any{"weigth_kg": 5}, nil); err == nil {
		t.Fatal("a misspelt bench field must be rejected")
	}
	var sc struct {
		WeightKG int64 `json:"weight_kg"`
	}
	if _, err := edge.do(ctx, http.MethodPost, "/api/fake/scale", map[string]any{"weight_kg": 12345}, &sc); err != nil || sc.WeightKG != 12345 {
		t.Fatalf("scale bench: %v %+v", err, sc)
	}

	// The profile is part of the audit trail.
	deadline := time.Now().Add(5 * time.Second)
	found := false
	for time.Now().Before(deadline) && !found {
		if f, err := os.Open(filepath.Join(dir, "ev.jsonl")); err == nil {
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 1<<20), 1<<20)
			for sc.Scan() {
				var rec struct {
					Event struct {
						Kind   string `json:"kind"`
						Action string `json:"action"`
					} `json:"event"`
				}
				if json.Unmarshal(sc.Bytes(), &rec) == nil && rec.Event.Kind == "HARDWARE_PROFILE" && rec.Event.Action == "SIMULATED" {
					found = true
				}
			}
			f.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		t.Fatal("HARDWARE_PROFILE event missing from the audit journal")
	}

	// Central delivery is blocked, and says why.
	var cs struct {
		Blocked bool   `json:"ticket_sync_blocked"`
		Reason  string `json:"block_reason"`
	}
	if _, err := edge.do(ctx, http.MethodGet, "/api/central/status", nil, &cs); err != nil {
		t.Fatal(err)
	}
	if !cs.Blocked || !strings.Contains(cs.Reason, "simulated") {
		t.Fatalf("central must be blocked while components are simulated: %+v", cs)
	}
	time.Sleep(700 * time.Millisecond)
	if n := len(central.Tickets()); n != 0 {
		t.Fatalf("%d ticket(s) reached Central from a simulated profile", n)
	}
}

// The whole fleet scenario, but with the edge simulating every component itself.
func TestEndToEndBenchProfileAllComponentsFake(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end simulation")
	}
	t.Parallel()
	cfg := baseCfg(4)
	cfg.InboundShare = 0.5
	cfg.RFIDMissRate, cfg.LPRMisreadRate = 0.25, 0.25
	rep, err := RunSite(context.Background(), SiteOptions{Config: cfg, EdgeBinary: edgeBinary(t), Timeout: 4 * time.Minute, Bench: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("\n" + rep.String())
	if !rep.OK() || rep.Stats.TurnsDone != 4 || rep.Tickets != 4 {
		t.Fatalf("bench-profile run failed: done=%d tickets=%d problems=%v", rep.Stats.TurnsDone, rep.Tickets, rep.Problems)
	}
}
