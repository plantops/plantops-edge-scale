package fleet

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/sim/modbusio"
)

// BenchIO drives an edge whose I/O adapter is "fake" through the edge's HTTP
// bench API. It is the same plant, but the sensors and lamps live inside the
// edge process instead of behind a Modbus connection.
type BenchIO struct {
	edge *EdgeClient

	mu      sync.Mutex
	cached  benchIOState
	fetched time.Time
}

type benchIOState struct {
	Inputs struct {
		EntryPresent bool `json:"entry_present"`
		FrontPresent bool `json:"front_present"`
		RearPresent  bool `json:"rear_present"`
		ExitPresent  bool `json:"exit_present"`
		SafetyClear  bool `json:"safety_clear"`
	} `json:"inputs"`
	Outputs struct {
		EntryGreen       bool `json:"entry_green"`
		ExitGreen        bool `json:"exit_green"`
		Buzzer           bool `json:"buzzer"`
		EntryBarrierOpen bool `json:"entry_barrier_open_request"`
		ExitBarrierOpen  bool `json:"exit_barrier_open_request"`
	} `json:"outputs"`
}

func NewBenchIO(edge *EdgeClient) *BenchIO { return &BenchIO{edge: edge} }

func (b *BenchIO) SetDI(addr int, on bool) {
	field := map[int]string{
		modbusio.DIEntryPresent: "entry_present", modbusio.DIFrontPresent: "front_present",
		modbusio.DIRearPresent: "rear_present", modbusio.DIExitPresent: "exit_present", modbusio.DISafetyClear: "safety_clear",
	}[addr]
	if field == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = b.edge.do(ctx, http.MethodPost, "/api/fake/io", map[string]bool{field: on}, nil)
	b.mu.Lock()
	b.fetched = time.Time{} // force a fresh read of the outputs
	b.mu.Unlock()
}

// Coil reads what the workflow has driven on the fake I/O, with a short cache so
// the plant's 10 ms polling loops do not become 100 requests a second.
func (b *BenchIO) Coil(addr int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if time.Since(b.fetched) > 8*time.Millisecond {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var st benchIOState
		if _, err := b.edge.do(ctx, http.MethodGet, "/api/fake/io", nil, &st); err == nil {
			b.cached, b.fetched = st, time.Now()
		}
	}
	o := b.cached.Outputs
	switch addr {
	case modbusio.CoilEntryGreen:
		return o.EntryGreen
	case modbusio.CoilExitGreen:
		return o.ExitGreen
	case modbusio.CoilEntryRed:
		return !o.EntryGreen
	case modbusio.CoilExitRed:
		return !o.ExitGreen
	case modbusio.CoilBuzzer:
		return o.Buzzer
	case modbusio.CoilEntryBarrierOpen:
		return o.EntryBarrierOpen
	case modbusio.CoilExitBarrierOpen:
		return o.ExitBarrierOpen
	}
	return false
}

// BenchScale drives the edge's simulated indicator. Weight changes make it read
// unstable until it settles by itself, so "stable" is expressed as "not moving".
type BenchScale struct{ edge *EdgeClient }

func NewBenchScale(edge *EdgeClient) *BenchScale { return &BenchScale{edge: edge} }

func (b *BenchScale) SetWeight(kg int32, stable bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = b.edge.do(ctx, http.MethodPost, "/api/fake/scale", map[string]any{"weight_kg": int64(kg), "moving": !stable}, nil)
}

// DropNext is a no-op: a simulated in-process indicator has no serial link to drop.
func (b *BenchScale) DropNext(int) {}
