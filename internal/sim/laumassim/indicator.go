// Package laumassim is a software stand-in for a Laumas W100/W200 indicator: a
// Modbus RTU slave that answers the weight poll on a TCP port (the way a
// transparent serial gateway would present it). It is test/simulation code only
// and is never linked into the production edge binary.
package laumassim

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/laumasrtu"
)

// Indicator holds the simulated display state and the fault-injection knobs.
type Indicator struct {
	slave byte

	mu          sync.Mutex
	weightKG    int32
	status      uint16
	dropNext    int           // swallow the next N requests (no reply)
	corruptNext int           // answer the next N requests with a damaged CRC
	silent      bool          // never answer (cable pulled)
	delay       time.Duration // answer latency

	requests atomic.Uint64
	replies  atomic.Uint64
}

func New(slave byte) *Indicator { return &Indicator{slave: slave} }

// SetWeight sets the displayed weight and the STABLE status bit.
func (i *Indicator) SetWeight(kg int32, stable bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.weightKG = kg
	if stable {
		i.status |= 1 << laumasrtu.StatusStableBit
	} else {
		i.status &^= 1 << laumasrtu.StatusStableBit
	}
}

func (i *Indicator) Weight() (kg int32, stable bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.weightKG, i.status&(1<<laumasrtu.StatusStableBit) != 0
}

// SetStatusBits ORs/clears raw status bits, for overload/fault decode tests.
func (i *Indicator) SetStatusBits(mask uint16, on bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if on {
		i.status |= mask
	} else {
		i.status &^= mask
	}
}

func (i *Indicator) DropNext(n int)           { i.mu.Lock(); i.dropNext = n; i.mu.Unlock() }
func (i *Indicator) CorruptNext(n int)        { i.mu.Lock(); i.corruptNext = n; i.mu.Unlock() }
func (i *Indicator) SetSilent(v bool)         { i.mu.Lock(); i.silent = v; i.mu.Unlock() }
func (i *Indicator) SetDelay(d time.Duration) { i.mu.Lock(); i.delay = d; i.mu.Unlock() }
func (i *Indicator) Requests() uint64         { return i.requests.Load() }
func (i *Indicator) Replies() uint64          { return i.replies.Load() }

// Serve accepts connections until ctx is cancelled or the listener fails.
func (i *Indicator) Serve(ctx context.Context, ln net.Listener) error {
	go func() { <-ctx.Done(); _ = ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go i.handle(ctx, conn)
	}
}

func (i *Indicator) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	req := make([]byte, laumasrtu.RequestLen)
	for {
		if ctx.Err() != nil {
			return
		}
		if _, err := io.ReadFull(conn, req); err != nil {
			return
		}
		i.requests.Add(1)
		reply, ok := i.answer(req)
		if !ok {
			continue
		}
		i.mu.Lock()
		d := i.delay
		i.mu.Unlock()
		if d > 0 {
			time.Sleep(d)
		}
		if _, err := conn.Write(reply); err != nil {
			return
		}
		i.replies.Add(1)
	}
}

// answer builds the reply to one request, or reports that the slave stays
// silent (wrong address, bad CRC, injected drop, cable pulled).
func (i *Indicator) answer(req []byte) ([]byte, bool) {
	if binary.LittleEndian.Uint16(req[6:]) != laumasrtu.CRC16(req[:6]) {
		return nil, false // RTU slaves ignore damaged requests
	}
	if req[0] != i.slave {
		return nil, false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.silent {
		return nil, false
	}
	if i.dropNext > 0 {
		i.dropNext--
		return nil, false
	}
	if req[1] != laumasrtu.FuncReadHoldingRegisters {
		return laumasrtu.AppendCRC([]byte{i.slave, req[1] | 0x80, 0x01}), true // illegal function
	}
	start, count := binary.BigEndian.Uint16(req[2:4]), binary.BigEndian.Uint16(req[4:6])
	if start != laumasrtu.StartRegister || count != laumasrtu.RegisterCount {
		return laumasrtu.AppendCRC([]byte{i.slave, req[1] | 0x80, 0x02}), true // illegal data address
	}
	resp := laumasrtu.EncodeReadResponse(i.slave, laumasrtu.Registers{Status: i.status, WeightKG: i.weightKG})
	if i.corruptNext > 0 {
		i.corruptNext--
		resp[len(resp)-1] ^= 0xFF
	}
	return resp, true
}
