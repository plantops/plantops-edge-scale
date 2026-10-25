// Package modbusio is a Modbus TCP slave that stands in for the remote I/O
// module (Moxa ioLogik reference): 32 discrete inputs and 32 coils, with the
// barrier controllers' OPEN/CLOSED feedback coupled to the open-request coils.
// Simulation/test code only; never linked into the production edge binary.
package modbusio

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Default logical addresses (modbustcp.DefaultMapping plus safety_clear=8).
const (
	DIEntryPresent       = 0
	DIFrontPresent       = 1
	DIRearPresent        = 2
	DIExitPresent        = 3
	DIEntryBarrierOpen   = 4
	DIEntryBarrierClosed = 5
	DIExitBarrierOpen    = 6
	DIExitBarrierClosed  = 7
	DISafetyClear        = 8

	CoilEntryRed         = 0
	CoilEntryGreen       = 1
	CoilExitRed          = 2
	CoilExitGreen        = 3
	CoilBuzzer           = 4
	CoilEntryBarrierOpen = 5
	CoilExitBarrierOpen  = 6

	size = 32
)

type Slave struct {
	mu    sync.RWMutex
	di    [size]bool
	coils [size]bool

	// BarrierDelay is how long a barrier takes to report OPEN/CLOSED after its
	// request coil changes. Zero means instantaneous feedback.
	BarrierDelay time.Duration

	Requests atomic.Uint64
	// onCoil, if set, observes every coil write (used by the plant monitor).
	onCoil func(addr int, on bool)
}

// New returns a slave in the safe initial plant state: empty lane, both
// barriers closed, physical safety clear.
func New() *Slave {
	s := &Slave{}
	s.di[DIEntryBarrierClosed] = true
	s.di[DIExitBarrierClosed] = true
	s.di[DISafetyClear] = true
	return s
}

func (s *Slave) OnCoilWrite(f func(addr int, on bool)) { s.mu.Lock(); s.onCoil = f; s.mu.Unlock() }

func (s *Slave) SetDI(addr int, on bool) { s.mu.Lock(); s.di[addr] = on; s.mu.Unlock() }
func (s *Slave) DI(addr int) bool        { s.mu.RLock(); defer s.mu.RUnlock(); return s.di[addr] }
func (s *Slave) Coil(addr int) bool      { s.mu.RLock(); defer s.mu.RUnlock(); return s.coils[addr] }

// Serve accepts connections until ctx ends. The edge client opens one TCP
// connection per request; persistent connections work too.
func (s *Slave) Serve(ctx context.Context, ln net.Listener) error {
	go func() { <-ctx.Done(); _ = ln.Close() }()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		go s.handle(ctx, conn)
	}
}

func (s *Slave) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	for ctx.Err() == nil {
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		header := make([]byte, 7)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		length := binary.BigEndian.Uint16(header[4:6])
		if length < 2 || length > 260 {
			return
		}
		pdu := make([]byte, int(length)-1)
		if _, err := io.ReadFull(conn, pdu); err != nil {
			return
		}
		s.Requests.Add(1)
		resp := s.process(pdu)
		out := make([]byte, 7+len(resp))
		copy(out[:2], header[:2])
		binary.BigEndian.PutUint16(out[4:6], uint16(len(resp)+1))
		out[6] = header[6]
		copy(out[7:], resp)
		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

func (s *Slave) process(pdu []byte) []byte {
	if len(pdu) < 1 {
		return []byte{0x80, 0x03}
	}
	switch pdu[0] {
	case 0x01, 0x02:
		if len(pdu) != 5 {
			return []byte{pdu[0] | 0x80, 0x03}
		}
		start, qty := int(binary.BigEndian.Uint16(pdu[1:3])), int(binary.BigEndian.Uint16(pdu[3:5]))
		if qty == 0 || start+qty > size {
			return []byte{pdu[0] | 0x80, 0x02}
		}
		n := (qty + 7) / 8
		resp := make([]byte, 2+n)
		resp[0], resp[1] = pdu[0], byte(n)
		s.mu.RLock()
		for i := 0; i < qty; i++ {
			v := s.coils[start+i]
			if pdu[0] == 0x02 {
				v = s.di[start+i]
			}
			if v {
				resp[2+i/8] |= 1 << uint(i%8)
			}
		}
		s.mu.RUnlock()
		return resp
	case 0x05:
		if len(pdu) != 5 {
			return []byte{0x85, 0x03}
		}
		addr, value := int(binary.BigEndian.Uint16(pdu[1:3])), binary.BigEndian.Uint16(pdu[3:5])
		if addr >= size {
			return []byte{0x85, 0x02}
		}
		if value != 0xFF00 && value != 0x0000 {
			return []byte{0x85, 0x03}
		}
		s.writeCoil(addr, value == 0xFF00)
		return append([]byte(nil), pdu...)
	default:
		return []byte{pdu[0] | 0x80, 0x01}
	}
}

func (s *Slave) writeCoil(addr int, on bool) {
	s.mu.Lock()
	s.coils[addr] = on
	cb := s.onCoil
	s.mu.Unlock()
	if cb != nil {
		cb(addr, on)
	}
	// Barrier feedback follows the open-request coil.
	var openDI, closedDI int
	switch addr {
	case CoilEntryBarrierOpen:
		openDI, closedDI = DIEntryBarrierOpen, DIEntryBarrierClosed
	case CoilExitBarrierOpen:
		openDI, closedDI = DIExitBarrierOpen, DIExitBarrierClosed
	default:
		return
	}
	apply := func() { s.mu.Lock(); s.di[openDI], s.di[closedDI] = on, !on; s.mu.Unlock() }
	if s.BarrierDelay <= 0 {
		apply()
		return
	}
	time.AfterFunc(s.BarrierDelay, apply)
}
