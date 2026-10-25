package w100

import (
	"encoding/binary"
	"io"
	"net"
	"sync"
)

// SimState is what a simulated W100 shows: whole kg, kg unit, no decimals.
type SimState struct {
	GrossKG int64
	Stable  bool
	Status  uint16 // extra status bits (faults, overload)
}

// Registers returns the 40007..40014 block for a state.
func (s SimState) Registers() []uint16 {
	st := s.Status
	g := s.GrossKG
	if g < 0 {
		st |= BitGrossNegative | BitNetNegative
		g = -g
	}
	if s.Stable {
		st |= BitStable
	}
	return []uint16{st, uint16(g >> 16), uint16(g), uint16(g >> 16), uint16(g), 0, 0, UnitKG<<8 | 4}
}

// Sim answers Modbus RTU function 03 on a byte stream, like a W100 behind a
// transparent RS485-Ethernet gateway. Used by tests, CI and the bench without hardware.
type Sim struct {
	Unit  byte
	mu    sync.Mutex
	state SimState
	Mute  bool // stop answering (cable pulled)
}

func (s *Sim) Set(st SimState) { s.mu.Lock(); s.state = st; s.mu.Unlock() }
func (s *Sim) SetMute(m bool)  { s.mu.Lock(); s.Mute = m; s.mu.Unlock() }

// Serve answers on one connection until it closes.
func (s *Sim) Serve(c io.ReadWriter) {
	req := make([]byte, 8)
	for {
		if _, err := io.ReadFull(c, req); err != nil {
			return
		}
		s.mu.Lock()
		st, mute := s.state, s.Mute
		s.mu.Unlock()
		if mute || req[0] != s.Unit || CRC16(req[:6]) != uint16(req[6])|uint16(req[7])<<8 {
			continue // a W100 stays silent on a bad frame
		}
		if req[1] != 0x03 {
			_, _ = c.Write(appendCRC([]byte{s.Unit, req[1] | 0x80, 1}))
			continue
		}
		start, n := binary.BigEndian.Uint16(req[2:]), binary.BigEndian.Uint16(req[4:])
		if start != RegStatus || n != StatusBlockLen {
			_, _ = c.Write(appendCRC([]byte{s.Unit, 0x83, 2}))
			continue
		}
		out := []byte{s.Unit, 0x03, byte(2 * n)}
		for _, r := range st.Registers() {
			out = binary.BigEndian.AppendUint16(out, r)
		}
		_, _ = c.Write(appendCRC(out))
	}
}

// ListenAndServe serves every connection on addr.
func (s *Sim) ListenAndServe(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() { defer c.Close(); s.Serve(c) }()
	}
}
