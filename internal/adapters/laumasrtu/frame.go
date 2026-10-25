// Package laumasrtu reads a Laumas W100/W200 weight indicator over Modbus RTU
// (RS485). The frames are RTU frames whether they travel on a local COM port or
// through a transparent serial-to-Ethernet gateway (for example a Moxa NPort in
// TCP server / raw mode): this package never adds a Modbus TCP MBAP header.
//
// The register map below is the one proven on a real indicator by the Python
// prototype: one "read holding registers" request for 5 registers starting at
// address 0x0006.
//
//	reg 6   status word; bit 11 = STABLE
//	reg 7-8 weight, signed 32-bit, big-endian word order (MSW first), in kg
//	reg 9-10 not decoded (preserved in the raw audit record)
//
// Overload and indicator-fault bits are NOT part of the verified map. They are
// configurable masks (DecodeConfig) and stay disabled until commissioning gate
// H1 confirms them against the vendor manual and the physical display.
package laumasrtu

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

const (
	FuncReadHoldingRegisters byte = 0x03

	// StartRegister and RegisterCount are the verified read window.
	StartRegister uint16 = 0x0006
	RegisterCount uint16 = 5

	// RequestLen is slave + func + start(2) + count(2) + crc(2).
	RequestLen = 8
	// ResponseLen is slave + func + bytecount + 10 data bytes + crc(2).
	ResponseLen = 3 + 2*int(RegisterCount) + 2
	// ExceptionLen is slave + (func|0x80) + code + crc(2).
	ExceptionLen = 5

	// StatusStableBit is bit 11 of the status register.
	StatusStableBit uint = 11
)

var (
	ErrShortFrame   = errors.New("laumasrtu: frame too short")
	ErrBadCRC       = errors.New("laumasrtu: CRC mismatch")
	ErrWrongSlave   = errors.New("laumasrtu: response from unexpected slave address")
	ErrBadFunction  = errors.New("laumasrtu: unexpected function code")
	ErrBadByteCount = errors.New("laumasrtu: unexpected byte count")
)

// ExceptionError is a Modbus exception response from the indicator.
type ExceptionError struct {
	Function byte
	Code     byte
}

func (e ExceptionError) Error() string {
	return fmt.Sprintf("laumasrtu: Modbus exception 0x%02X for function 0x%02X", e.Code, e.Function&0x7F)
}

// CRC16 is the Modbus RTU CRC (polynomial 0xA001, init 0xFFFF). On the wire it
// is sent low byte first.
func CRC16(data []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, b := range data {
		crc ^= uint16(b)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

// AppendCRC appends the CRC low byte first.
func AppendCRC(frame []byte) []byte {
	crc := CRC16(frame)
	return append(frame, byte(crc), byte(crc>>8))
}

// BuildReadRequest builds the fixed weight poll for a slave address.
func BuildReadRequest(slave byte) []byte {
	req := make([]byte, 0, RequestLen)
	req = append(req, slave, FuncReadHoldingRegisters)
	req = binary.BigEndian.AppendUint16(req, StartRegister)
	req = binary.BigEndian.AppendUint16(req, RegisterCount)
	return AppendCRC(req)
}

// Registers is the decoded content of one poll response.
type Registers struct {
	Status   uint16
	WeightKG int32
	Reg9     uint16
	Reg10    uint16
}

// ParseReadResponse validates and decodes one complete response frame. It
// rejects anything that is not exactly the expected shape; there is no
// best-effort decoding of a damaged frame.
func ParseReadResponse(slave byte, frame []byte) (Registers, error) {
	if len(frame) < ExceptionLen {
		return Registers{}, fmt.Errorf("%w: %d bytes", ErrShortFrame, len(frame))
	}
	// The CRC is checked first so that a corrupted slave/function byte is
	// reported as corruption rather than as a protocol mismatch.
	if got, want := binary.LittleEndian.Uint16(frame[len(frame)-2:]), CRC16(frame[:len(frame)-2]); got != want {
		return Registers{}, fmt.Errorf("%w: frame has 0x%04X, computed 0x%04X", ErrBadCRC, got, want)
	}
	if frame[0] != slave {
		return Registers{}, fmt.Errorf("%w: got %d want %d", ErrWrongSlave, frame[0], slave)
	}
	if frame[1] == FuncReadHoldingRegisters|0x80 {
		if len(frame) != ExceptionLen {
			return Registers{}, fmt.Errorf("%w: exception frame is %d bytes", ErrShortFrame, len(frame))
		}
		return Registers{}, ExceptionError{Function: frame[1], Code: frame[2]}
	}
	if frame[1] != FuncReadHoldingRegisters {
		return Registers{}, fmt.Errorf("%w: 0x%02X", ErrBadFunction, frame[1])
	}
	if len(frame) != ResponseLen {
		return Registers{}, fmt.Errorf("%w: %d bytes, want %d", ErrShortFrame, len(frame), ResponseLen)
	}
	if frame[2] != byte(2*RegisterCount) {
		return Registers{}, fmt.Errorf("%w: %d", ErrBadByteCount, frame[2])
	}
	d := frame[3:13]
	return Registers{
		Status:   binary.BigEndian.Uint16(d[0:2]),
		WeightKG: int32(binary.BigEndian.Uint32(d[2:6])), // MSW first, two's complement
		Reg9:     binary.BigEndian.Uint16(d[6:8]),
		Reg10:    binary.BigEndian.Uint16(d[8:10]),
	}, nil
}

// EncodeReadResponse builds the response an indicator would send. It is used by
// the simulator and by the tests; production code only parses.
func EncodeReadResponse(slave byte, r Registers) []byte {
	out := make([]byte, 0, ResponseLen)
	out = append(out, slave, FuncReadHoldingRegisters, byte(2*RegisterCount))
	out = binary.BigEndian.AppendUint16(out, r.Status)
	out = binary.BigEndian.AppendUint32(out, uint32(r.WeightKG))
	out = binary.BigEndian.AppendUint16(out, r.Reg9)
	out = binary.BigEndian.AppendUint16(out, r.Reg10)
	return AppendCRC(out)
}

// DecodeConfig says how status bits become domain flags. Zero masks disable the
// corresponding decode: an unverified bit must never be guessed.
type DecodeConfig struct {
	// StableBit defaults to StatusStableBit (11) when StableBitSet is false.
	StableBit    uint
	StableBitSet bool
	// OverloadMask / FaultMask: any set bit in the status word raises the flag.
	OverloadMask uint16
	FaultMask    uint16
}

func (c DecodeConfig) stableBit() uint {
	if c.StableBitSet {
		return c.StableBit
	}
	return StatusStableBit
}

// Reading converts decoded registers into the domain reading. The reading is
// HEALTHY only because a CRC-valid, correctly shaped frame arrived; a missing
// frame never produces a reading at all.
func (c DecodeConfig) Reading(r Registers, observed time.Time) domain.ScaleReading {
	out := domain.ScaleReading{
		WeightKG: int64(r.WeightKG),
		Stable:   r.Status&(1<<c.stableBit()) != 0,
		Overload: c.OverloadMask != 0 && r.Status&c.OverloadMask != 0,
		Health:   domain.HealthHealthy,
		Observed: observed,
	}
	if c.FaultMask != 0 && r.Status&c.FaultMask != 0 {
		out.Fault = fmt.Sprintf("indicator status fault bits 0x%04X", r.Status&c.FaultMask)
		out.Health = domain.HealthFault
	}
	return out
}
