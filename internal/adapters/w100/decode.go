// Package w100 reads a LAUMAS W100 / W200 weight indicator over Modbus RTU,
// either on a serial port (USB-RS485 dongle) or through an RS485-to-Ethernet
// gateway in transparent mode (RTU over TCP).
//
// Register map: LAUMAS "W200-W100 MODBUS" manual, list of usable registers and
// status register. The same map is already proven in the field on the SP01
// filling controller (firmware component tlb485, profile "w100").
package w100

import (
	"fmt"
	"strings"
)

// Registers, zero-based (manual 40007 = 6).
const (
	RegStatus      = 6 // 40007; the block 40007..40014 is read in one request
	StatusBlockLen = 8 // status, gross H/L, net H/L, peak H/L, divisions+unit

	idxStatus  = 0
	idxGrossH  = 1
	idxGrossL  = 2
	idxNetH    = 3
	idxNetL    = 4
	idxDivUnit = 7
)

// Status register bits (manual, "REGISTER STATUS").
const (
	BitCellError       = 1 << 0
	BitADCFault        = 1 << 1
	BitOverMax9Div     = 1 << 2
	BitOver110FS       = 1 << 3
	BitGrossOutOfRange = 1 << 4
	BitNetOutOfRange   = 1 << 5
	BitGrossNegative   = 1 << 7
	BitNetNegative     = 1 << 8
	BitNetDisplay      = 1 << 10
	BitStable          = 1 << 11
	BitNearZero        = 1 << 12
)

// Unit codes in the high byte of 40014, in the order of the manual's
// "setting units of measure" list. Only mass units are accepted for a weighbridge.
const (
	UnitKG = 0
	UnitG  = 1
	UnitT  = 2
)

// Block is one decoded status block. Weights are integers in kilograms,
// rounded from the instrument's displayed value.
type Block struct {
	Status   uint16
	GrossKG  int64
	NetKG    int64
	Decimals int
	Division uint8
	Unit     uint8
	Stable   bool
	Overload bool
	Fault    string // empty when healthy
	NetShown bool   // somebody switched the display to NET on the keypad
	NearZero bool
}

// decimalsFor maps the division code (low byte of 40014) to decimals. Same
// table the SP01 firmware uses; codes above 18 are not defined.
func decimalsFor(code uint8) (int, bool) {
	switch {
	case code <= 6:
		return 0, true
	case code <= 9:
		return 1, true
	case code <= 12:
		return 2, true
	case code <= 15:
		return 3, true
	case code <= 18:
		return 4, true
	}
	return 0, false
}

// toKG converts a displayed magnitude to whole kilograms, rounding half away from zero.
func toKG(magnitude uint32, decimals int, unit uint8) (int64, error) {
	num := int64(magnitude)
	den := int64(1)
	for i := 0; i < decimals; i++ {
		den *= 10
	}
	switch unit {
	case UnitKG:
	case UnitG:
		den *= 1000
	case UnitT:
		num *= 1000
	default:
		return 0, fmt.Errorf("W100 unit code %d is not a mass unit (kg, g, t)", unit)
	}
	return (num + den/2) / den, nil
}

// Decode turns the 8 registers 40007..40014 into a Block. The weighbridge
// uses GROSS: a tare taken on the keypad must never change a ticket.
func Decode(regs []uint16) (Block, error) {
	if len(regs) != StatusBlockLen {
		return Block{}, fmt.Errorf("W100 status block: %d registers, want %d", len(regs), StatusBlockLen)
	}
	st := regs[idxStatus]
	div := uint8(regs[idxDivUnit] & 0xFF)
	unit := uint8(regs[idxDivUnit] >> 8)
	dec, ok := decimalsFor(div)
	if !ok {
		return Block{}, fmt.Errorf("W100 division code %d unknown", div)
	}
	gross, err := toKG(uint32(regs[idxGrossH])<<16|uint32(regs[idxGrossL]), dec, unit)
	if err != nil {
		return Block{}, err
	}
	net, err := toKG(uint32(regs[idxNetH])<<16|uint32(regs[idxNetL]), dec, unit)
	if err != nil {
		return Block{}, err
	}
	if st&BitGrossNegative != 0 {
		gross = -gross
	}
	if st&BitNetNegative != 0 {
		net = -net
	}
	var faults []string
	if st&BitCellError != 0 {
		faults = append(faults, "CELL_ERROR")
	}
	if st&BitADCFault != 0 {
		faults = append(faults, "ADC_FAULT")
	}
	if st&BitGrossOutOfRange != 0 {
		faults = append(faults, "GROSS_OUT_OF_RANGE")
	}
	return Block{
		Status: st, GrossKG: gross, NetKG: net, Decimals: dec, Division: div, Unit: unit,
		Stable:   st&BitStable != 0,
		Overload: st&(BitOverMax9Div|BitOver110FS|BitGrossOutOfRange) != 0,
		Fault:    strings.Join(faults, ","),
		NetShown: st&BitNetDisplay != 0,
		NearZero: st&BitNearZero != 0,
	}, nil
}

// Text is the human-readable line kept next to the exact bytes in the raw journal.
func (b Block) Text() string {
	return fmt.Sprintf("W100 status=0x%04X gross_kg=%d net_kg=%d stable=%t overload=%t fault=%s net_display=%t div=%d unit=%d",
		b.Status, b.GrossKG, b.NetKG, b.Stable, b.Overload, b.Fault, b.NetShown, b.Division, b.Unit)
}
