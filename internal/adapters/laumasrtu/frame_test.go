package laumasrtu

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

// Golden frames.
//
// PROVENANCE: these vectors were generated with the Python prototype's
// calc_modbus_crc / decode logic (weighbridge/modbus.py), which ran against a
// real Laumas indicator on COM3 @ 9600 8N1. They pin this Go codec to that
// proven behaviour. They are NOT captures from the physical device, so they do
// not close commissioning gate H1 ("golden-frame tests checked against the
// physical display"); replace or extend them with real captures on site.
type golden struct {
	name     string
	slave    byte
	frameHex string
	status   uint16
	weight   int32
	stable   bool
}

var goldenFrames = []golden{
	{"empty deck, stable", 1, "01030a08000000000000000000a55c", 0x0800, 0, true},
	{"loaded 28460 kg, stable", 1, "01030a080000006f2c000000003dc5", 0x0800, 28460, true},
	{"moving 12500 kg, not stable", 1, "01030a0000000030d4000000001154", 0x0000, 12500, false},
	{"tare 9100 kg, stable", 1, "01030a08000000238c00000000b3d0", 0x0800, 9100, true},
	{"negative -35 kg (zero drift), stable", 1, "01030a0800ffffffdd000000001d4b", 0x0800, -35, true},
	{"max int32, stable", 1, "01030a08007fffffff00000000ed2c", 0x0800, 2147483647, true},
	{"slave address 7", 7, "07030a0800000004d200000000150c", 0x0800, 1234, true},
}

func mustHex(t testing.TB, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCRC16PublishedReferenceVector(t *testing.T) {
	// Widely published Modbus RTU example: 01 03 00 00 00 01 -> CRC 84 0A on the wire.
	got := AppendCRC([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x01})
	if hex.EncodeToString(got) != "010300000001840a" {
		t.Fatalf("CRC reference vector wrong: %x", got)
	}
}

func TestBuildReadRequestGolden(t *testing.T) {
	if got := hex.EncodeToString(BuildReadRequest(1)); got != "01030006000565c8" {
		t.Fatalf("slave 1 request = %s", got)
	}
	// Shape for any slave: addr, 0x03, start 0x0006, count 0x0005, CRC.
	req := BuildReadRequest(9)
	if len(req) != RequestLen || req[0] != 9 || req[1] != 0x03 || req[2] != 0x00 || req[3] != 0x06 || req[4] != 0x00 || req[5] != 0x05 {
		t.Fatalf("bad request shape: %x", req)
	}
}

func TestGoldenResponsesDecode(t *testing.T) {
	for _, g := range goldenFrames {
		t.Run(g.name, func(t *testing.T) {
			frame := mustHex(t, g.frameHex)
			if len(frame) != ResponseLen {
				t.Fatalf("golden frame length %d != %d", len(frame), ResponseLen)
			}
			regs, err := ParseReadResponse(g.slave, frame)
			if err != nil {
				t.Fatal(err)
			}
			if regs.Status != g.status || regs.WeightKG != g.weight {
				t.Fatalf("regs = %+v want status=0x%04X weight=%d", regs, g.status, g.weight)
			}
			rd := DecodeConfig{}.Reading(regs, time.Unix(0, 0))
			if rd.WeightKG != int64(g.weight) || rd.Stable != g.stable || rd.Health != domain.HealthHealthy || rd.Overload || rd.Fault != "" {
				t.Fatalf("reading = %+v", rd)
			}
		})
	}
}

// Round trip: the encoder side used by the simulator must reproduce each golden
// frame byte for byte.
func TestEncodeMatchesGolden(t *testing.T) {
	for _, g := range goldenFrames {
		got := EncodeReadResponse(g.slave, Registers{Status: g.status, WeightKG: g.weight})
		if hex.EncodeToString(got) != g.frameHex {
			t.Fatalf("%s: encoded %x want %s", g.name, got, g.frameHex)
		}
	}
}

func TestParseRejectsMalformedFrames(t *testing.T) {
	good := mustHex(t, goldenFrames[1].frameHex)
	flip := func(i int) []byte { b := append([]byte(nil), good...); b[i] ^= 0xFF; return b }
	withCRC := func(b []byte) []byte { return AppendCRC(append([]byte(nil), b[:len(b)-2]...)) }

	wrongSlave := append([]byte(nil), good...)
	wrongSlave[0] = 2
	wrongFunc := append([]byte(nil), good...)
	wrongFunc[1] = 0x04
	badCount := append([]byte(nil), good...)
	badCount[2] = 8

	cases := []struct {
		name  string
		frame []byte
		want  error
	}{
		{"empty", nil, ErrShortFrame},
		{"truncated", good[:ResponseLen-1], nil}, // CRC no longer matches the shorter body
		{"flipped data byte", flip(6), ErrBadCRC},
		{"flipped crc byte", flip(ResponseLen - 1), ErrBadCRC},
		{"wrong slave, valid crc", withCRC(wrongSlave), ErrWrongSlave},
		{"wrong function, valid crc", withCRC(wrongFunc), ErrBadFunction},
		{"bad byte count, valid crc", withCRC(badCount), ErrBadByteCount},
		{"extra byte appended", append(append([]byte(nil), good...), 0x00), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			regs, err := ParseReadResponse(1, c.frame)
			if err == nil {
				t.Fatalf("malformed frame accepted: %+v", regs)
			}
			if c.want != nil && !errors.Is(err, c.want) {
				t.Fatalf("error = %v, want %v", err, c.want)
			}
		})
	}
}

func TestParseExceptionResponse(t *testing.T) {
	exc := AppendCRC([]byte{0x01, 0x83, 0x02}) // illegal data address
	_, err := ParseReadResponse(1, exc)
	var ee ExceptionError
	if !errors.As(err, &ee) || ee.Code != 0x02 {
		t.Fatalf("want ExceptionError code 2, got %v", err)
	}
	if !strings.Contains(err.Error(), "0x02") {
		t.Fatalf("exception text should carry the code: %v", err)
	}
}

func TestDecodeConfigOverloadAndFaultMasksAreOptIn(t *testing.T) {
	regs := Registers{Status: 0x0800 | 0x0100 | 0x0004, WeightKG: 5000}
	// Defaults: unverified bits are ignored, never guessed.
	rd := DecodeConfig{}.Reading(regs, time.Unix(0, 0))
	if rd.Overload || rd.Fault != "" || rd.Health != domain.HealthHealthy {
		t.Fatalf("unverified bits must not raise flags by default: %+v", rd)
	}
	rd = DecodeConfig{OverloadMask: 0x0100, FaultMask: 0x0004}.Reading(regs, time.Unix(0, 0))
	if !rd.Overload || rd.Fault == "" || rd.Health != domain.HealthFault {
		t.Fatalf("configured masks must raise overload and fault: %+v", rd)
	}
}

func TestDecodeConfigCustomStableBit(t *testing.T) {
	regs := Registers{Status: 1 << 3, WeightKG: 100}
	if (DecodeConfig{}).Reading(regs, time.Time{}).Stable {
		t.Fatal("bit 3 must not be stable under the default (bit 11)")
	}
	if !(DecodeConfig{StableBit: 3, StableBitSet: true}).Reading(regs, time.Time{}).Stable {
		t.Fatal("configured stable bit 3 must be honoured")
	}
}

func FuzzParseReadResponse(f *testing.F) {
	for _, g := range goldenFrames {
		b, _ := hex.DecodeString(g.frameHex)
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, frame []byte) {
		regs, err := ParseReadResponse(1, frame)
		if err != nil {
			return
		}
		// Anything accepted must be a CRC-valid, exactly-shaped frame that
		// re-encodes to the very same bytes.
		if got := EncodeReadResponse(1, regs); string(got) != string(frame) {
			t.Fatalf("accepted frame does not round trip: in=%x re=%x", frame, got)
		}
	})
}
