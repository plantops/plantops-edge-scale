package w100

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"
)

// CRC16 is the Modbus RTU CRC (poly 0xA001, init 0xFFFF), low byte first on the wire.
func CRC16(b []byte) uint16 {
	crc := uint16(0xFFFF)
	for _, x := range b {
		crc ^= uint16(x)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = crc>>1 ^ 0xA001
			} else {
				crc >>= 1
			}
		}
	}
	return crc
}

func appendCRC(b []byte) []byte {
	c := CRC16(b)
	return append(b, byte(c), byte(c>>8))
}

// ReadRequest builds function 03 (read holding registers).
func ReadRequest(unit byte, start, count uint16) []byte {
	b := []byte{unit, 0x03, 0, 0, 0, 0}
	binary.BigEndian.PutUint16(b[2:], start)
	binary.BigEndian.PutUint16(b[4:], count)
	return appendCRC(b)
}

// ErrNoAnswer: the W100 does not answer a frame with a bad CRC or a wrong
// address; the master sees only a timeout (manual, communication error management).
var ErrNoAnswer = errors.New("W100 no answer (timeout)")

// ExceptionError is a Modbus exception answer.
type ExceptionError struct{ Code byte }

func (e ExceptionError) Error() string {
	names := map[byte]string{1: "ILLEGAL FUNCTION", 2: "ILLEGAL DATA ADDRESS", 3: "ILLEGAL DATA VALUE"}
	return fmt.Sprintf("W100 exception %d %s", e.Code, names[e.Code])
}

// ParseReadResponse checks a function 03 answer and returns its registers.
func ParseReadResponse(unit byte, count uint16, frame []byte) ([]uint16, error) {
	if len(frame) < 5 {
		return nil, fmt.Errorf("W100 frame too short (%d bytes)", len(frame))
	}
	n := len(frame)
	if got, want := uint16(frame[n-2])|uint16(frame[n-1])<<8, CRC16(frame[:n-2]); got != want {
		return nil, fmt.Errorf("W100 CRC mismatch: got %04X want %04X", got, want)
	}
	if frame[0] != unit {
		return nil, fmt.Errorf("W100 answer from unit %d, asked %d", frame[0], unit)
	}
	if frame[1] == 0x83 {
		return nil, ExceptionError{Code: frame[2]}
	}
	if frame[1] != 0x03 {
		return nil, fmt.Errorf("W100 answer function 0x%02X, want 0x03", frame[1])
	}
	if int(frame[2]) != 2*int(count) || n != 5+2*int(count) {
		return nil, fmt.Errorf("W100 answer carries %d bytes, want %d", frame[2], 2*count)
	}
	regs := make([]uint16, count)
	for i := range regs {
		regs[i] = binary.BigEndian.Uint16(frame[3+2*i:])
	}
	return regs, nil
}

// Port is a serial line or an RTU-over-TCP connection. Read returns what has
// arrived within a short poll interval, possibly 0 bytes with a nil error.
type Port interface {
	io.ReadWriteCloser
	Flush() error // drop stale input before a request
}

// Transact sends one request and collects one answer frame, by length.
// It returns the exact bytes received even when the answer is unusable.
func Transact(p Port, req []byte, timeout time.Duration) ([]byte, error) {
	_ = p.Flush()
	if _, err := p.Write(req); err != nil {
		return nil, fmt.Errorf("W100 write: %w", err)
	}
	deadline := time.Now().Add(timeout)
	buf := make([]byte, 0, 64)
	tmp := make([]byte, 64)
	for {
		want := 5 // exception length, and enough to know the real length
		if len(buf) >= 3 && buf[1] == 0x03 {
			want = 5 + int(buf[2])
		}
		if len(buf) >= want {
			return buf[:want], nil
		}
		if time.Now().After(deadline) {
			if len(buf) == 0 {
				return nil, ErrNoAnswer
			}
			return buf, fmt.Errorf("W100 answer incomplete: %d bytes", len(buf))
		}
		n, err := p.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil && !errors.Is(err, errReadIdle) {
			return buf, fmt.Errorf("W100 read: %w", err)
		}
		if len(buf) > 255 {
			return buf, errors.New("W100 answer too long")
		}
	}
}

// errReadIdle lets a port report "nothing yet" without being an error.
var errReadIdle = errors.New("idle")
