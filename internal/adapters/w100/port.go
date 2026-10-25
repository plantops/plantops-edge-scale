package w100

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
)

// LineSettings must match the W100 serial menu (baud, parity). Data bits are always 8.
type LineSettings struct {
	Baud     int
	Parity   byte // 'N', 'E' or 'O'
	StopBits int  // 1 or 2
}

func (s LineSettings) String() string { return fmt.Sprintf("%d 8%c%d", s.Baud, s.Parity, s.StopBits) }

// Validate checks values the W100 offers (manual: 2400..115200, none/even/odd).
func (s LineSettings) Validate() error {
	switch s.Baud {
	case 2400, 4800, 9600, 19200, 38400, 115200:
	default:
		return fmt.Errorf("baud %d not offered by the W100", s.Baud)
	}
	if s.Parity != 'N' && s.Parity != 'E' && s.Parity != 'O' {
		return fmt.Errorf("parity %q: use N, E or O", s.Parity)
	}
	if s.StopBits != 1 && s.StopBits != 2 {
		return fmt.Errorf("stop bits %d: use 1 or 2", s.StopBits)
	}
	return nil
}

// Open opens "COM5", "/dev/ttyUSB0" or "tcp:host:port" (RS485-Ethernet gateway, transparent RTU).
func Open(name string, s LineSettings) (Port, error) {
	if addr, ok := strings.CutPrefix(name, "tcp:"); ok {
		c, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			return nil, err
		}
		return &tcpPort{c: c}, nil
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return openSerial(name, s)
}

const readSlice = 20 * time.Millisecond

type tcpPort struct{ c net.Conn }

func (t *tcpPort) Read(b []byte) (int, error) {
	_ = t.c.SetReadDeadline(time.Now().Add(readSlice))
	n, err := t.c.Read(b)
	if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
		return n, errReadIdle
	}
	return n, err
}
func (t *tcpPort) Write(b []byte) (int, error) {
	_ = t.c.SetWriteDeadline(time.Now().Add(time.Second))
	return t.c.Write(b)
}
func (t *tcpPort) Close() error { return t.c.Close() }
func (t *tcpPort) Flush() error {
	buf := make([]byte, 256)
	for {
		_ = t.c.SetReadDeadline(time.Now().Add(time.Millisecond))
		n, err := t.c.Read(buf)
		if n == 0 || err != nil {
			return nil
		}
	}
}
