//go:build linux

package w100

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// Linux tty (e.g. /dev/ttyUSB0) in raw mode, for a bench PC or a Pi.
type ttyPort struct{ fd int }

var bauds = map[int]uint32{2400: unix.B2400, 4800: unix.B4800, 9600: unix.B9600, 19200: unix.B19200,
	38400: unix.B38400, 115200: unix.B115200}

func openSerial(name string, s LineSettings) (Port, error) {
	fd, err := unix.Open(name, unix.O_RDWR|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	t := unix.Termios{}
	t.Cflag = unix.CREAD | unix.CLOCAL | unix.CS8 | bauds[s.Baud]
	t.Ispeed, t.Ospeed = bauds[s.Baud], bauds[s.Baud]
	switch s.Parity {
	case 'E':
		t.Cflag |= unix.PARENB
	case 'O':
		t.Cflag |= unix.PARENB | unix.PARODD
	}
	if s.StopBits == 2 {
		t.Cflag |= unix.CSTOPB
	}
	t.Cc[unix.VMIN] = 0
	t.Cc[unix.VTIME] = 1 // 100 ms, the finest the tty offers
	if err := unix.IoctlSetTermios(fd, unix.TCSETS, &t); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("%s set %s: %w", name, s, err)
	}
	return &ttyPort{fd: fd}, nil
}

func (p *ttyPort) Read(b []byte) (int, error) {
	n, err := unix.Read(p.fd, b)
	if n < 0 {
		n = 0
	}
	if err == nil && n == 0 {
		return 0, errReadIdle
	}
	return n, err
}
func (p *ttyPort) Write(b []byte) (int, error) { return unix.Write(p.fd, b) }
func (p *ttyPort) Close() error                { return unix.Close(p.fd) }
func (p *ttyPort) Flush() error                { return unix.IoctlSetInt(p.fd, unix.TCFLSH, unix.TCIFLUSH) }
