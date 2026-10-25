//go:build windows

package laumasrtu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

// The Win32 comm API is called directly through syscall so the single-EXE build
// keeps zero third-party dependencies and CGO stays off.
//
// The DCB set-up mirrors what pyserial applies on Windows, which is the setup
// that was proven against the real indicator: binary mode, DTR and RTS
// asserted, no flow control, no software handshaking.
var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procGetCommState = kernel32.NewProc("GetCommState")
	procSetCommState = kernel32.NewProc("SetCommState")
	procSetCommTO    = kernel32.NewProc("SetCommTimeouts")
	procSetupComm    = kernel32.NewProc("SetupComm")
	procPurgeComm    = kernel32.NewProc("PurgeComm")
)

type dcb struct {
	DCBlength  uint32
	BaudRate   uint32
	Flags      uint32
	wReserved  uint16
	XonLim     uint16
	XoffLim    uint16
	ByteSize   byte
	Parity     byte
	StopBits   byte
	XonChar    int8
	XoffChar   int8
	ErrorChar  int8
	EofChar    int8
	EvtChar    int8
	wReserved1 uint16
}

type commTimeouts struct {
	ReadIntervalTimeout         uint32
	ReadTotalTimeoutMultiplier  uint32
	ReadTotalTimeoutConstant    uint32
	WriteTotalTimeoutMultiplier uint32
	WriteTotalTimeoutConstant   uint32
}

const (
	dcbBinary    = 1 << 0
	dcbParity    = 1 << 1
	dcbDtrEnable = 1 << 4  // fDtrControl = DTR_CONTROL_ENABLE
	dcbRtsEnable = 1 << 12 // fRtsControl = RTS_CONTROL_ENABLE
	purgeTxClear = 0x0004
	purgeRxClear = 0x0008
	noParity     = 0
	oddParity    = 1
	evenParity   = 2
	oneStopBit   = 0
	twoStopBits  = 2
)

// SerialDialer opens a native COM port.
func SerialDialer(cfg SerialConfig) Dialer {
	return func(context.Context) (Conn, error) { return openSerial(cfg) }
}

type serialConn struct {
	h        syscall.Handle
	deadline time.Time
}

func openSerial(cfg SerialConfig) (*serialConn, error) {
	port := strings.TrimSpace(cfg.Port)
	if port == "" {
		return nil, errors.New("laumasrtu: serial port name is empty")
	}
	path := port
	if !strings.HasPrefix(path, `\.\`) {
		path = `\.\` + path // required for COM10 and above, harmless below
	}
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, fmt.Errorf("laumasrtu: open %s: %w", port, err)
	}
	c := &serialConn{h: h}
	if err := c.configure(cfg); err != nil {
		syscall.CloseHandle(h)
		return nil, fmt.Errorf("laumasrtu: configure %s: %w", port, err)
	}
	return c, nil
}

func (c *serialConn) configure(cfg SerialConfig) error {
	_, _, _ = procSetupComm.Call(uintptr(c.h), 4096, 4096)
	var d dcb
	d.DCBlength = uint32(unsafe.Sizeof(d))
	if r, _, e := procGetCommState.Call(uintptr(c.h), uintptr(unsafe.Pointer(&d))); r == 0 {
		return fmt.Errorf("GetCommState: %w", e)
	}
	d.BaudRate = uint32(cfg.baud())
	d.ByteSize = 8
	d.Flags = dcbBinary | dcbDtrEnable | dcbRtsEnable
	switch cfg.parity() {
	case 'E':
		d.Parity, d.Flags = evenParity, d.Flags|dcbParity
	case 'O':
		d.Parity, d.Flags = oddParity, d.Flags|dcbParity
	default:
		d.Parity = noParity
	}
	if cfg.stopBits() == 2 {
		d.StopBits = twoStopBits
	} else {
		d.StopBits = oneStopBit
	}
	if r, _, e := procSetCommState.Call(uintptr(c.h), uintptr(unsafe.Pointer(&d))); r == 0 {
		return fmt.Errorf("SetCommState: %w", e)
	}
	return c.setTimeouts(200)
}

func (c *serialConn) setTimeouts(readMS uint32) error {
	t := commTimeouts{ReadTotalTimeoutConstant: readMS, WriteTotalTimeoutConstant: 1000}
	if r, _, e := procSetCommTO.Call(uintptr(c.h), uintptr(unsafe.Pointer(&t))); r == 0 {
		return fmt.Errorf("SetCommTimeouts: %w", e)
	}
	return nil
}

// Read honours the deadline set with SetDeadline. ReadFile reports a timeout as
// success with zero bytes, which io.ReadFull would retry forever, so a
// zero-byte read is turned into os.ErrDeadlineExceeded here.
func (c *serialConn) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	ms := uint32(250)
	if !c.deadline.IsZero() {
		rem := time.Until(c.deadline)
		if rem <= 0 {
			return 0, os.ErrDeadlineExceeded
		}
		ms = uint32(rem/time.Millisecond) + 1
	}
	if err := c.setTimeouts(ms); err != nil {
		return 0, err
	}
	var n uint32
	if err := syscall.ReadFile(c.h, b, &n, nil); err != nil {
		return int(n), err
	}
	if n == 0 {
		return 0, os.ErrDeadlineExceeded
	}
	return int(n), nil
}

func (c *serialConn) Write(b []byte) (int, error) {
	var n uint32
	err := syscall.WriteFile(c.h, b, &n, nil)
	return int(n), err
}

func (c *serialConn) SetDeadline(t time.Time) error { c.deadline = t; return nil }

// Purge discards stale bytes in both directions.
func (c *serialConn) Purge() error {
	if r, _, e := procPurgeComm.Call(uintptr(c.h), purgeRxClear|purgeTxClear); r == 0 {
		return e
	}
	return nil
}

func (c *serialConn) Close() error { return syscall.CloseHandle(c.h) }
