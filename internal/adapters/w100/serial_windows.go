//go:build windows

package w100

import (
	"fmt"
	"strings"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows COM port through the Win32 comm API; no CGO, no driver beyond the
// dongle's own (CH340, FTDI, CP210x).
type winPort struct{ h windows.Handle }

const (
	dcbBinary    = 0x0001 // fBinary must be TRUE on Windows
	dcbParity    = 0x0002 // fParity: check parity on input
	purgeTxClear = 0x0004
	purgeRxClear = 0x0008
)

func openSerial(name string, s LineSettings) (Port, error) {
	path := name
	if !strings.HasPrefix(path, `\\.\`) {
		path = `\\.\` + name // needed for COM10 and above
	}
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ|windows.GENERIC_WRITE, 0, nil, windows.OPEN_EXISTING, 0, 0)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", name, err)
	}
	var dcb windows.DCB
	dcb.DCBlength = uint32(unsafe.Sizeof(dcb))
	if err := windows.GetCommState(h, &dcb); err != nil {
		windows.CloseHandle(h)
		return nil, fmt.Errorf("%s GetCommState: %w", name, err)
	}
	dcb.BaudRate = uint32(s.Baud)
	dcb.ByteSize = 8
	dcb.Flags = dcbBinary // no flow control, DTR/RTS off; the dongle drives RS485 direction itself
	switch s.Parity {
	case 'E':
		dcb.Parity, dcb.Flags = 2, dcb.Flags|dcbParity
	case 'O':
		dcb.Parity, dcb.Flags = 1, dcb.Flags|dcbParity
	default:
		dcb.Parity = 0
	}
	if s.StopBits == 2 {
		dcb.StopBits = 2
	} else {
		dcb.StopBits = 0
	}
	if err := windows.SetCommState(h, &dcb); err != nil {
		windows.CloseHandle(h)
		return nil, fmt.Errorf("%s SetCommState %s: %w", name, s, err)
	}
	// Return within readSlice with whatever has arrived.
	to := windows.CommTimeouts{ReadIntervalTimeout: 0xFFFFFFFF, ReadTotalTimeoutMultiplier: 0xFFFFFFFF,
		ReadTotalTimeoutConstant: uint32(readSlice.Milliseconds()), WriteTotalTimeoutConstant: 1000}
	if err := windows.SetCommTimeouts(h, &to); err != nil {
		windows.CloseHandle(h)
		return nil, fmt.Errorf("%s SetCommTimeouts: %w", name, err)
	}
	return &winPort{h: h}, nil
}

func (w *winPort) Read(b []byte) (int, error) {
	var n uint32
	if err := windows.ReadFile(w.h, b, &n, nil); err != nil {
		return int(n), err
	}
	if n == 0 {
		return 0, errReadIdle
	}
	return int(n), nil
}

func (w *winPort) Write(b []byte) (int, error) {
	var n uint32
	err := windows.WriteFile(w.h, b, &n, nil)
	return int(n), err
}

func (w *winPort) Close() error { return windows.CloseHandle(w.h) }
func (w *winPort) Flush() error { return windows.PurgeComm(w.h, purgeRxClear|purgeTxClear) }
