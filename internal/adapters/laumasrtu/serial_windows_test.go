//go:build windows

package laumasrtu

import (
	"context"
	"testing"
)

// There is no COM port on a CI box, so this only proves that a missing port is
// reported as an error (not a panic or a hang) and that the Win32 plumbing links.
func TestSerialDialerReportsMissingPort(t *testing.T) {
	conn, err := SerialDialer(SerialConfig{Port: "COM254", Baud: 9600})(context.Background())
	if err == nil {
		conn.Close()
		t.Skip("COM254 unexpectedly exists on this machine")
	}
}

func TestSerialConfigDefaults(t *testing.T) {
	c := SerialConfig{}
	if c.baud() != 9600 || c.parity() != 'N' || c.stopBits() != 1 {
		t.Fatalf("defaults must be 9600 8N1: %d %c %d", c.baud(), c.parity(), c.stopBits())
	}
	if (SerialConfig{Parity: 'e', StopBits: 2}).parity() != 'E' || (SerialConfig{StopBits: 2}).stopBits() != 2 {
		t.Fatal("parity/stop-bit normalisation")
	}
}
