//go:build !windows

package laumasrtu

import (
	"context"
	"errors"
)

// SerialDialer is only implemented on Windows (the deployment target). On other
// platforms point the collector at a serial-to-Ethernet gateway with TCPDialer.
func SerialDialer(SerialConfig) Dialer {
	return func(context.Context) (Conn, error) {
		return nil, errors.New("laumasrtu: native serial is implemented for Windows only; use a serial-to-Ethernet gateway (TCP)")
	}
}
