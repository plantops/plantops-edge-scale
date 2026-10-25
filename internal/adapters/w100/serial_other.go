//go:build !windows && !linux

package w100

import "errors"

func openSerial(string, LineSettings) (Port, error) {
	return nil, errors.New("serial ports are supported on Windows and Linux; use tcp:host:port through an RS485 gateway")
}
