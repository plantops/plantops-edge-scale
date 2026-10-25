package laumasrtu

// SerialConfig describes a local COM port. The verified site setting is
// 9600 baud, 8 data bits, no parity, 1 stop bit (8N1), slave address 1.
type SerialConfig struct {
	Port     string // e.g. "COM3"
	Baud     int    // default 9600
	Parity   byte   // 'N' (default), 'E' or 'O'
	StopBits int    // 1 (default) or 2
}

func (c SerialConfig) baud() int {
	if c.Baud <= 0 {
		return 9600
	}
	return c.Baud
}

func (c SerialConfig) parity() byte {
	switch c.Parity {
	case 'E', 'e':
		return 'E'
	case 'O', 'o':
		return 'O'
	default:
		return 'N'
	}
}

func (c SerialConfig) stopBits() int {
	if c.StopBits == 2 {
		return 2
	}
	return 1
}
