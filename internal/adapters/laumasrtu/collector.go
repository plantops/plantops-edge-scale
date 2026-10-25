package laumasrtu

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

// Conn is the byte pipe to the indicator: a COM port, or a TCP connection to a
// transparent serial gateway. net.Conn satisfies it.
type Conn interface {
	io.ReadWriteCloser
	SetDeadline(time.Time) error
}

// Purger is optionally implemented by a Conn that can discard stale receive
// bytes (a serial port). It is called before every poll so a late reply to a
// previous poll can never be mistaken for the answer to the next one.
type Purger interface {
	Purge() error
}

type Dialer func(ctx context.Context) (Conn, error)

// TCPDialer connects to a serial-to-Ethernet gateway in transparent / raw mode.
func TCPDialer(addr string, timeout time.Duration) Dialer {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return func(ctx context.Context) (Conn, error) {
		d := net.Dialer{Timeout: timeout}
		c, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return nil, err
		}
		return c, nil
	}
}

type RawWeightRecordJournal interface {
	AppendRecord(context.Context, domain.RawWeightEvent) (domain.RawWeightRef, error)
}

// ErrIndicatorSilent is reported through OnFault once the indicator has failed
// to answer MaxMissedPolls polls in a row.
var ErrIndicatorSilent = errors.New("laumasrtu: indicator is not answering")

// Collector polls the indicator and feeds the workflow. It mirrors the audit
// ordering of scaleascii.StreamCollector:
//
//	controller bytes -> durable raw journal -> AuditedScaleReading callback
//
// Rules it keeps:
//   - a reading exists only for a CRC-valid frame of exactly the expected shape;
//   - a missed or damaged poll is journaled but NEVER replaced by a synthetic or
//     held-over reading (no invented "stable" value);
//   - isolated misses are tolerated (RS485 turn-around jitter) and only a run of
//     MaxMissedPolls consecutive failures is escalated through OnFault;
//   - if the raw journal cannot be written, Run stops with an error.
type Collector struct {
	Source string // audit label, e.g. "COM3@9600" or "192.168.10.20:4001"
	Dial   Dialer
	Slave  byte

	PollInterval    time.Duration // default 100 ms
	ResponseTimeout time.Duration // default 300 ms
	ReconnectDelay  time.Duration // default 2 s
	MaxMissedPolls  int           // default 3

	StationID     string
	TransactionID func() string
	Journal       RawWeightRecordJournal
	Decode        DecodeConfig

	OnReading func(domain.AuditedScaleReading)
	OnFault   func(error)

	// Now is injectable for tests.
	Now func() time.Time
}

func (c *Collector) Run(ctx context.Context) error {
	if c.Dial == nil {
		return errors.New("laumasrtu: dialer is required")
	}
	if c.Journal == nil {
		return errors.New("laumasrtu: raw weight journal is required")
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		conn, err := c.Dial(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if _, jerr := c.Journal.AppendRecord(ctx, c.transportEvent(c.now(), nil, nil, err)); jerr != nil {
				return fmt.Errorf("scale connect: %v; raw weight audit append: %w", err, jerr)
			}
			c.fault(err)
			if !sleepContext(ctx, c.reconnectDelay()) {
				return nil
			}
			continue
		}
		perr := c.poll(ctx, conn)
		_ = conn.Close()
		if perr != nil {
			return perr
		}
		if !sleepContext(ctx, c.reconnectDelay()) {
			return nil
		}
	}
}

// poll runs the request/response loop on one connection. It returns nil when
// the connection should be re-dialed and an error only for an audit failure.
func (c *Collector) poll(ctx context.Context, conn Conn) error {
	req := BuildReadRequest(c.Slave)
	missed := 0
	for {
		if ctx.Err() != nil {
			return nil
		}
		started := c.now()
		frame, ioErr := c.exchange(conn, req)
		received := c.now()

		if ioErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			missed++
			if _, err := c.Journal.AppendRecord(ctx, c.transportEvent(received, req, frame, ioErr)); err != nil {
				return fmt.Errorf("raw weight audit append: %w", err)
			}
			// A closed/reset connection cannot recover by polling again.
			if isDisconnect(ioErr) || missed >= c.maxMissed() {
				c.fault(fmt.Errorf("%w after %d poll(s): %v", ErrIndicatorSilent, missed, ioErr))
				return nil
			}
		} else {
			regs, perr := ParseReadResponse(c.Slave, frame)
			event := c.frameEvent(received, req, frame)
			if perr != nil {
				event.Health = domain.HealthFault
				event.Error = perr.Error()
				missed++
			} else {
				reading := c.Decode.Reading(regs, received)
				w, st, ov := reading.WeightKG, reading.Stable, reading.Overload
				event.WeightKG, event.Stable, event.Overload = &w, &st, &ov
				event.Fault, event.Health, event.ParseOK = reading.Fault, reading.Health, true
				ref, err := c.Journal.AppendRecord(ctx, event)
				if err != nil {
					return fmt.Errorf("raw weight audit append: %w", err)
				}
				missed = 0
				if c.OnReading != nil {
					c.OnReading(domain.AuditedScaleReading{Reading: reading, RawRef: ref})
				}
				goto next
			}
			if _, err := c.Journal.AppendRecord(ctx, event); err != nil {
				return fmt.Errorf("raw weight audit append: %w", err)
			}
			if missed >= c.maxMissed() {
				c.fault(fmt.Errorf("%w: %d damaged frame(s) in a row, last: %v", ErrIndicatorSilent, missed, perr))
				return nil
			}
		}
	next:
		if wait := c.pollInterval() - received.Sub(started); wait > 0 {
			if !sleepContext(ctx, wait) {
				return nil
			}
		}
	}
}

// exchange sends one request and reads one response frame. It returns whatever
// bytes did arrive together with the error so a partial frame can be audited.
func (c *Collector) exchange(conn Conn, req []byte) ([]byte, error) {
	if p, ok := conn.(Purger); ok {
		_ = p.Purge()
	}
	_ = conn.SetDeadline(c.now().Add(c.responseTimeout()))
	if _, err := conn.Write(req); err != nil {
		return nil, err
	}
	head := make([]byte, 3)
	if n, err := io.ReadFull(conn, head); err != nil {
		return head[:n], err
	}
	var rest int
	switch {
	case head[1]&0x80 != 0: // exception: one code byte already read, then CRC
		rest = 2
	case head[1] == FuncReadHoldingRegisters && int(head[2]) <= 250:
		rest = int(head[2]) + 2
	default:
		// Not a shape we can frame; hand the bytes to the parser to classify.
		return head, nil
	}
	body := make([]byte, rest)
	n, err := io.ReadFull(conn, body)
	return append(head, body[:n]...), err
}

func (c *Collector) frameEvent(at time.Time, req, resp []byte) domain.RawWeightEvent {
	return domain.RawWeightEvent{
		StationID: c.StationID, TransactionID: c.txID(),
		Kind: domain.RawWeightFrame, ReceivedAtUTC: at.UTC(), Source: c.Source,
		RawBase64: base64.StdEncoding.EncodeToString(resp),
		RawText:   exchangeText(req, resp),
		Health:    domain.HealthHealthy,
	}
}

func (c *Collector) transportEvent(at time.Time, req, partial []byte, err error) domain.RawWeightEvent {
	ev := domain.RawWeightEvent{
		StationID: c.StationID, TransactionID: c.txID(),
		Kind: domain.RawWeightTransportError, ReceivedAtUTC: at.UTC(), Source: c.Source,
		Health: domain.HealthDisconnected, ParseOK: false, Error: err.Error(),
	}
	if len(partial) > 0 {
		ev.RawBase64 = base64.StdEncoding.EncodeToString(partial)
		ev.RawText = exchangeText(req, partial)
	}
	return ev
}

func exchangeText(req, resp []byte) string {
	return fmt.Sprintf("REQ %s RESP %s", strings.ToUpper(fmt.Sprintf("%x", req)), strings.ToUpper(fmt.Sprintf("%x", resp)))
}

func isDisconnect(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed)
}

func (c *Collector) txID() string {
	if c.TransactionID == nil {
		return ""
	}
	return c.TransactionID()
}

func (c *Collector) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

func (c *Collector) fault(err error) {
	if c.OnFault != nil && err != nil {
		c.OnFault(err)
	}
}

func (c *Collector) pollInterval() time.Duration { return durOr(c.PollInterval, 100*time.Millisecond) }
func (c *Collector) responseTimeout() time.Duration {
	return durOr(c.ResponseTimeout, 300*time.Millisecond)
}
func (c *Collector) reconnectDelay() time.Duration { return durOr(c.ReconnectDelay, 2*time.Second) }
func (c *Collector) maxMissed() int {
	if c.MaxMissedPolls <= 0 {
		return 3
	}
	return c.MaxMissedPolls
}

func durOr(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
