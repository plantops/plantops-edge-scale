package w100

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

// RawWeightRecordJournal is the raw weight journal (same contract as the ASCII stream).
type RawWeightRecordJournal interface {
	AppendRecord(context.Context, domain.RawWeightEvent) (domain.RawWeightRef, error)
}

// Collector polls the W100 status block and hands audited readings to the workflow.
//
// Audit ordering is the same as the ASCII stream collector:
//
//	W100 answer bytes -> durable raw journal -> OnReading
//
// A reading that was not journaled is never delivered. To keep the journal a
// sane size while the deck stands still, an answer byte-identical to the last
// journaled one is journaled (and delivered) at most once per RepeatEvery.
// Any change, however small, is journaled and delivered at once. RepeatEvery 0
// journals every poll.
type Collector struct {
	Port           string // COM5, /dev/ttyUSB0 or tcp:host:port
	Line           LineSettings
	Unit           byte // W100 Modbus address 1..99
	Poll           time.Duration
	Timeout        time.Duration
	RepeatEvery    time.Duration
	ReconnectDelay time.Duration
	StationID      string
	TransactionID  func() string
	Journal        RawWeightRecordJournal
	OnReading      func(domain.AuditedScaleReading)
	OnFault        func(error)

	open func(string, LineSettings) (Port, error) // tests
}

// Source names the line in every journal record.
func (c *Collector) Source() string {
	if len(c.Port) > 4 && c.Port[:4] == "tcp:" {
		return fmt.Sprintf("w100 %s unit %d", c.Port, c.Unit)
	}
	return fmt.Sprintf("w100 %s %s unit %d", c.Port, c.Line, c.Unit)
}

// Run polls until ctx ends. It returns an error only when the journal fails:
// an unaudited weight must stop the collector, not slip through.
func (c *Collector) Run(ctx context.Context) error {
	if c.Port == "" {
		return errors.New("W100 port is empty")
	}
	if c.Journal == nil {
		return errors.New("raw weight journal is required")
	}
	open := c.open
	if open == nil {
		open = Open
	}
	for ctx.Err() == nil {
		p, err := open(c.Port, c.Line)
		if err != nil {
			if jerr := c.transportError(ctx, err); jerr != nil {
				return jerr
			}
			if !sleep(ctx, c.reconnect()) {
				return nil
			}
			continue
		}
		err = c.session(ctx, p)
		_ = p.Close()
		if err != nil {
			return err
		}
		if !sleep(ctx, c.reconnect()) {
			return nil
		}
	}
	return nil
}

// session polls one open port; it returns nil to reopen, or a journal error.
func (c *Collector) session(ctx context.Context, p Port) error {
	req := ReadRequest(c.unit(), RegStatus, StatusBlockLen)
	tick := time.NewTicker(c.poll())
	defer tick.Stop()
	var last []byte
	var lastAt time.Time
	misses := 0
	for {
		raw, err := Transact(p, req, c.timeout())
		at := time.Now().UTC()
		var b Block
		perr := err
		if len(raw) > 0 {
			var regs []uint16
			regs, perr = ParseReadResponse(c.unit(), StatusBlockLen, raw)
			if perr == nil && err != nil {
				perr = err
			}
			if perr == nil {
				b, perr = Decode(regs)
			}
			repeat := perr == nil && c.RepeatEvery > 0 && bytes.Equal(raw, last) && at.Sub(lastAt) < c.RepeatEvery
			if !repeat {
				ref, jerr := c.Journal.AppendRecord(ctx, c.frameEvent(at, raw, b, perr))
				if jerr != nil {
					return fmt.Errorf("raw weight audit append: %w", jerr)
				}
				if perr == nil {
					last, lastAt = append(last[:0], raw...), at
					if c.OnReading != nil {
						c.OnReading(domain.AuditedScaleReading{Reading: b.reading(at), RawRef: ref})
					}
				} else {
					last = nil
				}
			}
		}
		if perr == nil {
			misses = 0
		} else if misses++; misses >= 3 {
			// One lost or garbled answer is normal RS485 life (each garbled one is
			// journaled above); three in a row is a lost line: fault and reopen.
			return c.transportError(ctx, perr)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

func (b Block) reading(at time.Time) domain.ScaleReading {
	r := domain.ScaleReading{WeightKG: b.GrossKG, Stable: b.Stable, Overload: b.Overload, Fault: b.Fault,
		Health: domain.HealthHealthy, Observed: at}
	if b.Fault != "" {
		r.Health = domain.HealthFault
	}
	return r
}

func (c *Collector) frameEvent(at time.Time, raw []byte, b Block, perr error) domain.RawWeightEvent {
	e := domain.RawWeightEvent{StationID: c.StationID, TransactionID: c.txID(), Kind: domain.RawWeightFrame,
		ReceivedAtUTC: at, Source: c.Source(), RawBase64: base64.StdEncoding.EncodeToString(raw),
		RawText: hex.EncodeToString(raw), ParseOK: perr == nil}
	if perr != nil {
		e.Health, e.Error = domain.HealthFault, perr.Error()
		return e
	}
	r := b.reading(at)
	w, s, o := r.WeightKG, r.Stable, r.Overload
	e.WeightKG, e.Stable, e.Overload, e.Fault, e.Health = &w, &s, &o, r.Fault, r.Health
	e.RawText += " | " + b.Text()
	return e
}

func (c *Collector) transportError(ctx context.Context, err error) error {
	if _, jerr := c.Journal.AppendRecord(ctx, domain.RawWeightEvent{StationID: c.StationID, TransactionID: c.txID(),
		Kind: domain.RawWeightTransportError, ReceivedAtUTC: time.Now().UTC(), Source: c.Source(),
		Health: domain.HealthDisconnected, Error: err.Error()}); jerr != nil {
		return fmt.Errorf("W100: %v; raw weight audit append: %w", err, jerr)
	}
	c.fault(err)
	return nil
}

func (c *Collector) fault(err error) {
	if c.OnFault != nil && err != nil {
		c.OnFault(err)
	}
}

func (c *Collector) txID() string {
	if c.TransactionID == nil {
		return ""
	}
	return c.TransactionID()
}

func (c *Collector) unit() byte {
	if c.Unit == 0 {
		return 1
	}
	return c.Unit
}

func (c *Collector) poll() time.Duration    { return orDefault(c.Poll, 200*time.Millisecond) }
func (c *Collector) timeout() time.Duration { return orDefault(c.Timeout, 300*time.Millisecond) }
func (c *Collector) reconnect() time.Duration {
	return orDefault(c.ReconnectDelay, 2*time.Second)
}

func orDefault(d, def time.Duration) time.Duration {
	if d <= 0 {
		return def
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
