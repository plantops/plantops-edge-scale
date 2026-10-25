// Package fleet simulates a weighbridge site end to end: a fleet of trucks that
// obey the lamps, a Laumas indicator, the remote I/O module, RFID and plate
// webhooks, a dispatcher that CALLs trucks back, and a mock Central. It drives
// the real edge binary over its real interfaces, so every adapter and the whole
// two-pass workflow are exercised without hardware.
package fleet

import (
	"fmt"
	"strings"
	"time"
)

// Config holds every knob of a run. All "S" durations are SIMULATED seconds;
// they are divided by Speed to get real time (with a floor so the edge's
// 50-100 ms polling can always observe a sensor change).
type Config struct {
	EdgeURL string // base URL of the running edge service
	Speed   float64
	Seed    int64

	Fleet        int     // number of distinct trucks
	TurnsPerDay  float64 // site load; arrival rate = TurnsPerDay / (WorkHours*3600) per sim second
	WorkHours    float64 // length of the working day the load is spread over (16 = 06:00-22:00)
	MaxTurns     int     // stop starting new turns after this many (0 = until the context ends)
	InboundShare float64 // share of turns that are MATERIAL_IN (gross first); the rest are MATERIAL_OUT

	// Trip between the first weighing and being ready at the store for the return.
	TripMin, TripMax time.Duration // sim time

	TareMeanKG, TareStdKG      int64
	PayloadMinKG, PayloadMaxKG int64
	ResidualMaxKG              int64   // goods left on the truck at its "empty" weighing (accepted tare remainder)
	ResidualShare              float64 // share of passes that carry a remainder

	// Edge polling intervals (must match the edge flags) set the minimum hold
	// time for sensor changes.
	IOPoll    time.Duration
	ScalePoll time.Duration

	// Pass choreography, simulated seconds.
	ApproachS, IdentityS, DriveOnS, SettleS, LeaveS float64

	// Fault injection, probability per pass.
	RFIDMissRate   float64 // tag not read on first try, read on retry
	LPRMisreadRate float64 // wrong plate first, correct plate on re-read
	ScaleDropRate  float64 // indicator swallows a poll

	StepTimeout time.Duration // real time to wait for the edge to react (lamp, release)
}

func (c Config) withDefaults() Config {
	def := func(v *float64, d float64) {
		if *v <= 0 {
			*v = d
		}
	}
	defI := func(v *int64, d int64) {
		if *v <= 0 {
			*v = d
		}
	}
	defD := func(v *time.Duration, d time.Duration) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&c.Speed, 1)
	if c.Seed == 0 {
		c.Seed = 1
	}
	if c.Fleet <= 0 {
		c.Fleet = 16
	}
	def(&c.TurnsPerDay, 450)
	def(&c.WorkHours, 16)
	if c.InboundShare <= 0 || c.InboundShare >= 1 {
		c.InboundShare = 0.6
	}
	defD(&c.TripMin, 6*time.Minute)
	defD(&c.TripMax, 25*time.Minute)
	defI(&c.TareMeanKG, 9000)
	defI(&c.TareStdKG, 500)
	defI(&c.PayloadMinKG, 14000)
	defI(&c.PayloadMaxKG, 28000)
	defI(&c.ResidualMaxKG, 300)
	def(&c.ResidualShare, 0.3)
	defD(&c.IOPoll, 100*time.Millisecond)
	defD(&c.ScalePoll, 100*time.Millisecond)
	def(&c.ApproachS, 4)
	def(&c.IdentityS, 2)
	def(&c.DriveOnS, 5)
	def(&c.SettleS, 3)
	def(&c.LeaveS, 6)
	defD(&c.StepTimeout, 20*time.Second)
	return c
}

// scaled turns simulated seconds into real time, never below the polling floor.
func (c Config) scaled(simSeconds float64) time.Duration {
	d := time.Duration(simSeconds / c.Speed * float64(time.Second))
	if floor := c.minHold(); d < floor {
		return floor
	}
	return d
}

// minHold is how long a sensor or weight must stay put so that three polls of
// the slower channel can see it.
func (c Config) minHold() time.Duration {
	p := c.IOPoll
	if c.ScalePoll > p {
		p = c.ScalePoll
	}
	return 3 * p
}

func (c Config) simDuration(real time.Duration) time.Duration {
	return time.Duration(float64(real) * c.Speed)
}

func fmtSim(d time.Duration) string {
	d = d.Round(time.Second)
	h, m, s := int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	return fmt.Sprintf("%dm%02ds", m, s)
}

func joinPairs(pairs []string) string { return strings.Join(pairs, ",") }
