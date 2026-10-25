package fleet

import (
	"fmt"
	"math/rand"
	"strings"
)

type Flow string

const (
	TaskIn  Flow = "MATERIAL_IN"  // loaded A->B (gross first), empty back
	TaskOut Flow = "MATERIAL_OUT" // empty A->B (tare first), loaded back
)

type Phase int

const (
	PhaseIdle Phase = iota
	PhaseWaitingA
	PhasePass1
	PhaseInLoop
	PhaseReadyAtStore
	PhasePass2
)

func (p Phase) String() string {
	return [...]string{"idle", "waiting-A", "pass-1", "in-loop", "ready-at-store", "pass-2"}[p]
}

// Truck is one vehicle of the fleet. Its weights are exact integers, so the
// ticket the edge produces can be checked against the truth.
type Truck struct {
	ID     int
	Plate  string
	RFID   string
	TareKG int64

	// Current turn.
	Phase     Phase
	Task      Flow
	PayloadKG int64
	Residual1 int64   // goods still aboard at the empty weighing
	Cycle     string  // edge cycle id once known
	Arrived   float64 // sim seconds since run start
	Ready     float64
	Called    bool
	Turns     int
}

// WeightKG is what the scale shows with this truck on it for pass 1 or 2.
func (t *Truck) WeightKG(pass int) int64 {
	switch {
	case t.Task == TaskIn && pass == 1:
		return t.TareKG + t.PayloadKG
	case t.Task == TaskIn && pass == 2:
		return t.TareKG + t.Residual1
	case t.Task == TaskOut && pass == 1:
		return t.TareKG + t.Residual1
	default:
		return t.TareKG + t.PayloadKG
	}
}

// ExpectedNetKG is the truth the ticket must reproduce.
func (t *Truck) ExpectedNetKG() int64 {
	a, b := t.WeightKG(1), t.WeightKG(2)
	if a > b {
		return a - b
	}
	return b - a
}

func newFleet(n int, rng *rand.Rand, cfg Config) []*Truck {
	out := make([]*Truck, 0, n)
	used := map[string]bool{}
	for i := 0; i < n; i++ {
		var plate, tag string
		for {
			plate = fmt.Sprintf("%d%c-%03d.%02d", []int{61, 51, 60, 50, 43}[rng.Intn(5)], "CDHK"[rng.Intn(4)], 100+rng.Intn(900), 10+rng.Intn(90))
			if !used[plate] {
				used[plate] = true
				break
			}
		}
		tag = fmt.Sprintf("E200%08X", rng.Uint32())
		tare := cfg.TareMeanKG + int64(rng.NormFloat64()*float64(cfg.TareStdKG))
		if tare < 5000 {
			tare = 5000
		}
		out = append(out, &Truck{ID: i + 1, Plate: plate, RFID: tag, TareKG: (tare / 10) * 10})
	}
	return out
}

// VehicleMap is the value for the edge flag -vehicle-map (RFID=PLATE pairs).
func VehicleMap(trucks []*Truck) string {
	p := make([]string, 0, len(trucks))
	for _, t := range trucks {
		p = append(p, t.RFID+"="+t.Plate)
	}
	return joinPairs(p)
}

// misread returns the plate with one character replaced, as a bad LPR frame would.
func misread(plate string, rng *rand.Rand) string {
	r := []rune(plate)
	for tries := 0; tries < 20; tries++ {
		i := rng.Intn(len(r))
		if r[i] >= '0' && r[i] <= '9' {
			r[i] = '0' + (r[i]-'0'+1+rune(rng.Intn(8)))%10
			return string(r)
		}
	}
	return strings.ToLower(plate) + "?"
}
