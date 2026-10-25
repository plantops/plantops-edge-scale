// W100 simulator: Modbus RTU over TCP (like an RS485-Ethernet gateway in
// transparent mode) on 127.0.0.1:19003, unit 1, kg, no decimals.
// Same two-pass timeline as scale-sim.go, counted from start.
//
//	go run ./.github/testdata/w100-sim.go
//	plantops-edge-scale -scale-driver w100 -scale-port tcp:127.0.0.1:19003 ...
package main

import (
	"log"
	"net"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/w100"
)

func main() {
	ln, err := net.Listen("tcp", "127.0.0.1:19003")
	if err != nil {
		log.Fatal(err)
	}
	sim := &w100.Sim{Unit: 1}
	sim.Set(w100.SimState{GrossKG: 0, Stable: true})
	log.Printf("W100 RTU simulator on %s, unit 1", ln.Addr())
	go func() { log.Fatal(sim.ListenAndServe(ln)) }()

	step := func(d time.Duration, s w100.SimState) { time.Sleep(d); sim.Set(s) }
	// PASS #1 gross 28,460 kg.
	step(5*time.Second, w100.SimState{GrossKG: 28300})
	step(200*time.Millisecond, w100.SimState{GrossKG: 28455, Stable: true})
	step(200*time.Millisecond, w100.SimState{GrossKG: 28460, Stable: true})
	// Deck empty again before the CALLED return.
	step(4*time.Second, w100.SimState{GrossKG: 0, Stable: true})
	// PASS #2 tare 11,820 kg.
	step(5*time.Second, w100.SimState{GrossKG: 11900})
	step(200*time.Millisecond, w100.SimState{GrossKG: 11825, Stable: true})
	step(200*time.Millisecond, w100.SimState{GrossKG: 11820, Stable: true})
	time.Sleep(30 * time.Second)
}
