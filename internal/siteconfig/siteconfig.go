// Package siteconfig describes which hardware each component of the site really
// is. Every component is configured on its own: the scale can be a real Laumas
// indicator while the I/O module, RFID reader, plate camera and cameras are
// simulated, or the other way round.
//
// A component whose adapter is "fake" is SIMULATED. The resulting Profile is
// published at /api/hardware, written to the audit journal at start-up and shown
// in the operator UI, and tickets produced under a profile with simulated
// components are not synced to Central unless that is explicitly allowed. A
// bench ticket must never be mistaken for a production one.
package siteconfig

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
)

type Component string

const (
	CompScale   Component = "scale"
	CompIO      Component = "io"
	CompRFID    Component = "rfid"
	CompLPR     Component = "lpr"
	CompCameras Component = "cameras"
)

// Adapter values.
const (
	AdapterNone      = "none"       // component absent / disabled
	AdapterFake      = "fake"       // simulated in the edge process, driven from the bench panel
	AdapterASCII     = "ascii"      // scale: TCP line stream (placeholder protocol)
	AdapterLaumasRTU = "laumas-rtu" // scale: Laumas W100/W200 Modbus RTU
	AdapterModbusTCP = "modbustcp"  // io: remote I/O module over Modbus TCP
	AdapterWebhook   = "webhook"    // rfid/lpr/cameras: the real device POSTs to /io/...
)

var allowed = map[Component][]string{
	CompScale:   {AdapterNone, AdapterASCII, AdapterLaumasRTU, AdapterFake},
	CompIO:      {AdapterNone, AdapterModbusTCP, AdapterFake},
	CompRFID:    {AdapterWebhook, AdapterFake},
	CompLPR:     {AdapterWebhook, AdapterFake},
	CompCameras: {AdapterWebhook, AdapterFake},
}

// Allowed returns the adapter names valid for a component.
func Allowed(c Component) []string { return append([]string(nil), allowed[c]...) }

// Check validates one adapter name.
func Check(c Component, adapter string) error {
	for _, a := range allowed[c] {
		if a == adapter {
			return nil
		}
	}
	return fmt.Errorf("%s adapter %q is not valid; use one of: %s", c, adapter, strings.Join(allowed[c], ", "))
}

// File is the JSON site configuration. Command-line flags override it.
type File struct {
	StationID string `json:"station_id,omitempty"`
	Listen    string `json:"listen,omitempty"`

	Storage struct {
		DB               string `json:"db,omitempty"`
		RawWeightJournal string `json:"raw_weight_journal,omitempty"`
		EventJournal     string `json:"event_journal,omitempty"`
	} `json:"storage"`

	Scale struct {
		Adapter           string `json:"adapter,omitempty"` // none | ascii | laumas-rtu | fake
		Addr              string `json:"addr,omitempty"`    // TCP address (ascii, or laumas-rtu through a gateway)
		Serial            string `json:"serial,omitempty"`  // laumas-rtu: COM port
		Baud              int    `json:"baud,omitempty"`
		Parity            string `json:"parity,omitempty"`
		StopBits          int    `json:"stop_bits,omitempty"`
		Slave             uint   `json:"slave,omitempty"`
		PollMS            int    `json:"poll_ms,omitempty"`
		ResponseTimeoutMS int    `json:"response_timeout_ms,omitempty"`
		MissedPolls       int    `json:"missed_polls,omitempty"`
		StableBit         *int   `json:"stable_bit,omitempty"`
		OverloadMask      uint   `json:"overload_mask,omitempty"`
		FaultMask         uint   `json:"fault_mask,omitempty"`
		SettleMS          int    `json:"settle_ms,omitempty"` // fake: time after a weight change before STABLE
	} `json:"scale"`

	IO struct {
		Adapter          string `json:"adapter,omitempty"` // none | modbustcp | fake
		Addr             string `json:"addr,omitempty"`
		UnitID           uint   `json:"unit_id,omitempty"`
		Map              string `json:"map,omitempty"`
		PollMS           int    `json:"poll_ms,omitempty"`
		TimeoutMS        int    `json:"timeout_ms,omitempty"`
		BarrierDelayMS   int    `json:"barrier_delay_ms,omitempty"` // fake: time a barrier takes to move
		SafetyClearFixed *bool  `json:"safety_clear,omitempty"`     // fake: initial physical-safety state
	} `json:"io"`

	RFID struct {
		Adapter string `json:"adapter,omitempty"`
	} `json:"rfid"`
	LPR struct {
		Adapter string `json:"adapter,omitempty"`
	} `json:"lpr"`
	Cameras struct {
		Adapter string   `json:"adapter,omitempty"`
		IDs     []string `json:"ids,omitempty"`
	} `json:"cameras"`

	// Vehicles maps RFID tag -> plate (the vehicle registry).
	Vehicles map[string]string `json:"vehicles,omitempty"`

	Central struct {
		TicketURL      string `json:"ticket_url,omitempty"`
		HeartbeatURL   string `json:"heartbeat_url,omitempty"`
		Token          string `json:"token,omitempty"`
		AllowSimulated bool   `json:"allow_simulated,omitempty"`
	} `json:"central"`
}

// Load reads and strictly parses a site configuration file: an unknown key is an
// error, so a misspelt component setting cannot silently fall back to a default.
func Load(path string) (File, error) {
	var f File
	b, err := os.ReadFile(path)
	if err != nil {
		return f, err
	}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return f, fmt.Errorf("%s: %w", path, err)
	}
	return f, nil
}

// Entry is one line of the hardware profile.
type Entry struct {
	Component Component `json:"component"`
	Adapter   string    `json:"adapter"`
	Simulated bool      `json:"simulated"`
	Detail    string    `json:"detail,omitempty"`
}

// Profile is the resolved hardware configuration of a running edge.
type Profile struct {
	Entries []Entry `json:"components"`
}

// Set adds a component. The adapter is validated by the caller (Check).
func (p *Profile) Set(c Component, adapter, detail string) {
	p.Entries = append(p.Entries, Entry{Component: c, Adapter: adapter, Simulated: adapter == AdapterFake, Detail: detail})
}

// Simulated lists the components that are not real hardware.
func (p Profile) Simulated() []string {
	var out []string
	for _, e := range p.Entries {
		if e.Simulated {
			out = append(out, string(e.Component))
		}
	}
	sort.Strings(out)
	return out
}

// Mode is LIVE (nothing simulated), SIMULATED (everything present is simulated)
// or MIXED.
func (p Profile) Mode() string {
	sim, real := 0, 0
	for _, e := range p.Entries {
		switch {
		case e.Simulated:
			sim++
		case e.Adapter != AdapterNone:
			real++
		}
	}
	switch {
	case sim == 0:
		return "LIVE"
	case real == 0:
		return "SIMULATED"
	default:
		return "MIXED"
	}
}

// View is the JSON served at /api/hardware.
func (p Profile) View() map[string]any {
	return map[string]any{
		"mode":                 p.Mode(),
		"simulated_components": p.Simulated(),
		"components":           p.Entries,
	}
}
