package main

import (
	"strings"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/siteconfig"
)

func baseConfig() config {
	return config{scaleBaud: 9600, scaleParity: "N", scaleStopBits: 1, scaleSlave: 1, scalePoll: 100 * time.Millisecond,
		ioPoll: 100 * time.Millisecond, stationID: "EDGE-01", listen: "127.0.0.1:8080", cameraIDs: "C1A,C1B,C3"}
}

func TestBenchPresetRealScaleEverythingElseFake(t *testing.T) {
	f, err := siteconfig.Load("../../config/bench-real-scale.json")
	if err != nil {
		t.Fatal(err)
	}
	c := baseConfig()
	applySiteFile(&c, f, map[string]bool{})
	if err := resolveAdapters(&c); err != nil {
		t.Fatal(err)
	}
	p := hardwareProfile(c)
	if p.Mode() != "MIXED" || strings.Join(p.Simulated(), ",") != "cameras,io,lpr,rfid" {
		t.Fatalf("mode=%s simulated=%v", p.Mode(), p.Simulated())
	}
	if c.scaleProtocol != siteconfig.AdapterLaumasRTU || c.scaleSerial != "COM3" || c.scaleSlave != 1 {
		t.Fatalf("scale not real Laumas on COM3: %+v", c)
	}
	if r := realComponents(p); len(r) != 1 || r[0] != "scale" {
		t.Fatalf("only the scale is real, got %v", r)
	}
	if !strings.Contains(c.vehicleMap, "E2001111AAAA=61C-123.45") {
		t.Fatalf("vehicles not applied: %q", c.vehicleMap)
	}
	if got := centralBlockReason(p, false); !strings.Contains(got, "io") || centralBlockReason(p, true) != "" {
		t.Fatalf("central must be blocked for a mixed profile unless allowed: %q", got)
	}
}

func TestExplicitFlagsOverrideTheFile(t *testing.T) {
	f, err := siteconfig.Load("../../config/bench-real-scale.json")
	if err != nil {
		t.Fatal(err)
	}
	c := baseConfig()
	c.scaleSerial, c.scaleSlave, c.ioAdapter = "COM9", 5, "none" // as if given on the command line
	applySiteFile(&c, f, map[string]bool{"scale-serial": true, "scale-slave": true, "io-adapter": true})
	if c.scaleSerial != "COM9" || c.scaleSlave != 5 || c.ioAdapter != "none" {
		t.Fatalf("flags must win over the file: %+v", c)
	}
	if c.scaleProtocol != siteconfig.AdapterLaumasRTU {
		t.Fatal("settings the flags did not set must still come from the file")
	}
}

func TestResolveDefaultsKeepOldFlagBehaviour(t *testing.T) {
	c := baseConfig()
	c.rfidAdapter, c.lprAdapter, c.cameraAdapter = "webhook", "webhook", "webhook"
	if err := resolveAdapters(&c); err != nil || c.scaleProtocol != "none" || c.ioAdapter != "none" {
		t.Fatalf("no addresses means no scale and no I/O: %v %+v", err, c)
	}
	c = baseConfig()
	c.rfidAdapter, c.lprAdapter, c.cameraAdapter = "webhook", "webhook", "webhook"
	c.scaleAddr, c.ioAddr = "10.0.0.1:4001", "10.0.0.2:502"
	if err := resolveAdapters(&c); err != nil || c.scaleProtocol != "ascii" || c.ioAdapter != "modbustcp" {
		t.Fatalf("addresses alone keep selecting ascii + modbustcp: %v %+v", err, c)
	}
}

func TestResolveRejectsInconsistentHardware(t *testing.T) {
	mk := func(mut func(*config)) error {
		c := baseConfig()
		c.rfidAdapter, c.lprAdapter, c.cameraAdapter = "webhook", "webhook", "webhook"
		mut(&c)
		return resolveAdapters(&c)
	}
	cases := map[string]func(*config){
		"laumas without a port":      func(c *config) { c.scaleProtocol = "laumas-rtu" },
		"laumas with both transports": func(c *config) { c.scaleProtocol, c.scaleSerial, c.scaleAddr = "laumas-rtu", "COM3", "h:1" },
		"bad parity":                 func(c *config) { c.scaleProtocol, c.scaleSerial, c.scaleParity = "laumas-rtu", "COM3", "X" },
		"slave out of range":         func(c *config) { c.scaleProtocol, c.scaleSerial, c.scaleSlave = "laumas-rtu", "COM3", 300 },
		"modbustcp without address":  func(c *config) { c.ioAdapter = "modbustcp" },
		"unknown scale adapter":      func(c *config) { c.scaleProtocol = "modbus" },
		"io adapter on the scale":    func(c *config) { c.ioAdapter = "laumas-rtu" },
		"unknown rfid adapter":       func(c *config) { c.rfidAdapter = "usb" },
	}
	for name, mut := range cases {
		if err := mk(mut); err == nil {
			t.Fatalf("%s must be refused at start-up", name)
		}
	}
}
