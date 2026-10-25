package siteconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "site.json")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadIsStrictAboutUnknownKeys(t *testing.T) {
	// A misspelt component setting must fail loudly, not fall back to a default.
	_, err := Load(write(t, `{"scale":{"adaptor":"laumas-rtu"}}`))
	if err == nil || !strings.Contains(err.Error(), "adaptor") {
		t.Fatalf("misspelt key must be rejected, got %v", err)
	}
	if _, err := Load(write(t, `{"remote_io":{}}`)); err == nil {
		t.Fatal("the old unused schema must be rejected, not silently ignored")
	}
}

func TestShippedExampleConfigsParse(t *testing.T) {
	for _, p := range []string{"../../config.example.json", "../../config/bench-real-scale.json", "../../config/all-fake.json"} {
		f, err := Load(p)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		for comp, adapter := range map[Component]string{CompScale: f.Scale.Adapter, CompIO: f.IO.Adapter, CompRFID: f.RFID.Adapter, CompLPR: f.LPR.Adapter, CompCameras: f.Cameras.Adapter} {
			if err := Check(comp, adapter); err != nil {
				t.Fatalf("%s: %v", p, err)
			}
		}
	}
}

func TestCheckRejectsAdapterNotValidForComponent(t *testing.T) {
	if err := Check(CompIO, AdapterLaumasRTU); err == nil {
		t.Fatal("laumas-rtu is a scale adapter, not an I/O adapter")
	}
	if err := Check(CompRFID, AdapterNone); err == nil {
		t.Fatal("rfid has no 'none': it is either the real reader or fake")
	}
	if err := Check(CompScale, AdapterFake); err != nil {
		t.Fatal(err)
	}
}

func TestProfileModes(t *testing.T) {
	build := func(scale, io string) Profile {
		var p Profile
		p.Set(CompScale, scale, "")
		p.Set(CompIO, io, "")
		p.Set(CompRFID, AdapterWebhook, "")
		return p
	}
	cases := []struct {
		scale, io, mode string
		sim             []string
	}{
		{AdapterLaumasRTU, AdapterModbusTCP, "LIVE", nil},
		{AdapterLaumasRTU, AdapterFake, "MIXED", []string{"io"}},
		{AdapterFake, AdapterFake, "MIXED", []string{"io", "scale"}}, // the real RFID webhook keeps it mixed
	}
	for _, c := range cases {
		p := build(c.scale, c.io)
		if p.Mode() != c.mode || strings.Join(p.Simulated(), ",") != strings.Join(c.sim, ",") {
			t.Fatalf("%s/%s: mode=%s simulated=%v", c.scale, c.io, p.Mode(), p.Simulated())
		}
	}
	var all Profile
	all.Set(CompScale, AdapterFake, "")
	all.Set(CompIO, AdapterFake, "")
	all.Set(CompRFID, AdapterFake, "")
	if all.Mode() != "SIMULATED" {
		t.Fatalf("everything fake must be SIMULATED, got %s", all.Mode())
	}
	var none Profile
	none.Set(CompScale, AdapterNone, "")
	none.Set(CompIO, AdapterFake, "")
	if none.Mode() != "SIMULATED" {
		t.Fatalf("an absent component does not make a profile mixed, got %s", none.Mode())
	}
}
