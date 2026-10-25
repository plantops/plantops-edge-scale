package registry

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

// Static is a zero-dependency bootstrap registry. Production master data can
// later move to SQLite/Central sync behind the same VehicleRegistry port.
type Static struct {
	Vehicles map[string]domain.VehicleIdentity
}

func (s Static) ResolveRFID(_ context.Context, tag string) (domain.VehicleIdentity, bool, error) {
	v, ok := s.Vehicles[strings.TrimSpace(tag)]
	return v, ok, nil
}

// List returns the registered vehicles ordered by plate.
func (s Static) List() []domain.VehicleIdentity {
	out := make([]domain.VehicleIdentity, 0, len(s.Vehicles))
	for _, v := range s.Vehicles {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Plate < out[j].Plate })
	return out
}

// Parse accepts comma-separated TAG=PLATE pairs, for example:
// RFID-DEMO-001=15C-123.45,RFID-002=15C-456.78
func Parse(spec string) (Static, error) {
	out := Static{Vehicles: map[string]domain.VehicleIdentity{}}
	if strings.TrimSpace(spec) == "" {
		return out, nil
	}
	for _, item := range strings.Split(spec, ",") {
		parts := strings.SplitN(strings.TrimSpace(item), "=", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			return Static{}, fmt.Errorf("invalid vehicle mapping %q; expected RFID=PLATE", item)
		}
		tag := strings.TrimSpace(parts[0])
		plate := strings.TrimSpace(parts[1])
		out.Vehicles[tag] = domain.VehicleIdentity{RFIDTag: tag, Plate: plate}
	}
	return out, nil
}
