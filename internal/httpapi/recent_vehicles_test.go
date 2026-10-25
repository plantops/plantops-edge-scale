package httpapi

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/adapters/ingress"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

func TestRecentTicketsAPIPassesLimitAndNeverReturnsNull(t *testing.T) {
	gotLimit := 0
	s := &Server{RFID: &ingress.RFID{}, LPR: &ingress.LPR{}, RecentTickets: func(_ context.Context, n int) ([]domain.Ticket, error) {
		gotLimit = n
		return nil, nil
	}}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/tickets/recent?limit=7", nil))
	if rr.Code != http.StatusOK || gotLimit != 7 || !bytes.Contains(rr.Body.Bytes(), []byte(`"tickets":[]`)) {
		t.Fatalf("status=%d limit=%d body=%s", rr.Code, gotLimit, rr.Body.String())
	}
}

func TestVehiclesAPI(t *testing.T) {
	s := &Server{RFID: &ingress.RFID{}, LPR: &ingress.LPR{}, Vehicles: func() any {
		return []domain.VehicleIdentity{{RFIDTag: "E2001111AAAA", Plate: "61C-123.45"}}
	}}
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/vehicles", nil))
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"plate":"61C-123.45"`)) {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	// Not configured: an empty list, not an error.
	rr = httptest.NewRecorder()
	(&Server{RFID: &ingress.RFID{}, LPR: &ingress.LPR{}}).Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/vehicles", nil))
	if rr.Code != http.StatusOK || !bytes.Contains(rr.Body.Bytes(), []byte(`"vehicles":[]`)) {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}
