package fleet

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

// MockCentral receives what the edge pushes upstream: completed tickets and
// heartbeats. It is also where the sim reads back the tickets to check them.
type MockCentral struct {
	mu         sync.Mutex
	tickets    []domain.Ticket
	seen       map[string]bool
	Duplicates int
	Heartbeats int
	srv        *http.Server
	Addr       string
}

func StartMockCentral(ctx context.Context, listen string) (*MockCentral, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	m := &MockCentral{seen: map[string]bool{}, Addr: ln.Addr().String()}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /tickets", func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var t domain.Ticket
		if json.Unmarshal(b, &t) != nil || t.ID == "" {
			http.Error(w, "bad ticket", http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		if m.seen[t.ID] {
			m.Duplicates++
		} else {
			m.seen[t.ID] = true
			m.tickets = append(m.tickets, t)
		}
		m.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("POST /heartbeat", func(w http.ResponseWriter, _ *http.Request) {
		m.mu.Lock()
		m.Heartbeats++
		m.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	})
	m.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = m.srv.Serve(ln) }()
	go func() { <-ctx.Done(); _ = m.srv.Close() }()
	return m, nil
}

func (m *MockCentral) Tickets() []domain.Ticket {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]domain.Ticket(nil), m.tickets...)
}
func (m *MockCentral) TicketURL() string    { return "http://" + m.Addr + "/tickets" }
func (m *MockCentral) HeartbeatURL() string { return "http://" + m.Addr + "/heartbeat" }
