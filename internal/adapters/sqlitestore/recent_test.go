package sqlitestore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

func TestRecentTicketsNewestFirstAndLimited(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "edge.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	t0 := time.Date(2026, 10, 25, 6, 0, 0, 0, time.UTC)
	for i := 1; i <= 5; i++ {
		tk := domain.Ticket{ID: fmt.Sprintf("T%d", i), StationID: "S", TransactionID: fmt.Sprintf("TX%d", i), Plate: "61C-1",
			WeightKG: int64(1000 * i), WeightObservedAt: t0.Add(time.Duration(i) * time.Minute),
			WeightRawRef: domain.RawWeightRef{Seq: uint64(i), Hash: "h"}, Mode: domain.ModeNormal,
			CommittedAt: t0.Add(time.Duration(i) * time.Minute)}
		if err := s.Commit(ctx, tk); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.RecentTickets(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != "T5" || got[1].ID != "T4" || got[2].ID != "T3" {
		ids := []string{}
		for _, g := range got {
			ids = append(ids, g.ID)
		}
		t.Fatalf("want T5,T4,T3 got %v", ids)
	}
	all, err := s.RecentTickets(ctx, 0) // out-of-range limit falls back to the default
	if err != nil || len(all) != 5 {
		t.Fatalf("default limit: n=%d err=%v", len(all), err)
	}
}
