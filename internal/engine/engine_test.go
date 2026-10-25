package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
)

type fakeRegistry struct{ vehicles map[string]domain.VehicleIdentity }

func (r fakeRegistry) ResolveRFID(_ context.Context, tag string) (domain.VehicleIdentity, bool, error) {
	v, ok := r.vehicles[tag]
	return v, ok, nil
}

type fakeTicketStore struct {
	mu      sync.Mutex
	tickets []domain.Ticket
	err     error
}

func (s *fakeTicketStore) Commit(_ context.Context, t domain.Ticket) error {
	if s.err != nil {
		return s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tickets = append(s.tickets, t)
	return nil
}

func (s *fakeTicketStore) latest() domain.Ticket {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.tickets) == 0 {
		return domain.Ticket{}
	}
	return s.tickets[len(s.tickets)-1]
}

func newTestEngine(store *fakeTicketStore) *Engine {
	return New(Config{
		StationID: "S01", EmptyScaleMaxKG: 200, MinStableWeightKG: 1000,
		StableConfirmations: 2, StableToleranceKG: 20,
	}, store, fakeRegistry{vehicles: map[string]domain.VehicleIdentity{
		"RFID-1": {RFIDTag: "RFID-1", Plate: "15C-123.45"},
	}})
}

func audited(weight int64, stable bool, seq uint64, at time.Time) domain.AuditedScaleReading {
	return domain.AuditedScaleReading{
		Reading: domain.ScaleReading{WeightKG: weight, Stable: stable, Health: domain.HealthHealthy, Observed: at},
		RawRef: domain.RawWeightRef{Seq: seq, Hash: fmt.Sprintf("hash-%d", seq)},
	}
}

func beginIdentifiedTruck(t *testing.T, e *Engine) time.Time {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC()
	// Entry authorization requires two independent empty-deck signals:
	// position sensors clear + an audited stable near-zero controller reading.
	if err := e.ObserveScale(ctx, audited(0, true, 1, now)); err != nil {
		t.Fatal(err)
	}
	if err := e.ObservePosition(ctx, domain.PositionSnapshot{EntryPresent: true, SafetyClear: true, Observed: now.Add(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	start := e.Snapshot().Transaction.StartedAt
	at := start.Add(time.Millisecond)
	if err := e.ObserveRFID(ctx, domain.RFIDObservation{Tag: "RFID-1", Health: domain.HealthHealthy, Observed: at}); err != nil {
		t.Fatal(err)
	}
	if err := e.ObserveLPR(ctx, domain.LPRObservation{Plate: "15C12345", Health: domain.HealthHealthy, Observed: at.Add(time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	s := e.Snapshot()
	if s.State != domain.StateEntryAuthorized || s.Transaction.Identity != domain.IdentityAccepted {
		t.Fatalf("identity/entry state=%s identity=%s reason=%s", s.State, s.Transaction.Identity, s.Transaction.LastBlockReason)
	}
	if !s.Transaction.Outputs.EntryGreen || !s.Transaction.Outputs.EntryBarrierOpen {
		t.Fatalf("entry outputs not authorized: %+v", s.Transaction.Outputs)
	}
	return at
}

func TestHappyPathCommitsTicketWithExactRawWeightRef(t *testing.T) {
	ctx := context.Background()
	store := &fakeTicketStore{}
	e := newTestEngine(store)
	at := beginIdentifiedTruck(t, e)

	if err := e.ObservePosition(ctx, domain.PositionSnapshot{
		FrontPresent: true, RearPresent: true, SafetyClear: true, Observed: at.Add(2 * time.Millisecond),
	}); err != nil {
		t.Fatal(err)
	}
	if got := e.Snapshot().Transaction.Position; got != domain.PositionAccepted {
		t.Fatalf("position=%s", got)
	}

	if err := e.ObserveScale(ctx, audited(28300, false, 10, at.Add(3*time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	if got := e.Snapshot().State; got != domain.StateWeighing {
		t.Fatalf("state=%s want WEIGHING", got)
	}
	if err := e.ObserveScale(ctx, audited(28455, true, 11, at.Add(4*time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	if err := e.ObserveScale(ctx, audited(28460, true, 12, at.Add(5*time.Millisecond))); err != nil {
		t.Fatal(err)
	}

	s := e.Snapshot()
	if s.State != domain.StateExitAuthorized {
		t.Fatalf("state=%s want EXIT_AUTHORIZED; block=%s", s.State, s.Transaction.LastBlockReason)
	}
	if s.Transaction.AcceptedWeight == nil || s.Transaction.AcceptedWeight.RawRef.Seq != 12 {
		t.Fatalf("accepted raw ref=%+v", s.Transaction.AcceptedWeight)
	}
	if !s.Transaction.Outputs.ExitGreen || !s.Transaction.Outputs.ExitBarrierOpen {
		t.Fatalf("exit release not derived after local commit: %+v", s.Transaction.Outputs)
	}
	ticket := store.latest()
	if ticket.WeightKG != 28460 || ticket.WeightRawRef.Seq != 12 || ticket.WeightRawRef.Hash != s.Transaction.AcceptedWeight.RawRef.Hash {
		t.Fatalf("ticket is not linked to accepted raw frame: %+v", ticket)
	}

	if err := e.ObservePosition(ctx, domain.PositionSnapshot{ExitPresent: true, SafetyClear: true, Observed: at.Add(6 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if err := e.ObservePosition(ctx, domain.PositionSnapshot{ExitPresent: false, SafetyClear: true, Observed: at.Add(7 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if got := e.Snapshot().State; got != domain.StateComplete {
		t.Fatalf("state=%s want COMPLETE", got)
	}
}

func TestEntryBlockedWithoutAuditedStableEmptyScale(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(&fakeTicketStore{})
	now := time.Now().UTC()
	_ = e.ObservePosition(ctx, domain.PositionSnapshot{EntryPresent: true, SafetyClear: true, Observed: now})
	at := e.Snapshot().Transaction.StartedAt.Add(time.Millisecond)
	_ = e.ObserveRFID(ctx, domain.RFIDObservation{Tag: "RFID-1", Health: domain.HealthHealthy, Observed: at})
	_ = e.ObserveLPR(ctx, domain.LPRObservation{Plate: "15C-123.45", Health: domain.HealthHealthy, Observed: at.Add(time.Millisecond)})
	if got := e.Snapshot().State; got == domain.StateEntryAuthorized {
		t.Fatal("entry must not open without audited stable empty-scale proof")
	}
	if err := e.ObserveScale(ctx, audited(800, true, 2, at.Add(2*time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	if got := e.Snapshot().State; got == domain.StateEntryAuthorized {
		t.Fatal("entry must remain blocked while scale carries material/vehicle weight")
	}
	if err := e.ObserveScale(ctx, audited(0, true, 3, at.Add(3*time.Millisecond))); err != nil {
		t.Fatal(err)
	}
	if got := e.Snapshot().State; got != domain.StateEntryAuthorized {
		t.Fatalf("state=%s want ENTRY_AUTHORIZED after audited stable zero", got)
	}
}

func TestWorkflowRejectsBareUnauditedScaleReading(t *testing.T) {
	store := &fakeTicketStore{}
	e := newTestEngine(store)
	err := e.ObserveScale(context.Background(), domain.AuditedScaleReading{
		Reading: domain.ScaleReading{WeightKG: 28460, Stable: true, Health: domain.HealthHealthy, Observed: time.Now().UTC()},
	})
	if err == nil {
		t.Fatal("expected missing raw audit reference to be rejected")
	}
}

func TestIdentityMismatchBlocksEntry(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(&fakeTicketStore{})
	now := time.Now().UTC()
	_ = e.ObserveScale(ctx, audited(0, true, 1, now))
	_ = e.ObservePosition(ctx, domain.PositionSnapshot{EntryPresent: true, SafetyClear: true, Observed: now.Add(time.Millisecond)})
	at := e.Snapshot().Transaction.StartedAt.Add(time.Millisecond)
	_ = e.ObserveRFID(ctx, domain.RFIDObservation{Tag: "RFID-1", Health: domain.HealthHealthy, Observed: at})
	_ = e.ObserveLPR(ctx, domain.LPRObservation{Plate: "15C-999.99", Health: domain.HealthHealthy, Observed: at.Add(time.Millisecond)})
	s := e.Snapshot()
	if s.State != domain.StateIdentityMismatch || s.Transaction.Identity != domain.IdentityMismatch {
		t.Fatalf("state=%s identity=%s", s.State, s.Transaction.Identity)
	}
	if s.Transaction.Outputs.EntryBarrierOpen || s.Transaction.Outputs.EntryGreen {
		t.Fatal("identity mismatch must not authorize entry")
	}
}

func TestSinglePositionSensorFaultRequiresTransactionOverride(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(&fakeTicketStore{})
	at := beginIdentifiedTruck(t, e)

	if err := e.ObserveFault(ctx, domain.Fault{
		Device: domain.DeviceFrontSensor, Health: domain.HealthFault,
		Reason: "no signal", Overridable: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := e.ObservePosition(ctx, domain.PositionSnapshot{RearPresent: true, SafetyClear: true, Observed: at.Add(2 * time.Millisecond)}); err != nil {
		t.Fatal(err)
	}
	if got := e.Snapshot().Transaction.Position; got == domain.PositionAccepted {
		t.Fatal("position must not be accepted before override evidence")
	}

	txID := e.ActiveTransactionID()
	if err := e.AuthorizeOverride(ctx, domain.Override{
		TransactionID: txID, Device: domain.DeviceFrontSensor, Reason: "front sensor no signal",
		RequestedBy: "operator01", AuthorizedBy: "operator01", AuthorizedAs: domain.RoleOperator,
		Evidence: []string{domain.EvidencePositionConfirmed},
	}); err != nil {
		t.Fatal(err)
	}
	s := e.Snapshot()
	if s.Transaction.Position != domain.PositionAccepted || s.Mode != domain.ModeDegraded {
		t.Fatalf("position=%s mode=%s block=%s", s.Transaction.Position, s.Mode, s.Transaction.LastBlockReason)
	}
}

func TestBothDeckSensorFaultsForceManual(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(&fakeTicketStore{})
	_ = beginIdentifiedTruck(t, e)
	_ = e.ObserveFault(ctx, domain.Fault{Device: domain.DeviceFrontSensor, Health: domain.HealthFault, Reason: "front failed", Overridable: true})
	_ = e.ObserveFault(ctx, domain.Fault{Device: domain.DeviceRearSensor, Health: domain.HealthFault, Reason: "rear failed", Overridable: true})
	s := e.Snapshot()
	if s.Mode != domain.ModeManual || s.Transaction.Position != domain.PositionManual {
		t.Fatalf("mode=%s position=%s", s.Mode, s.Transaction.Position)
	}
}

func TestScaleFaultLocksOutAndDropsAllDesiredOutputs(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(&fakeTicketStore{})
	_ = beginIdentifiedTruck(t, e)
	if err := e.ObserveFault(ctx, domain.Fault{
		Device: domain.DeviceScale, Health: domain.HealthDisconnected,
		Reason: "TCP disconnected", Overridable: false, Critical: true,
	}); err != nil {
		t.Fatal(err)
	}
	s := e.Snapshot()
	if s.State != domain.StateFaultLockout || s.Mode != domain.ModeLockout {
		t.Fatalf("state=%s mode=%s", s.State, s.Mode)
	}
	if s.Transaction.Outputs != (domain.DesiredOutputs{}) {
		t.Fatalf("lockout must remove software motion requests: %+v", s.Transaction.Outputs)
	}
}

func TestTicketCommitFailurePreventsRelease(t *testing.T) {
	ctx := context.Background()
	store := &fakeTicketStore{err: errors.New("disk full")}
	e := newTestEngine(store)
	at := beginIdentifiedTruck(t, e)
	_ = e.ObservePosition(ctx, domain.PositionSnapshot{FrontPresent: true, RearPresent: true, SafetyClear: true, Observed: at.Add(time.Millisecond)})
	_ = e.ObserveScale(ctx, audited(28460, true, 21, at.Add(2*time.Millisecond)))
	err := e.ObserveScale(ctx, audited(28460, true, 22, at.Add(3*time.Millisecond)))
	if err == nil {
		t.Fatal("expected durable ticket commit failure")
	}
	s := e.Snapshot()
	if s.State != domain.StateFaultLockout || s.Transaction.LocalCommittedAt != nil {
		t.Fatalf("state=%s committed=%v", s.State, s.Transaction.LocalCommittedAt)
	}
	if s.Transaction.Outputs.ExitBarrierOpen || s.Transaction.Outputs.ExitGreen {
		t.Fatal("failed local commit must never authorize release")
	}
}

// Regression found by the fleet simulator. The edge calls ClearFault(SCALE) on
// every healthy scale frame; that re-evaluated identity and re-ran entry
// authorisation without looking at the state. As soon as the truck left the deck
// and the scale read empty, a transaction that had already been released
// (EXIT_AUTHORIZED / EXITING) was dragged back to ENTRY_AUTHORIZED, re-opening
// the entry lamp and barrier while the exit side was still open.
func TestScaleFaultClearAfterReleaseNeverReauthorizesEntry(t *testing.T) {
	for _, tc := range []struct {
		name       string
		exitSeen   bool
		wantStates []domain.WorkflowState
	}{
		{"truck gone from deck, exit sensor not yet reached", false, []domain.WorkflowState{domain.StateExitAuthorized}},
		{"truck on the exit sensor", true, []domain.WorkflowState{domain.StateExiting}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			e := newTestEngine(&fakeTicketStore{})
			at := beginIdentifiedTruck(t, e)
			deck := domain.PositionSnapshot{FrontPresent: true, RearPresent: true, SafetyClear: true, Observed: at.Add(5 * time.Millisecond)}
			if err := e.ObservePosition(ctx, deck); err != nil { t.Fatal(err) }
			for i := uint64(0); i < 3; i++ {
				if err := e.ObserveScale(ctx, audited(24000, true, 10+i, at.Add(time.Duration(10+i)*time.Millisecond))); err != nil { t.Fatal(err) }
			}
			if s := e.Snapshot(); s.State != domain.StateExitAuthorized || !s.Transaction.Outputs.ExitGreen {
				t.Fatalf("setup: expected EXIT_AUTHORIZED with exit green, got %s %+v", s.State, s.Transaction.Outputs)
			}

			pos := domain.PositionSnapshot{ExitPresent: tc.exitSeen, SafetyClear: true, Observed: at.Add(50 * time.Millisecond)}
			if err := e.ObservePosition(ctx, pos); err != nil { t.Fatal(err) }
			// The deck is empty again and the scale reads zero, exactly as the
			// live collector reports it, followed by its per-frame ClearFault.
			if err := e.ObserveScale(ctx, audited(0, true, 20, at.Add(60*time.Millisecond))); err != nil { t.Fatal(err) }
			for i := 0; i < 3; i++ {
				if err := e.ClearFault(ctx, domain.DeviceScale); err != nil { t.Fatal(err) }
			}

			s := e.Snapshot()
			ok := false
			for _, w := range tc.wantStates { ok = ok || s.State == w }
			if !ok {
				t.Fatalf("released transaction was pulled back to %s, want %v", s.State, tc.wantStates)
			}
			if o := s.Transaction.Outputs; o.EntryGreen || o.EntryBarrierOpen {
				t.Fatalf("entry permission re-granted after release: %+v", o)
			}
		})
	}
}

// A truck whose identity is still being resolved must keep being authorised by
// later identity evidence; the guard above must not break that path.
func TestIdentityEvidenceStillAuthorizesEntryWhileIdentifying(t *testing.T) {
	ctx := context.Background()
	e := newTestEngine(&fakeTicketStore{})
	now := time.Now().UTC()
	if err := e.ObserveScale(ctx, audited(0, true, 1, now)); err != nil { t.Fatal(err) }
	if err := e.ObservePosition(ctx, domain.PositionSnapshot{EntryPresent: true, SafetyClear: true, Observed: now.Add(time.Millisecond)}); err != nil { t.Fatal(err) }
	start := e.Snapshot().Transaction.StartedAt
	if err := e.ObserveRFID(ctx, domain.RFIDObservation{Tag: "RFID-1", Health: domain.HealthHealthy, Observed: start.Add(time.Millisecond)}); err != nil { t.Fatal(err) }
	if s := e.Snapshot(); s.State == domain.StateEntryAuthorized {
		t.Fatalf("RFID alone must not authorise entry, state=%s", s.State)
	}
	if err := e.ObserveLPR(ctx, domain.LPRObservation{Plate: "15C-123.45", Health: domain.HealthHealthy, Observed: start.Add(2 * time.Millisecond)}); err != nil { t.Fatal(err) }
	if s := e.Snapshot(); s.State != domain.StateEntryAuthorized || !s.Transaction.Outputs.EntryGreen {
		t.Fatalf("matching RFID+LPR must authorise entry, state=%s", s.State)
	}
}
