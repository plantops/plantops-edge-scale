package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/domain"
	"github.com/ngtrthanh/plantops-edge-scale/goedge/internal/ports"
)

type Config struct {
	StationID           string
	EmptyScaleMaxKG     int64
	MinStableWeightKG   int64
	StableConfirmations int
	StableToleranceKG   int64
}

type Snapshot struct {
	StationID   string                       `json:"station_id"`
	State       domain.WorkflowState         `json:"state"`
	Mode        domain.Mode                  `json:"mode"`
	Transaction *domain.Transaction          `json:"transaction,omitempty"`
	LatestScale *domain.AuditedScaleReading  `json:"latest_scale,omitempty"`
}

type Engine struct {
	mu       sync.RWMutex
	cfg      Config
	tickets  ports.TicketStore
	registry ports.VehicleRegistry

	tx         *domain.Transaction
	lastScale  *domain.AuditedScaleReading
	faults     map[domain.DeviceID]domain.Fault
	stableBase *domain.AuditedScaleReading

	now   func() time.Time
	newID func(string) string
}

func New(cfg Config, tickets ports.TicketStore, registry ports.VehicleRegistry) *Engine {
	if cfg.StationID == "" {
		cfg.StationID = "EDGE-01"
	}
	if cfg.EmptyScaleMaxKG <= 0 {
		cfg.EmptyScaleMaxKG = 500
	}
	if cfg.MinStableWeightKG <= 0 {
		cfg.MinStableWeightKG = 500
	}
	if cfg.StableConfirmations <= 0 {
		cfg.StableConfirmations = 2
	}
	if cfg.StableToleranceKG < 0 {
		cfg.StableToleranceKG = 0
	}
	return &Engine{
		cfg: cfg, tickets: tickets, registry: registry,
		faults: make(map[domain.DeviceID]domain.Fault),
		now: func() time.Time { return time.Now().UTC() },
		newID: randomID,
	}
}

func (e *Engine) ActiveTransactionID() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.tx == nil || e.tx.State == domain.StateComplete {
		return ""
	}
	return e.tx.ID
}

func (e *Engine) Snapshot() Snapshot {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := Snapshot{StationID: e.cfg.StationID, State: domain.StateIdle, Mode: domain.ModeNormal}
	if e.lastScale != nil {
		v := *e.lastScale
		out.LatestScale = &v
	}
	if e.tx != nil {
		tx := cloneTransaction(e.tx)
		out.Transaction = tx
		out.State = tx.State
		out.Mode = tx.Mode
	}
	return out
}

// ObservePosition is the authoritative normalized position/presence input to
// the workflow. Weight is intentionally not used as a substitute for position.
func (e *Engine) ObservePosition(ctx context.Context, p domain.PositionSnapshot) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.tx == nil && p.EntryPresent {
		e.startTransactionLocked(p.Observed)
	}
	if e.tx == nil {
		return nil
	}
	e.tx.PositionSnapshot = p
	e.touchLocked(p.Observed)

	if e.tx.State == domain.StateComplete || e.tx.State == domain.StateFaultLockout {
		return nil
	}

	if e.tx.Identity == domain.IdentityAccepted &&
		(e.tx.State == domain.StateApproach || e.tx.State == domain.StateIdentifying) {
		e.tryAuthorizeEntryLocked()
	}

	if p.FrontPresent && e.tx.State == domain.StateEntryAuthorized {
		e.transitionLocked(domain.StateEntering)
	}

	if p.FrontPresent || p.RearPresent {
		if e.tx.State == domain.StateEntering || e.tx.State == domain.StateEntryAuthorized {
			e.transitionLocked(domain.StatePositioning)
		}
		e.evaluatePositionLocked()
		if e.tx.Position == domain.PositionAccepted {
			e.tryReadyToWeighLocked()
		}
	}

	if e.tx.State == domain.StateExitAuthorized && p.ExitPresent {
		e.tx.ExitSeen = true
		e.transitionLocked(domain.StateExiting)
	}
	if e.tx.State == domain.StateExiting && e.tx.ExitSeen && !p.ExitPresent {
		if p.SafetyClear || e.hasOverrideLocked(domain.DeviceExitSensor, domain.EvidenceExitClear) {
			now := e.nowUTC(p.Observed)
			e.tx.CompletedAt = &now
			e.transitionLocked(domain.StateComplete)
			e.tx.Outputs = domain.DesiredOutputs{}
		} else {
			e.tx.LastBlockReason = "exit not proven clear; barrier close must remain inhibited"
		}
	}
	return nil
}

func (e *Engine) ObserveRFID(ctx context.Context, o domain.RFIDObservation) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tx == nil || !e.observationBelongsLocked(o.Observed) {
		return nil
	}
	e.tx.RFID = o
	e.touchLocked(o.Observed)
	return e.evaluateIdentityLocked(ctx)
}

func (e *Engine) ObserveLPR(ctx context.Context, o domain.LPRObservation) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tx == nil || !e.observationBelongsLocked(o.Observed) {
		return nil
	}
	e.tx.LPR = o
	e.touchLocked(o.Observed)
	return e.evaluateIdentityLocked(ctx)
}

// ObserveScale accepts only a reading carrying the immutable raw-audit
// sequence/hash reference returned after fsync. There is deliberately no API
// here for a bare ScaleReading.
func (e *Engine) ObserveScale(ctx context.Context, audited domain.AuditedScaleReading) error {
	if audited.RawRef.Seq == 0 || audited.RawRef.Hash == "" {
		return errors.New("workflow rejects scale reading without raw audit reference")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	v := audited
	e.lastScale = &v
	if e.tx != nil {
		e.tx.LatestScale = &v
		e.touchLocked(audited.Reading.Observed)
	}

	r := audited.Reading
	if r.Health != domain.HealthHealthy || r.Fault != "" || r.Overload {
		e.faults[domain.DeviceScale] = domain.Fault{
			Device: domain.DeviceScale, Health: r.Health, Reason: "authoritative scale unhealthy/faulted/overload",
			Overridable: false, Critical: true,
		}
		if e.tx != nil {
			e.syncFaultSliceLocked()
			e.lockoutLocked("authoritative scale unhealthy/faulted/overload")
		}
		return nil
	}

	// A fresh audited healthy controller frame clears a pre-transaction scale
	// transport fault. It never auto-unlocks an already locked transaction.
	delete(e.faults, domain.DeviceScale)
	if e.tx != nil && e.tx.State != domain.StateFaultLockout {
		e.syncFaultSliceLocked()
	}
	if e.tx == nil || e.tx.State == domain.StateComplete || e.tx.State == domain.StateFaultLockout {
		return nil
	}

	if e.tx.Identity == domain.IdentityAccepted &&
		(e.tx.State == domain.StateApproach || e.tx.State == domain.StateIdentifying) {
		e.tryAuthorizeEntryLocked()
	}
	e.tryReadyToWeighLocked()
	if e.tx.State == domain.StateReadyToWeigh {
		e.transitionLocked(domain.StateWeighing)
	}
	if e.tx.State != domain.StateWeighing {
		return nil
	}

	if !r.Stable || r.WeightKG < e.cfg.MinStableWeightKG {
		e.tx.StableConfirmations = 0
		e.stableBase = nil
		if r.Stable && r.WeightKG < e.cfg.MinStableWeightKG {
			e.tx.LastBlockReason = fmt.Sprintf("stable weight %d kg below configured minimum %d kg", r.WeightKG, e.cfg.MinStableWeightKG)
		}
		return nil
	}

	if e.stableBase == nil || abs64(e.stableBase.Reading.WeightKG-r.WeightKG) > e.cfg.StableToleranceKG {
		base := audited
		e.stableBase = &base
		e.tx.StableConfirmations = 1
	} else {
		e.tx.StableConfirmations++
		base := audited
		e.stableBase = &base
	}
	if e.tx.StableConfirmations < e.cfg.StableConfirmations {
		e.tx.LastBlockReason = fmt.Sprintf("waiting for stable confirmation %d/%d", e.tx.StableConfirmations, e.cfg.StableConfirmations)
		return nil
	}

	return e.commitTicketLocked(ctx, audited)
}

func (e *Engine) ObserveFault(ctx context.Context, f domain.Fault) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.faults[f.Device] = f
	if e.tx == nil {
		return nil
	}
	e.syncFaultSliceLocked()

	if f.Device == domain.DeviceScale || f.Critical || !f.Overridable {
		e.lockoutLocked(fmt.Sprintf("critical/non-overridable fault: %s: %s", f.Device, f.Reason))
		return nil
	}
	e.updateModeLocked()
	if f.Device == domain.DeviceFrontSensor || f.Device == domain.DeviceRearSensor {
		e.evaluatePositionLocked()
	}
	return e.evaluateIdentityLocked(ctx)
}

func (e *Engine) ClearFault(ctx context.Context, device domain.DeviceID) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.faults, device)
	if e.tx == nil {
		return nil
	}
	e.syncFaultSliceLocked()
	if e.tx.State != domain.StateFaultLockout {
		e.updateModeLocked()
	}
	if device == domain.DeviceFrontSensor || device == domain.DeviceRearSensor {
		e.evaluatePositionLocked()
	}
	return e.evaluateIdentityLocked(ctx)
}

func (e *Engine) AuthorizeOverride(ctx context.Context, o domain.Override) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tx == nil {
		return errors.New("no active transaction")
	}
	if o.TransactionID != e.tx.ID {
		return errors.New("override transaction does not match active transaction")
	}
	if err := domain.ValidateOverride(o); err != nil {
		return err
	}
	fault, ok := e.faults[o.Device]
	if !ok || !fault.Overridable || fault.Critical {
		return fmt.Errorf("device %s has no active overridable fault", o.Device)
	}
	required := domain.RequiredRoleForOverride(o.Device, e.auxFaultCountLocked())
	if roleRank(o.AuthorizedAs) < roleRank(required) {
		return fmt.Errorf("%s authorization required", required)
	}
	for _, existing := range e.tx.Overrides {
		if existing.Device == o.Device && existing.ExpiredAt == nil {
			return fmt.Errorf("active override already exists for %s", o.Device)
		}
	}
	if o.AuthorizedAt.IsZero() {
		o.AuthorizedAt = e.now()
	}
	e.tx.Overrides = append(e.tx.Overrides, o)
	e.updateModeLocked()
	if o.Device == domain.DeviceFrontSensor || o.Device == domain.DeviceRearSensor {
		e.evaluatePositionLocked()
		e.tryReadyToWeighLocked()
	}
	return e.evaluateIdentityLocked(ctx)
}

func (e *Engine) ResetCompleted() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.tx == nil {
		return nil
	}
	if e.tx.State != domain.StateComplete {
		return errors.New("transaction is not complete")
	}
	now := e.now()
	for i := range e.tx.Overrides {
		if e.tx.Overrides[i].ExpiredAt == nil {
			t := now
			e.tx.Overrides[i].ExpiredAt = &t
		}
	}
	e.tx = nil
	e.stableBase = nil
	return nil
}

func (e *Engine) startTransactionLocked(observed time.Time) {
	now := e.nowUTC(observed)
	e.tx = &domain.Transaction{
		ID: e.newID("tx"), StationID: e.cfg.StationID,
		State: domain.StateApproach, Mode: domain.ModeNormal,
		StartedAt: now, UpdatedAt: now,
		Identity: domain.IdentityPending, Position: domain.PositionPending,
		Outputs: domain.DesiredOutputs{},
	}
	e.syncFaultSliceLocked()
	for _, f := range e.faults {
		if f.Device == domain.DeviceScale || f.Critical || !f.Overridable {
			e.lockoutLocked(fmt.Sprintf("active critical fault at transaction start: %s: %s", f.Device, f.Reason))
			return
		}
	}
	e.updateModeLocked()
	e.transitionLocked(domain.StateIdentifying)
}

func (e *Engine) evaluateIdentityLocked(ctx context.Context) error {
	if e.tx == nil || e.tx.State == domain.StateFaultLockout || e.tx.State == domain.StateComplete {
		return nil
	}
	rfidFault := e.faults[domain.DeviceRFID]
	lprFault := e.faults[domain.DeviceLPR]
	rfidBad := rfidFault.Device != "" || e.tx.RFID.Health == domain.HealthFault || e.tx.RFID.Health == domain.HealthDisconnected
	lprBad := lprFault.Device != "" || e.tx.LPR.Health == domain.HealthFault || e.tx.LPR.Health == domain.HealthDisconnected

	if !rfidBad && e.tx.RFID.Tag != "" && !lprBad && e.tx.LPR.Plate != "" {
		if e.registry == nil {
			e.tx.Identity = domain.IdentityManual
			e.tx.Mode = domain.ModeManual
			e.tx.LastBlockReason = "vehicle registry unavailable; RFID/LPR cannot be cross-checked"
			return nil
		}
		identity, found, err := e.registry.ResolveRFID(ctx, e.tx.RFID.Tag)
		if err != nil {
			e.tx.Identity = domain.IdentityManual
			e.tx.Mode = domain.ModeManual
			e.tx.LastBlockReason = "vehicle registry lookup failed: " + err.Error()
			return nil
		}
		if !found {
			e.tx.Identity = domain.IdentityManual
			e.tx.Mode = domain.ModeManual
			e.tx.LastBlockReason = "RFID is not mapped to a known vehicle"
			return nil
		}
		if normalizePlate(identity.Plate) != normalizePlate(e.tx.LPR.Plate) {
			e.tx.Identity = domain.IdentityMismatch
			e.tx.IdentityReason = fmt.Sprintf("RFID expected plate %s but LPR read %s", identity.Plate, e.tx.LPR.Plate)
			e.transitionLocked(domain.StateIdentityMismatch)
			e.tx.Outputs.EntryGreen = false
			e.tx.Outputs.EntryBarrierOpen = false
			return nil
		}
		e.tx.Identity = domain.IdentityAccepted
		e.tx.IdentityReason = "RFID/LPR match"
		if e.tx.State == domain.StateIdentityMismatch {
			e.transitionLocked(domain.StateIdentifying)
		}
		e.tryAuthorizeEntryLocked()
		return nil
	}

	if rfidBad && !lprBad && e.tx.LPR.Plate != "" && e.hasOverrideLocked(domain.DeviceRFID, domain.EvidenceIdentityConfirmed) {
		e.tx.Identity = domain.IdentityAccepted
		e.tx.IdentityReason = "RFID degraded override + confirmed LPR identity"
		e.tx.Mode = domain.ModeDegraded
		e.tryAuthorizeEntryLocked()
		return nil
	}

	if lprBad && !rfidBad && e.tx.RFID.Tag != "" && e.hasOverrideLocked(domain.DeviceLPR, domain.EvidenceIdentityConfirmed) {
		if e.registry == nil {
			e.tx.Identity = domain.IdentityManual
			e.tx.Mode = domain.ModeManual
			e.tx.LastBlockReason = "LPR degraded but vehicle registry unavailable"
			return nil
		}
		identity, found, err := e.registry.ResolveRFID(ctx, e.tx.RFID.Tag)
		if err != nil || !found {
			e.tx.Identity = domain.IdentityManual
			e.tx.Mode = domain.ModeManual
			e.tx.LastBlockReason = "LPR degraded and RFID identity cannot be resolved"
			return nil
		}
		e.tx.LPR = domain.LPRObservation{Plate: identity.Plate, Health: domain.HealthFault, Observed: e.tx.RFID.Observed}
		e.tx.Identity = domain.IdentityAccepted
		e.tx.IdentityReason = "LPR degraded override + RFID registry identity"
		e.tx.Mode = domain.ModeDegraded
		e.tryAuthorizeEntryLocked()
		return nil
	}

	if rfidBad && lprBad {
		e.tx.Identity = domain.IdentityManual
		e.tx.Mode = domain.ModeManual
		e.tx.LastBlockReason = "both identity channels unavailable; supervisor manual identity required"
	}
	return nil
}

func (e *Engine) tryAuthorizeEntryLocked() {
	if e.tx == nil || e.tx.Identity != domain.IdentityAccepted || e.tx.State == domain.StateFaultLockout {
		return
	}
	// Entry can only be granted while the truck is still waiting to come on. This
	// function is reached from identity re-evaluation, which the edge triggers on
	// every healthy scale frame (ClearFault); without this guard a transaction that
	// was already weighed and released was pulled back to ENTRY_AUTHORIZED as soon
	// as the deck read empty, re-opening the entry lamp and barrier while the exit
	// side was still open.
	if e.tx.State != domain.StateApproach && e.tx.State != domain.StateIdentifying {
		return
	}
	p := e.tx.PositionSnapshot
	if !p.SafetyClear {
		e.tx.LastBlockReason = "physical safety is not clear for entry barrier motion"
		return
	}
	if p.FrontPresent || p.RearPresent {
		e.tx.LastBlockReason = "scale deck position sensors show occupied"
		return
	}
	if e.lastScale == nil {
		e.tx.LastBlockReason = "entry blocked until an audited scale reading proves empty deck"
		return
	}
	r := e.lastScale.Reading
	if r.Health != domain.HealthHealthy || r.Fault != "" || r.Overload {
		e.lockoutLocked("entry blocked: authoritative scale unhealthy/faulted/overload")
		return
	}
	if !r.Stable || abs64(r.WeightKG) > e.cfg.EmptyScaleMaxKG {
		e.tx.LastBlockReason = fmt.Sprintf("entry blocked: scale not proven empty/stable (weight=%d kg, stable=%v)", r.WeightKG, r.Stable)
		return
	}
	e.transitionLocked(domain.StateEntryAuthorized)
	e.tx.Outputs.EntryGreen = true
	e.tx.Outputs.EntryBarrierOpen = true
	e.tx.LastBlockReason = ""
}

func (e *Engine) evaluatePositionLocked() {
	if e.tx == nil {
		return
	}
	p := e.tx.PositionSnapshot
	_, frontFault := e.faults[domain.DeviceFrontSensor]
	_, rearFault := e.faults[domain.DeviceRearSensor]

	switch {
	case !frontFault && !rearFault && p.FrontPresent && p.RearPresent:
		e.tx.Position = domain.PositionAccepted
		e.tx.Outputs.EntryGreen = false
		e.tx.Outputs.EntryBarrierOpen = false
		e.tx.LastBlockReason = ""
	case frontFault && rearFault:
		e.tx.Position = domain.PositionManual
		e.tx.Mode = domain.ModeManual
		e.tx.LastBlockReason = "both deck position sensors unavailable; supervised manual mode required"
	case frontFault && p.RearPresent && e.hasOverrideLocked(domain.DeviceFrontSensor, domain.EvidencePositionConfirmed):
		e.tx.Position = domain.PositionAccepted
		e.tx.Mode = domain.ModeDegraded
		e.tx.Outputs.EntryGreen = false
		e.tx.Outputs.EntryBarrierOpen = false
		e.tx.LastBlockReason = ""
	case rearFault && p.FrontPresent && e.hasOverrideLocked(domain.DeviceRearSensor, domain.EvidencePositionConfirmed):
		e.tx.Position = domain.PositionAccepted
		e.tx.Mode = domain.ModeDegraded
		e.tx.Outputs.EntryGreen = false
		e.tx.Outputs.EntryBarrierOpen = false
		e.tx.LastBlockReason = ""
	default:
		e.tx.Position = domain.PositionPending
		if frontFault || rearFault {
			e.tx.LastBlockReason = "degraded position requires healthy peer sensor + transaction override with POSITION_CONFIRMED evidence"
		}
	}
}

func (e *Engine) tryReadyToWeighLocked() {
	if e.tx == nil || e.tx.State == domain.StateFaultLockout || e.tx.State == domain.StateComplete {
		return
	}
	if e.tx.Identity != domain.IdentityAccepted || e.tx.Position != domain.PositionAccepted {
		return
	}
	if e.lastScale == nil {
		e.tx.LastBlockReason = "waiting for audited scale reading"
		return
	}
	r := e.lastScale.Reading
	if r.Health != domain.HealthHealthy || r.Fault != "" || r.Overload {
		e.lockoutLocked("authoritative scale unavailable/faulted/overload")
		return
	}
	if stateBeforeWeigh(e.tx.State) {
		e.transitionLocked(domain.StateReadyToWeigh)
	}
}

func (e *Engine) commitTicketLocked(ctx context.Context, audited domain.AuditedScaleReading) error {
	if e.tx == nil {
		return errors.New("no active transaction")
	}
	if e.tickets == nil {
		e.lockoutLocked("local durable ticket store unavailable")
		return errors.New("ticket store unavailable")
	}
	now := e.now()
	ticketID := e.newID("ticket")
	ticket := domain.Ticket{
		ID: ticketID, StationID: e.cfg.StationID, TransactionID: e.tx.ID,
		Plate: e.tx.LPR.Plate, RFID: e.tx.RFID.Tag,
		WeightKG: audited.Reading.WeightKG, WeightObservedAt: audited.Reading.Observed,
		WeightRawRef: audited.RawRef, Mode: e.tx.Mode,
		Overrides: append([]domain.Override(nil), e.tx.Overrides...), CommittedAt: now,
	}
	if err := e.tickets.Commit(ctx, ticket); err != nil {
		e.lockoutLocked("local ticket commit failed: " + err.Error())
		return err
	}

	e.tx.AcceptedWeight = &domain.WeightAcceptance{
		WeightKG: audited.Reading.WeightKG, ObservedAt: audited.Reading.Observed, RawRef: audited.RawRef,
	}
	e.tx.TicketID = ticketID
	e.tx.LocalCommittedAt = &now
	e.transitionLocked(domain.StateLocalCommitted)

	// Release authorization follows local durability only. Central is not part
	// of this condition.
	e.transitionLocked(domain.StateExitAuthorized)
	e.tx.Outputs.EntryGreen = false
	e.tx.Outputs.EntryBarrierOpen = false
	e.tx.Outputs.ExitGreen = true
	e.tx.Outputs.ExitBarrierOpen = true
	e.tx.Outputs.Buzzer = true
	e.tx.LastBlockReason = ""
	return nil
}

func (e *Engine) lockoutLocked(reason string) {
	if e.tx == nil {
		return
	}
	e.tx.Mode = domain.ModeLockout
	e.tx.State = domain.StateFaultLockout
	e.tx.LastBlockReason = reason
	e.tx.Outputs = domain.DesiredOutputs{}
	e.tx.UpdatedAt = e.now()
}

func (e *Engine) updateModeLocked() {
	if e.tx == nil || e.tx.State == domain.StateFaultLockout {
		return
	}
	aux := e.auxFaultCountLocked()
	switch {
	case aux == 0:
		if e.tx.Mode != domain.ModeManual {
			e.tx.Mode = domain.ModeNormal
		}
	case aux == 1:
		if e.tx.Mode != domain.ModeManual {
			e.tx.Mode = domain.ModeDegraded
		}
	default:
		e.tx.Mode = domain.ModeManual
	}
}

func (e *Engine) auxFaultCountLocked() int {
	count := 0
	for device, f := range e.faults {
		if device == domain.DeviceScale || f.Critical || !f.Overridable {
			continue
		}
		count++
	}
	return count
}

func (e *Engine) syncFaultSliceLocked() {
	if e.tx == nil {
		return
	}
	e.tx.Faults = e.tx.Faults[:0]
	for _, f := range e.faults {
		e.tx.Faults = append(e.tx.Faults, f)
	}
}

func (e *Engine) hasOverrideLocked(device domain.DeviceID, evidence string) bool {
	if e.tx == nil {
		return false
	}
	for _, o := range e.tx.Overrides {
		if o.Device != device || o.ExpiredAt != nil {
			continue
		}
		for _, ev := range o.Evidence {
			if strings.EqualFold(strings.TrimSpace(ev), evidence) {
				return true
			}
		}
	}
	return false
}

func (e *Engine) observationBelongsLocked(observed time.Time) bool {
	if e.tx == nil {
		return false
	}
	if observed.IsZero() {
		return true
	}
	return !observed.Before(e.tx.StartedAt)
}

func (e *Engine) transitionLocked(state domain.WorkflowState) {
	if e.tx == nil {
		return
	}
	e.tx.State = state
	e.tx.UpdatedAt = e.now()
}

func (e *Engine) touchLocked(observed time.Time) {
	if e.tx == nil {
		return
	}
	e.tx.UpdatedAt = e.nowUTC(observed)
}

func (e *Engine) nowUTC(observed time.Time) time.Time {
	if !observed.IsZero() {
		return observed.UTC()
	}
	return e.now()
}

func stateBeforeWeigh(s domain.WorkflowState) bool {
	switch s {
	case domain.StateEntryAuthorized, domain.StateEntering, domain.StatePositioning, domain.StateReadyToWeigh:
		return true
	default:
		return false
	}
}

func normalizePlate(s string) string {
	var b strings.Builder
	for _, r := range strings.ToUpper(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func roleRank(r domain.Role) int {
	switch r {
	case domain.RoleSupervisor:
		return 2
	case domain.RoleOperator:
		return 1
	default:
		return 0
	}
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func randomID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%s-%d", prefix, time.Now().UTC().UnixNano())
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

func cloneTransaction(in *domain.Transaction) *domain.Transaction {
	if in == nil {
		return nil
	}
	out := *in
	out.Faults = append([]domain.Fault(nil), in.Faults...)
	out.Overrides = append([]domain.Override(nil), in.Overrides...)
	if in.LatestScale != nil {
		v := *in.LatestScale
		out.LatestScale = &v
	}
	if in.AcceptedWeight != nil {
		v := *in.AcceptedWeight
		out.AcceptedWeight = &v
	}
	if in.LocalCommittedAt != nil {
		v := *in.LocalCommittedAt
		out.LocalCommittedAt = &v
	}
	if in.CompletedAt != nil {
		v := *in.CompletedAt
		out.CompletedAt = &v
	}
	return &out
}
