package domain

import (
	"errors"
	"fmt"
	"time"
)

type Direction string

const (
	DirectionAToB Direction = "A_TO_B"
	DirectionBToA Direction = "B_TO_A"
)

type PassNumber int

const (
	PassFirst  PassNumber = 1
	PassSecond PassNumber = 2
)

type CycleStatus string

const (
	CycleQueued            CycleStatus = "QUEUED"
	CycleCalled            CycleStatus = "CALLED"
	CycleComplete          CycleStatus = "COMPLETE"
	CycleOrphanedFirstPass CycleStatus = "ORPHANED_FIRST_PASS"
	CycleUnpairedReturn    CycleStatus = "UNPAIRED_RETURN"
	CyclePairTimeInvalid   CycleStatus = "PAIR_TIME_INVALID"
	CyclePairWeightInvalid CycleStatus = "PAIR_WEIGHT_INVALID"
	CycleWrongTruck        CycleStatus = "WRONG_TRUCK"
)

// MaterialFlow says which way the goods moved across the weighbridge. It is
// derived from the two weights, never from the physical pass direction: pass 1
// is always A_TO_B and pass 2 always B_TO_A.
//
//	MATERIAL_IN  first pass heavier  (loaded in on A->B, empty back on B->A)
//	MATERIAL_OUT first pass lighter  (empty in on A->B, loaded back on B->A)
type MaterialFlow string

const (
	FlowMaterialIn  MaterialFlow = "MATERIAL_IN"
	FlowMaterialOut MaterialFlow = "MATERIAL_OUT"
)

// FlowOf derives the material flow from the two accepted weights. Equal weights
// carry no net and therefore no flow.
func FlowOf(firstKG, secondKG int64) MaterialFlow {
	switch {
	case firstKG > secondKG:
		return FlowMaterialIn
	case secondKG > firstKG:
		return FlowMaterialOut
	default:
		return ""
	}
}

type CameraEvidence struct {
	CameraID   string    `json:"camera_id"`
	Role       string    `json:"role,omitempty"`
	ImageRef   string    `json:"image_ref"`
	CapturedAt time.Time `json:"captured_at"`
}

// WeighPass is the durable business evidence created by one short physical
// scale crossing. A pass is not itself a completed business transaction.
type WeighPass struct {
	ID            string           `json:"id"`
	CycleID       string           `json:"cycle_id,omitempty"`
	Number        PassNumber       `json:"number"`
	Direction     Direction        `json:"direction"`
	StationID     string           `json:"station_id"`
	Plate         string           `json:"plate"`
	RFID          string           `json:"rfid"`
	Weight        WeightAcceptance `json:"weight"`
	Mode          Mode             `json:"mode"`
	Overrides     []Override       `json:"overrides,omitempty"`
	CameraEvidence []CameraEvidence `json:"camera_evidence,omitempty"`
	CommittedAt   time.Time        `json:"committed_at"`
}

// WeighCycle is the long-lived business transaction spanning two independent
// physical pass sessions. The scale may serve many other vehicles while a
// cycle remains queued between first and second pass.
type WeighCycle struct {
	ID             string       `json:"id"`
	StationID      string       `json:"station_id"`
	Plate          string       `json:"plate"`
	RFID           string       `json:"rfid"`
	Status         CycleStatus  `json:"status"`
	FirstPass      WeighPass    `json:"first_pass"`
	SecondPass     *WeighPass   `json:"second_pass,omitempty"`
	QueuedAt       time.Time    `json:"queued_at"`
	CalledAt       *time.Time   `json:"called_at,omitempty"`
	CompletedAt    *time.Time   `json:"completed_at,omitempty"`
	PairElapsed    time.Duration `json:"pair_elapsed_ns,omitempty"`
	GrossKG        int64        `json:"gross_kg,omitempty"`
	TareKG         int64        `json:"tare_kg,omitempty"`
	NetKG          int64        `json:"net_kg,omitempty"`
	Flow           MaterialFlow `json:"flow,omitempty"`
	LastBlockReason string      `json:"last_block_reason,omitempty"`
}

// PairPolicy is business/master-data configuration. Zero-valued limits mean
// "not configured"; the workflow must not hide site-specific limits in code.
//
// MinGross/MaxGross apply to the heavier of the two weights and MinTare/MaxTare
// to the lighter one, whichever pass produced them.
type PairPolicy struct {
	// RequireGrossFirst restores the legacy inbound-only rule: pass 1 must be the
	// heavier weight. Leave false to accept tare-first (material OUT) trips too.
	RequireGrossFirst bool `json:"require_gross_first"`

	MinElapsed time.Duration `json:"min_elapsed"`
	MaxElapsed time.Duration `json:"max_elapsed"`
	MinGrossKG int64         `json:"min_gross_kg"`
	MaxGrossKG int64         `json:"max_gross_kg"`
	MinTareKG  int64         `json:"min_tare_kg"`
	MaxTareKG  int64         `json:"max_tare_kg"`
	MinNetKG   int64         `json:"min_net_kg"`
	MaxNetKG   int64         `json:"max_net_kg"`
}

type PairValidation struct {
	Valid   bool          `json:"valid"`
	Status  CycleStatus   `json:"status"`
	Reason  string        `json:"reason,omitempty"`
	Elapsed time.Duration `json:"elapsed_ns"`
	GrossKG int64         `json:"gross_kg"`
	TareKG  int64         `json:"tare_kg"`
	NetKG   int64         `json:"net_kg"`
	Flow    MaterialFlow  `json:"flow,omitempty"`
}

func ValidatePair(first, second WeighPass, policy PairPolicy) PairValidation {
	out := PairValidation{
		Valid: false,
		Status: CyclePairWeightInvalid,
		GrossKG: first.Weight.WeightKG,
		TareKG: second.Weight.WeightKG,
		Flow: FlowOf(first.Weight.WeightKG, second.Weight.WeightKG),
	}
	// Roles follow the weights: gross is the heavier pass, tare the lighter one.
	if out.Flow == FlowMaterialOut {
		out.GrossKG, out.TareKG = second.Weight.WeightKG, first.Weight.WeightKG
	}
	out.NetKG = out.GrossKG - out.TareKG
	out.Elapsed = second.Weight.ObservedAt.Sub(first.Weight.ObservedAt)

	invalid := func(status CycleStatus, reason string) PairValidation {
		out.Status = status
		out.Reason = reason
		return out
	}

	if first.Number != PassFirst || first.Direction != DirectionAToB {
		return invalid(CyclePairWeightInvalid, "first pass must be A_TO_B pass #1")
	}
	if second.Number != PassSecond || second.Direction != DirectionBToA {
		return invalid(CyclePairWeightInvalid, "second pass must be B_TO_A pass #2")
	}
	if normalizeCycleIdentity(first.Plate) == "" || normalizeCycleIdentity(second.Plate) == "" || normalizeCycleIdentity(first.Plate) != normalizeCycleIdentity(second.Plate) {
		return invalid(CycleWrongTruck, "plate identity does not match first pass")
	}
	if first.RFID != "" && second.RFID != "" && first.RFID != second.RFID {
		return invalid(CycleWrongTruck, "RFID identity does not match first pass")
	}
	if first.Weight.RawRef.Seq == 0 || first.Weight.RawRef.Hash == "" || second.Weight.RawRef.Seq == 0 || second.Weight.RawRef.Hash == "" {
		return invalid(CyclePairWeightInvalid, "both pass weights require immutable raw audit references")
	}
	if out.Elapsed <= 0 {
		return invalid(CyclePairTimeInvalid, "second pass time must be after first pass time")
	}
	if policy.MinElapsed > 0 && out.Elapsed < policy.MinElapsed {
		return invalid(CyclePairTimeInvalid, fmt.Sprintf("pair elapsed %s is below configured minimum %s", out.Elapsed, policy.MinElapsed))
	}
	if policy.MaxElapsed > 0 && out.Elapsed > policy.MaxElapsed {
		return invalid(CyclePairTimeInvalid, fmt.Sprintf("pair elapsed %s exceeds configured maximum %s", out.Elapsed, policy.MaxElapsed))
	}
	if !withinConfigured(out.GrossKG, policy.MinGrossKG, policy.MaxGrossKG) {
		return invalid(CyclePairWeightInvalid, "gross weight outside configured range")
	}
	if !withinConfigured(out.TareKG, policy.MinTareKG, policy.MaxTareKG) {
		return invalid(CyclePairWeightInvalid, "tare weight outside configured range")
	}
	if out.NetKG <= 0 {
		return invalid(CyclePairWeightInvalid, "net weight must be positive: the two pass weights are equal")
	}
	if policy.RequireGrossFirst && out.Flow == FlowMaterialOut {
		return invalid(CyclePairWeightInvalid, "policy requires gross first: second pass is heavier than first")
	}
	if !withinConfigured(out.NetKG, policy.MinNetKG, policy.MaxNetKG) {
		return invalid(CyclePairWeightInvalid, "net weight outside configured range")
	}

	out.Valid = true
	out.Status = CycleComplete
	out.Reason = ""
	return out
}

func ValidateFirstPass(pass WeighPass) error {
	if pass.Number != PassFirst {
		return errors.New("first pass number must be 1")
	}
	if pass.Direction != DirectionAToB {
		return errors.New("first pass direction must be A_TO_B")
	}
	if normalizeCycleIdentity(pass.Plate) == "" {
		return errors.New("first pass requires vehicle plate identity")
	}
	if pass.Weight.RawRef.Seq == 0 || pass.Weight.RawRef.Hash == "" {
		return errors.New("first pass requires raw weight audit reference")
	}
	if pass.Weight.WeightKG <= 0 {
		return errors.New("first pass requires positive accepted weight")
	}
	if pass.Weight.ObservedAt.IsZero() {
		return errors.New("first pass requires observed timestamp")
	}
	return nil
}

func withinConfigured(v, min, max int64) bool {
	if min > 0 && v < min {
		return false
	}
	if max > 0 && v > max {
		return false
	}
	return true
}

func normalizeCycleIdentity(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r-'a'+'A')
		case r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			out = append(out, r)
		}
	}
	return string(out)
}
