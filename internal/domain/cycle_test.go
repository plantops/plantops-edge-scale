package domain

import (
	"testing"
	"time"
)

func TestValidatePairHappyPath(t *testing.T) {
	t0 := time.Date(2026, 8, 21, 1, 0, 0, 0, time.UTC)
	first := WeighPass{
		Number: PassFirst, Direction: DirectionAToB, Plate: "15C-123.45", RFID: "RFID-1",
		Weight: WeightAcceptance{WeightKG: 28460, ObservedAt: t0, RawRef: RawWeightRef{Seq: 10, Hash: "a"}},
	}
	second := WeighPass{
		Number: PassSecond, Direction: DirectionBToA, Plate: "15C12345", RFID: "RFID-1",
		Weight: WeightAcceptance{WeightKG: 11820, ObservedAt: t0.Add(45 * time.Minute), RawRef: RawWeightRef{Seq: 90, Hash: "b"}},
	}
	got := ValidatePair(first, second, PairPolicy{MaxElapsed: 4 * time.Hour, MinNetKG: 1000})
	if !got.Valid || got.Status != CycleComplete {
		t.Fatalf("expected valid pair, got %+v", got)
	}
	if got.NetKG != 16640 {
		t.Fatalf("expected net 16640, got %d", got.NetKG)
	}
}

func TestValidatePairRejectsWrongDirection(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionBToA, "15C-1", 20000, t0, 1)
	second := passForTest(PassSecond, DirectionAToB, "15C-1", 10000, t0.Add(time.Hour), 2)
	got := ValidatePair(first, second, PairPolicy{})
	if got.Valid {
		t.Fatal("wrong-direction pair must not validate")
	}
}

func TestValidatePairRejectsWrongTruck(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-111.11", 20000, t0, 1)
	second := passForTest(PassSecond, DirectionBToA, "15C-222.22", 10000, t0.Add(time.Hour), 2)
	got := ValidatePair(first, second, PairPolicy{})
	if got.Status != CycleWrongTruck {
		t.Fatalf("expected WRONG_TRUCK, got %+v", got)
	}
}

func TestValidatePairRejectsExpiredPair(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-1", 20000, t0, 1)
	second := passForTest(PassSecond, DirectionBToA, "15C-1", 10000, t0.Add(7*time.Hour), 2)
	got := ValidatePair(first, second, PairPolicy{MaxElapsed: 6 * time.Hour})
	if got.Status != CyclePairTimeInvalid {
		t.Fatalf("expected PAIR_TIME_INVALID, got %+v", got)
	}
}

func TestValidatePairRejectsEqualWeights(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-1", 10000, t0, 1)
	second := passForTest(PassSecond, DirectionBToA, "15C-1", 10000, t0.Add(time.Hour), 2)
	got := ValidatePair(first, second, PairPolicy{})
	if got.Valid || got.Status != CyclePairWeightInvalid {
		t.Fatalf("equal weights carry no net and must be PAIR_WEIGHT_INVALID, got %+v", got)
	}
}

func TestValidatePairGrossFirstIsMaterialIn(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-1", 28460, t0, 1)
	second := passForTest(PassSecond, DirectionBToA, "15C-1", 9300, t0.Add(20*time.Minute), 2)
	got := ValidatePair(first, second, PairPolicy{})
	if !got.Valid || got.Flow != FlowMaterialIn || got.GrossKG != 28460 || got.TareKG != 9300 || got.NetKG != 19160 {
		t.Fatalf("gross-first pair wrong: %+v", got)
	}
}

// Outbound material: the truck crosses empty (tare) on A->B, is loaded at the
// store, and crosses loaded (gross) on B->A. The physical directions stay the
// same; only the weight roles swap.
func TestValidatePairTareFirstIsMaterialOut(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-1", 9100, t0, 1)
	second := passForTest(PassSecond, DirectionBToA, "15C-1", 27000, t0.Add(20*time.Minute), 2)
	got := ValidatePair(first, second, PairPolicy{})
	if !got.Valid || got.Status != CycleComplete || got.Flow != FlowMaterialOut {
		t.Fatalf("tare-first pair must be valid MATERIAL_OUT: %+v", got)
	}
	if got.GrossKG != 27000 || got.TareKG != 9100 || got.NetKG != 17900 {
		t.Fatalf("gross/tare roles must follow the weights, got gross=%d tare=%d net=%d", got.GrossKG, got.TareKG, got.NetKG)
	}
}

func TestValidatePairRequireGrossFirstRestoresLegacyRule(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-1", 9100, t0, 1)
	second := passForTest(PassSecond, DirectionBToA, "15C-1", 27000, t0.Add(20*time.Minute), 2)
	got := ValidatePair(first, second, PairPolicy{RequireGrossFirst: true})
	if got.Valid || got.Status != CyclePairWeightInvalid {
		t.Fatalf("legacy policy must reject tare-first, got %+v", got)
	}
}

// Configured ranges bind the heavier weight as gross and the lighter as tare
// regardless of which pass produced them.
func TestValidatePairRangesFollowRoleNotPassOrder(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-1", 9100, t0, 1)
	second := passForTest(PassSecond, DirectionBToA, "15C-1", 27000, t0.Add(20*time.Minute), 2)
	policy := PairPolicy{MinTareKG: 8000, MaxTareKG: 10000, MinGrossKG: 20000, MaxGrossKG: 40000}
	if got := ValidatePair(first, second, policy); !got.Valid {
		t.Fatalf("tare-first within role ranges must be valid: %+v", got)
	}
	policy.MaxTareKG = 9000 // the 9100 kg first pass is the TARE and now exceeds the limit
	if got := ValidatePair(first, second, policy); got.Valid {
		t.Fatalf("tare range must be applied to the lighter (first) pass: %+v", got)
	}
}

func TestValidatePairRequiresRawRefs(t *testing.T) {
	t0 := time.Now().UTC()
	first := passForTest(PassFirst, DirectionAToB, "15C-1", 20000, t0, 0)
	second := passForTest(PassSecond, DirectionBToA, "15C-1", 10000, t0.Add(time.Hour), 2)
	got := ValidatePair(first, second, PairPolicy{})
	if got.Valid {
		t.Fatal("pair without first raw audit ref must not validate")
	}
}

func passForTest(n PassNumber, d Direction, plate string, weight int64, at time.Time, seq uint64) WeighPass {
	hash := ""
	if seq != 0 {
		hash = "hash"
	}
	return WeighPass{
		Number: n,
		Direction: d,
		Plate: plate,
		Weight: WeightAcceptance{WeightKG: weight, ObservedAt: at, RawRef: RawWeightRef{Seq: seq, Hash: hash}},
	}
}
