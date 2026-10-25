package registry

import "testing"

func TestListIsSortedByPlate(t *testing.T) {
	r, err := Parse("TAG-B=61C-200.00,TAG-A=51D-100.00,TAG-C=60H-300.00")
	if err != nil {
		t.Fatal(err)
	}
	got := r.List()
	if len(got) != 3 || got[0].Plate != "51D-100.00" || got[1].Plate != "60H-300.00" || got[2].Plate != "61C-200.00" {
		t.Fatalf("list = %+v", got)
	}
	if got[0].RFIDTag != "TAG-A" {
		t.Fatalf("tag lost: %+v", got[0])
	}
	empty, _ := Parse("")
	if len(empty.List()) != 0 {
		t.Fatal("empty registry must list nothing")
	}
}
