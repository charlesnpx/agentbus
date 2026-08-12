package engine

import "testing"

func TestTimeoutResolutionValidRejectsNegativeEffective(t *testing.T) {
	requested := int64(0)
	if (TimeoutResolution{
		Requested: &requested,
		Effective: 0,
		Source:    TimeoutSourceClient,
	}).Valid() != true {
		t.Fatal("explicit zero timeout resolution is invalid")
	}
	if (TimeoutResolution{
		Requested: &requested,
		Effective: -1,
		Source:    TimeoutSourceClient,
	}).Valid() {
		t.Fatal("negative effective timeout resolution is valid")
	}
}
