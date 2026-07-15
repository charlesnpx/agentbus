package model

import (
	"reflect"
	"testing"
)

func TestSubmissionModesAreExactAndExhaustive(t *testing.T) {
	want := []Mode{ModeIdentifiedFenced, ModeLegacyFenced, ModeLegacyUnfenced}
	got := AllModes()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("AllModes() = %#v, want %#v", got, want)
	}

	seen := map[Mode]struct{}{}
	for _, mode := range got {
		if !mode.Valid() {
			t.Fatalf("mode %v from AllModes is invalid", mode)
		}
		if mode.String() == "" {
			t.Fatalf("mode %v has empty String", mode)
		}
		if _, ok := seen[mode]; ok {
			t.Fatalf("mode %v appears more than once", mode)
		}
		seen[mode] = struct{}{}
	}
	if len(seen) != 3 {
		t.Fatalf("mode count = %d, want 3", len(seen))
	}

	for _, invalid := range []Mode{0, 4} {
		if invalid.Valid() {
			t.Fatalf("mode %d is valid, want invalid", invalid)
		}
		if err := invalid.Validate(); err == nil {
			t.Fatalf("mode %d Validate() succeeded, want error", invalid)
		}
	}
}
