package main

import "testing"

func TestVersionIsSet(t *testing.T) {
	t.Parallel()

	if version == "" {
		t.Fatal("version must not be empty")
	}
}
