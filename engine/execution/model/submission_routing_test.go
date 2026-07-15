package model

import (
	"errors"
	"testing"
)

func TestRouteSubmissionModeByExecutionCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		caps    ExecutionCapabilities
		want    Mode
		wantErr error
	}{
		{
			name: "built_in_fenced_routes_identified_fenced",
			caps: ExecutionCapabilities{FencedLaunch: true},
			want: ModeIdentifiedFenced,
		},
		{
			name: "external_fenced_routes_legacy_fenced",
			caps: ExecutionCapabilities{ExternalRunner: true, FencedLaunch: true},
			want: ModeLegacyFenced,
		},
		{
			name: "external_unfenced_routes_legacy_unfenced",
			caps: ExecutionCapabilities{ExternalRunner: true},
			want: ModeLegacyUnfenced,
		},
		{
			name:    "built_in_unfenced_rejects_before_acceptance",
			caps:    ExecutionCapabilities{},
			wantErr: ErrIncompatibleExecutionCapabilities,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RouteSubmissionMode(tt.caps)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("error = %v, want %v", err, tt.wantErr)
				}
				var incompatible IncompatibleExecutionCapabilitiesError
				if !errors.As(err, &incompatible) {
					t.Fatalf("error = %T, want IncompatibleExecutionCapabilitiesError", err)
				}
				if incompatible.Capabilities != tt.caps {
					t.Fatalf("error capabilities = %#v, want %#v", incompatible.Capabilities, tt.caps)
				}
				if got != 0 {
					t.Fatalf("mode = %s, want zero mode on rejection", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("RouteSubmissionMode error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("mode = %s, want %s", got, tt.want)
			}
		})
	}
}
