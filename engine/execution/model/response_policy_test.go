package model

import "testing"

func TestOnResponseOutcomeIsModeSpecific(t *testing.T) {
	tests := []struct {
		name      string
		mode      Mode
		delivered bool
		want      ResponseAction
	}{
		{
			name:      "identified_fenced_delivered_runs_obligation",
			mode:      ModeIdentifiedFenced,
			delivered: true,
			want:      RunAcceptedObligation,
		},
		{
			name:      "identified_fenced_failed_retains_for_replay",
			mode:      ModeIdentifiedFenced,
			delivered: false,
			want:      RetainObligationForReplay,
		},
		{
			name:      "legacy_fenced_delivered_acknowledges_and_grants",
			mode:      ModeLegacyFenced,
			delivered: true,
			want:      AcknowledgeGrantAndRelease,
		},
		{
			name:      "legacy_fenced_failed_rejects_without_grant",
			mode:      ModeLegacyFenced,
			delivered: false,
			want:      RejectAndRetireNoGrant,
		},
		{
			name:      "legacy_unfenced_delivered_runs_legacy",
			mode:      ModeLegacyUnfenced,
			delivered: true,
			want:      RunLegacyUnfenced,
		},
		{
			name:      "legacy_unfenced_failed_rejects_before_run",
			mode:      ModeLegacyUnfenced,
			delivered: false,
			want:      RejectLegacyUnfencedBeforeRun,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := OnResponseOutcome(tt.mode, tt.delivered)
			if got != tt.want {
				t.Fatalf("action = %s, want %s", got, tt.want)
			}
			if tt.mode == ModeIdentifiedFenced && !tt.delivered && got == CancelAccepted {
				t.Fatal("identified fenced response failure canceled accepted obligation")
			}
		})
	}
}

func TestOnResponseOutcomeRejectsUnknownMode(t *testing.T) {
	if got := OnResponseOutcome(Mode(99), true); got != ResponseActionInvalid {
		t.Fatalf("action = %s, want invalid", got)
	}
}
