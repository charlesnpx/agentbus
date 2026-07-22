package protocol

import "testing"

func TestVersionAndStrictRejectionCauseConstants(t *testing.T) {
	t.Parallel()

	if Version != 2 {
		t.Fatalf("Version = %d, want 2", Version)
	}

	for name, tt := range map[string]struct {
		got  string
		want string
	}{
		"AdmissionRejectMissingIdentity":               {got: AdmissionRejectMissingIdentity, want: "missing_identity"},
		"AdmissionRejectReplayConflict":                {got: AdmissionRejectReplayConflict, want: "replay_conflict"},
		"AdmissionRejectRequestExpired":                {got: AdmissionRejectRequestExpired, want: "request_expired"},
		"AdmissionRejectRequestFingerprintUnsupported": {got: AdmissionRejectRequestFingerprintUnsupported, want: "request_fingerprint_unsupported"},
		"AdmissionRejectUnsupportedBackend":            {got: AdmissionRejectUnsupportedBackend, want: "unsupported_backend"},
		"AdmissionRejectUnfenceableBackend":            {got: AdmissionRejectUnfenceableBackend, want: "unfenceable_backend"},
		"AdmissionRejectInvalidStrictConfig":           {got: AdmissionRejectInvalidStrictConfig, want: "invalid_strict_config"},
		"AdmissionRejectUnavailableNativeRuntime":      {got: AdmissionRejectUnavailableNativeRuntime, want: "unavailable_native_runtime"},
		"AdmissionRejectRootCorrupt":                   {got: AdmissionRejectRootCorrupt, want: "root_corrupt"},
		"AdmissionRejectRootIdentityMismatch":          {got: AdmissionRejectRootIdentityMismatch, want: "root_identity_mismatch"},
		"AdmissionRejectRootFailStopped":               {got: AdmissionRejectRootFailStopped, want: "root_fail_stopped"},
		"AdmissionRejectRootSealed":                    {got: AdmissionRejectRootSealed, want: "root_sealed"},
		"AdmissionRejectAdmissionClosing":              {got: AdmissionRejectAdmissionClosing, want: "admission_closing"},
	} {
		if tt.got != tt.want {
			t.Fatalf("%s = %q, want %q", name, tt.got, tt.want)
		}
	}
}
