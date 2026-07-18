package custodian

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestAttestationChannelsRejectCrossChannelQuiescence(t *testing.T) {
	_, verifierA := NewAttestationChannel()
	issuerB, _ := NewAttestationChannel()
	verified, err := issuerB.AttestQuiescence(testPhysicalQuiescence())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifierA.VerifyQuiescence(verified); !errors.Is(err, ErrInvalidAttestation) {
		t.Fatalf("cross-channel VerifyQuiescence error = %v, want ErrInvalidAttestation", err)
	}
}

func TestUnavailableRuntimeActiveCustodyCountIsZero(t *testing.T) {
	runtime := NewUnavailableRuntime(nil)
	if got := runtime.ActiveCustodyCount(); got != 0 {
		t.Fatalf("NewUnavailableRuntime ActiveCustodyCount() = %d, want 0", got)
	}
	if got := runtime.Process().ActiveCustodyCount(); got != 0 {
		t.Fatalf("UnavailableCustodian ActiveCustodyCount() = %d, want 0", got)
	}
}

func TestPhysicalQuiescenceCarriesOnlyPhysicalFields(t *testing.T) {
	payloadType := reflect.TypeOf(PhysicalQuiescence{})
	if payloadType.NumField() != 2 {
		t.Fatalf("PhysicalQuiescence fields = %d, want 2", payloadType.NumField())
	}
	for _, name := range []string{"Group", "Method"} {
		if _, ok := payloadType.FieldByName(name); !ok {
			t.Fatalf("PhysicalQuiescence missing field %s", name)
		}
	}
	for _, name := range []string{"Attempt", "Ordinal", "CertifiedBy"} {
		if _, ok := payloadType.FieldByName(name); ok {
			t.Fatalf("PhysicalQuiescence carries logical field %s", name)
		}
	}
}

func TestProductionCodeDoesNotMintQuiescenceOutsideCustodian(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	custodianDir := filepath.Dir(thisFile)
	root := filepath.Clean(filepath.Join(custodianDir, "..", "..", ".."))
	disallowed := []string{"AttestQuiescence(", "AttestationIssuer"}
	var offenders []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			name := entry.Name()
			if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "tmp-") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || filepath.Dir(path) == custodianDir {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		for _, needle := range disallowed {
			if strings.Contains(text, needle) {
				offenders = append(offenders, path)
				break
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(offenders) != 0 {
		t.Fatalf("production quiescence minter(s) outside custodian: %s", strings.Join(offenders, ", "))
	}
}

func TestSupportValidationRejectsContradictoryStates(t *testing.T) {
	tests := []struct {
		name    string
		support Support
	}{
		{
			name: "advertised without configured",
			support: supportWith(func(s *Support) {
				s.FeatureConfigured = false
			}),
		},
		{
			name: "configured without probe",
			support: supportWith(func(s *Support) {
				s.RuntimeProbePassed = false
				s.RuntimeProbeResult = errors.New("probe failed")
			}),
		},
		{
			name: "probe without implementation",
			support: supportWith(func(s *Support) {
				s.ImplementationCompiled = false
			}),
		},
		{
			name: "advertised without parked exec",
			support: supportWith(func(s *Support) {
				s.ParkedExec = false
			}),
		},
		{
			name: "advertised without verified containment",
			support: supportWith(func(s *Support) {
				s.VerifiedContainment = false
			}),
		},
		{
			name: "probe passed with failure result",
			support: supportWith(func(s *Support) {
				s.RuntimeProbeResult = errors.New("probe failed")
			}),
		},
		{
			name: "probe failed without result",
			support: Support{
				ParkedExec:             true,
				VerifiedContainment:    true,
				ImplementationCompiled: true,
				RuntimeProbePassed:     false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewSupport(tt.support); !errors.Is(err, ErrInvalidSupport) {
				t.Fatalf("NewSupport() error = %v, want ErrInvalidSupport", err)
			}
		})
	}
}

func TestSupportAdvertisedAvailableRequiresFullChain(t *testing.T) {
	available, err := NewSupport(supportWith(nil))
	if err != nil {
		t.Fatalf("NewSupport() available error = %v", err)
	}
	if !available.AdvertisedAvailable() {
		t.Fatal("AdvertisedAvailable() = false, want true")
	}

	capabilityOnly, err := NewSupport(Support{
		ParkedExec:             true,
		VerifiedContainment:    true,
		ImplementationCompiled: true,
		RuntimeProbePassed:     true,
	})
	if err != nil {
		t.Fatalf("NewSupport() capability-only error = %v", err)
	}
	if capabilityOnly.AdvertisedAvailable() {
		t.Fatal("AdvertisedAvailable() = true without configured/advertised lifecycle")
	}

	unavailable := NewUnavailableRuntime(nil).Support()
	if err := unavailable.Validate(); err != nil {
		t.Fatalf("NewUnavailableRuntime support Validate() error = %v", err)
	}
	if unavailable.AdvertisedAvailable() {
		t.Fatal("NewUnavailableRuntime support advertised available")
	}

	invalidProbeResult := supportWith(func(s *Support) {
		s.RuntimeProbeResult = errors.New("probe failed")
	})
	if invalidProbeResult.AdvertisedAvailable() {
		t.Fatal("AdvertisedAvailable() = true with non-nil runtime probe result")
	}
}

func supportWith(mutate func(*Support)) Support {
	support := Support{
		ParkedExec:             true,
		VerifiedContainment:    true,
		ImplementationCompiled: true,
		RuntimeProbePassed:     true,
		FeatureConfigured:      true,
		FeatureAdvertised:      true,
	}
	if mutate != nil {
		mutate(&support)
	}
	return support
}

func testPhysicalQuiescence() PhysicalQuiescence {
	ref := model.AttemptRef{JobID: "job-custodian", AttemptID: "attempt-custodian", Epoch: 1}
	group := model.GroupRef{
		Version:   1,
		CustodyID: "custody-custodian",
		Launch: model.LaunchKey{
			Attempt: ref,
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID:        "host-boot-custodian",
		PIDNamespaceState: model.PIDNamespaceNotApplicable,
		PGID:              100,
		Leader:            model.ProcessIdentity{PID: 100, HighResStartToken: "leader-start-custodian"},
		Monitor:           model.ProcessIdentity{PID: 102, HighResStartToken: "monitor-start-custodian"},
		RetainedID:        "retained-custodian",
	}
	return PhysicalQuiescence{
		Group:  group,
		Method: model.QuiescenceAlreadyAbsent,
	}
}
