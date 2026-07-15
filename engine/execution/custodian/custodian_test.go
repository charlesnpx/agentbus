package custodian

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func TestAttestationChannelsRejectCrossChannelQuiescence(t *testing.T) {
	_, verifierA := NewAttestationChannel()
	issuerB, _ := NewAttestationChannel()
	verified, err := issuerB.AttestQuiescence(testQuiescenceCertificate())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifierA.VerifyQuiescence(verified); !errors.Is(err, ErrInvalidAttestation) {
		t.Fatalf("cross-channel VerifyQuiescence error = %v, want ErrInvalidAttestation", err)
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

func testQuiescenceCertificate() model.QuiescenceCertificate {
	ref := model.AttemptRef{JobID: "job-custodian", AttemptID: "attempt-custodian", Epoch: 1}
	group := model.GroupRef{
		Version:   1,
		CustodyID: "custody-custodian",
		Launch: model.LaunchKey{
			Attempt: ref,
			Ordinal: model.LaunchOrdinalOne,
		},
		HostBootID: "host-boot-custodian",
		PGID:       100,
		Leader:     model.ProcessIdentity{PID: 101, HighResStartToken: "leader-start-custodian"},
		Monitor:    model.ProcessIdentity{PID: 102, HighResStartToken: "monitor-start-custodian"},
		RetainedID: "retained-custodian",
	}
	return model.QuiescenceCertificate{
		Attempt:     ref,
		Ordinal:     model.LaunchOrdinalOne,
		Group:       group,
		Method:      model.QuiescenceAlreadyAbsent,
		CertifiedBy: model.BootRef{BootID: "boot-custodian", OwnerID: "owner-custodian"},
	}
}
