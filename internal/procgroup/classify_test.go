package procgroup

import (
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type fakeKernelReader struct {
	domain     model.KernelDomainID
	domainErr  error
	processes  map[int]processSnapshot
	processErr map[int]error
	groups     map[int][]processSnapshot
	groupErr   map[int]error
	groupProbe map[int]groupExistenceProbeResult
	probeFunc  func(int) groupExistenceProbeResult
	probeCalls map[int]int
}

func (reader fakeKernelReader) CurrentKernelDomain() (model.KernelDomainID, error) {
	if reader.domainErr != nil {
		return model.KernelDomainID{}, reader.domainErr
	}
	return reader.domain, nil
}

func (reader fakeKernelReader) ReadProcess(pid int) (processSnapshot, error) {
	if err := reader.processErr[pid]; err != nil {
		return processSnapshot{}, err
	}
	snapshot, ok := reader.processes[pid]
	if !ok {
		return processSnapshot{}, ErrProcessMissing
	}
	return snapshot, nil
}

func (reader fakeKernelReader) ProcessesInGroup(pgid int) ([]processSnapshot, error) {
	if err := reader.groupErr[pgid]; err != nil {
		return nil, err
	}
	return reader.groups[pgid], nil
}

func (reader fakeKernelReader) GroupExistenceProbe(pgid int) groupExistenceProbeResult {
	if reader.probeCalls != nil {
		reader.probeCalls[pgid]++
	}
	if reader.probeFunc != nil {
		return reader.probeFunc(pgid)
	}
	if result, ok := reader.groupProbe[pgid]; ok {
		return result
	}
	return groupExistenceIndeterminate
}

func TestClassifyProcessWithFakeReader(t *testing.T) {
	domain := testDomain(t, "boot-process")
	base := ProcessClaim{PID: 10, PGID: 20, StartToken: "start-10", KernelDomainID: domain}
	baseReader := fakeKernelReader{
		domain:    domain,
		processes: map[int]processSnapshot{10: {PID: 10, PGID: 20, StartToken: "start-10"}},
	}
	readErr := errors.New("permission denied")

	tests := []struct {
		name     string
		reader   fakeKernelReader
		expected ProcessClaim
		want     model.ProcessIdentityObservation
	}{
		{
			name:     "matching",
			reader:   baseReader,
			expected: base,
			want:     model.ProcessIdentityMatching,
		},
		{
			name: "reused when start token differs",
			reader: fakeKernelReader{
				domain:    domain,
				processes: map[int]processSnapshot{10: {PID: 10, PGID: 20, StartToken: "other-start"}},
			},
			expected: base,
			want:     model.ProcessIdentityReused,
		},
		{
			name: "reused when pgid differs",
			reader: fakeKernelReader{
				domain:    domain,
				processes: map[int]processSnapshot{10: {PID: 10, PGID: 21, StartToken: "start-10"}},
			},
			expected: base,
			want:     model.ProcessIdentityReused,
		},
		{
			name: "missing",
			reader: fakeKernelReader{
				domain:    domain,
				processes: map[int]processSnapshot{},
			},
			expected: base,
			want:     model.ProcessIdentityMissing,
		},
		{
			name: "read error is unknown",
			reader: fakeKernelReader{
				domain:     domain,
				processErr: map[int]error{10: readErr},
			},
			expected: base,
			want:     model.ProcessIdentityUnknown,
		},
		{
			name: "ambiguous domain is unknown",
			reader: fakeKernelReader{
				domain: model.KernelDomainID{HostBootID: domain.HostBootID},
				processes: map[int]processSnapshot{
					10: {PID: 10, PGID: 20, StartToken: "start-10"},
				},
			},
			expected: base,
			want:     model.ProcessIdentityUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyProcess(tt.reader, tt.expected); got != tt.want {
				t.Fatalf("classifyProcess() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestObserveProcessSurfacesRunStateWithoutChangingIdentity(t *testing.T) {
	domain := testDomain(t, "boot-process-state")
	expected := ProcessClaim{PID: 10, PGID: 20, StartToken: "start-10", KernelDomainID: domain}
	reader := fakeKernelReader{
		domain: domain,
		processes: map[int]processSnapshot{
			10: {PID: 10, PGID: 20, StartToken: "start-10", RunState: ProcessRunStateZombie},
		},
	}

	observation := observeProcess(reader, expected)
	if observation.Identity != model.ProcessIdentityMatching {
		t.Fatalf("observeProcess().Identity = %v, want %v", observation.Identity, model.ProcessIdentityMatching)
	}
	if observation.RunState != ProcessRunStateZombie {
		t.Fatalf("observeProcess().RunState = %v, want %v", observation.RunState, ProcessRunStateZombie)
	}
	if got := classifyProcess(reader, expected); got != model.ProcessIdentityMatching {
		t.Fatalf("classifyProcess() = %v, want %v", got, model.ProcessIdentityMatching)
	}
}

func TestClassifyGroupWithFakeReader(t *testing.T) {
	domain := testDomain(t, "boot-group")
	base := GroupClaim{PGID: 20, KernelDomainID: domain}
	readErr := errors.New("permission denied")

	tests := []struct {
		name     string
		reader   fakeKernelReader
		expected GroupClaim
		want     model.GroupExistenceObservation
	}{
		{
			name: "live does not require probe",
			reader: fakeKernelReader{
				domain: domain,
				groups: map[int][]processSnapshot{
					20: {{PID: 10, PGID: 20, StartToken: "start-10"}},
				},
				probeCalls: map[int]int{},
			},
			expected: base,
			want:     model.GroupLive,
		},
		{
			name: "empty group is absent only after definite absence probe",
			reader: fakeKernelReader{
				domain:     domain,
				groups:     map[int][]processSnapshot{20: nil},
				groupProbe: map[int]groupExistenceProbeResult{20: groupExistenceDefinitelyAbsent},
			},
			expected: base,
			want:     model.GroupAbsent,
		},
		{
			name: "empty group with existing invisible member is unknown",
			reader: fakeKernelReader{
				domain:     domain,
				groups:     map[int][]processSnapshot{20: nil},
				groupProbe: map[int]groupExistenceProbeResult{20: groupExistenceExists},
			},
			expected: base,
			want:     model.GroupExistenceUnknown,
		},
		{
			name: "empty group with indeterminate probe is unknown",
			reader: fakeKernelReader{
				domain:     domain,
				groups:     map[int][]processSnapshot{20: nil},
				groupProbe: map[int]groupExistenceProbeResult{20: groupExistenceIndeterminate},
			},
			expected: base,
			want:     model.GroupExistenceUnknown,
		},
		{
			name: "read error is unknown",
			reader: fakeKernelReader{
				domain:   domain,
				groupErr: map[int]error{20: readErr},
			},
			expected: base,
			want:     model.GroupExistenceUnknown,
		},
		{
			name: "contradictory wrong pgid",
			reader: fakeKernelReader{
				domain: domain,
				groups: map[int][]processSnapshot{
					20: {{PID: 10, PGID: 21, StartToken: "start-10"}},
				},
			},
			expected: base,
			want:     model.GroupExistenceContradictory,
		},
		{
			name: "contradictory duplicate pid",
			reader: fakeKernelReader{
				domain: domain,
				groups: map[int][]processSnapshot{
					20: {
						{PID: 10, PGID: 20, StartToken: "start-10"},
						{PID: 10, PGID: 20, StartToken: "other-start"},
					},
				},
			},
			expected: base,
			want:     model.GroupExistenceContradictory,
		},
		{
			name: "different domain is absent",
			reader: fakeKernelReader{
				domain: testDomain(t, "other-boot"),
				groups: map[int][]processSnapshot{
					20: {{PID: 10, PGID: 20, StartToken: "start-10"}},
				},
			},
			expected: base,
			want:     model.GroupAbsent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyGroup(tt.reader, tt.expected); got != tt.want {
				t.Fatalf("classifyGroup() = %v, want %v", got, tt.want)
			}
			if tt.name == "live does not require probe" && tt.reader.probeCalls[20] != 0 {
				t.Fatalf("GroupExistenceProbe(%d) called %d times, want 0", 20, tt.reader.probeCalls[20])
			}
		})
	}
}

func TestClassifyGroupPGIDOneEmptyMembershipIsUnknown(t *testing.T) {
	domain := testDomain(t, "boot-group-pgid-one")
	reader := fakeKernelReader{
		domain: domain,
		groups: map[int][]processSnapshot{1: nil},
		probeFunc: func(pgid int) groupExistenceProbeResult {
			return probeProcessGroupExistence(pgid, func(pid int, signal syscall.Signal) error {
				t.Fatalf("kill(%d, %d) should not be called for pgid 1", pid, signal)
				return nil
			})
		},
	}
	expected := GroupClaim{PGID: 1, KernelDomainID: domain}

	if got := classifyGroup(reader, expected); got != model.GroupExistenceUnknown {
		t.Fatalf("classifyGroup(pgid 1 empty membership) = %v, want %v", got, model.GroupExistenceUnknown)
	}
}

func TestParseLinuxProcStat(t *testing.T) {
	stat := "1234 (cmd with ) paren) S 1 2345 2345 0 -1 4194304 1 2 3 4 5 6 7 8 20 0 1 0 987654321 0 0"
	snapshot, err := parseLinuxProcStat(stat)
	if err != nil {
		t.Fatalf("parseLinuxProcStat() error = %v", err)
	}
	if snapshot.PID != 1234 {
		t.Fatalf("snapshot PID = %d, want 1234", snapshot.PID)
	}
	if snapshot.PGID != 2345 {
		t.Fatalf("snapshot PGID = %d, want 2345", snapshot.PGID)
	}
	if snapshot.StartToken != "987654321" {
		t.Fatalf("snapshot StartToken = %q, want 987654321", snapshot.StartToken)
	}
	if snapshot.RunState != ProcessRunStateRunning {
		t.Fatalf("snapshot RunState = %q, want %q", snapshot.RunState, ProcessRunStateRunning)
	}
	zombie, err := parseLinuxProcStat(strings.Replace(stat, ") S ", ") Z ", 1))
	if err != nil {
		t.Fatalf("parseLinuxProcStat(zombie) error = %v", err)
	}
	if zombie.RunState != ProcessRunStateZombie {
		t.Fatalf("zombie RunState = %q, want %q", zombie.RunState, ProcessRunStateZombie)
	}
}

func testDomain(t *testing.T, hostBootID string) model.KernelDomainID {
	t.Helper()
	domain, err := model.NewKernelDomainIDWithoutPIDNamespace(hostBootID)
	if err != nil {
		t.Fatalf("NewKernelDomainIDWithoutPIDNamespace() error = %v", err)
	}
	return domain
}
