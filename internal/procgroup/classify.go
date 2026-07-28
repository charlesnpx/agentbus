package procgroup

import (
	"errors"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

type kernelReader interface {
	CurrentKernelDomain() (model.KernelDomainID, error)
	ReadProcess(pid int) (processSnapshot, error)
	ProcessesInGroup(pgid int) ([]processSnapshot, error)
	GroupExistenceProbe(pgid int) groupExistenceProbeResult
}

type nativeKernelReader struct{}

// HostBootID returns a stable token for the current host boot.
func HostBootID() (string, error) {
	return nativeHostBootID()
}

// PIDNamespaceID returns the current PID namespace identity on platforms that
// expose one. Platforms without PID namespaces return an empty id and nil error.
func PIDNamespaceID() (string, error) {
	return nativePIDNamespaceID()
}

// CurrentKernelDomain returns the current host boot and PID namespace domain.
func CurrentKernelDomain() (model.KernelDomainID, error) {
	return nativeKernelReader{}.CurrentKernelDomain()
}

// ReadProcessStartToken reads the process start token directly from the kernel.
func ReadProcessStartToken(pid int) (StartToken, error) {
	snapshot, err := nativeKernelReader{}.ReadProcess(pid)
	if err != nil {
		return "", err
	}
	return snapshot.StartToken, nil
}

// ReadProcessPGID reads the process group id directly from the kernel.
func ReadProcessPGID(pid int) (int, error) {
	snapshot, err := nativeKernelReader{}.ReadProcess(pid)
	if err != nil {
		return 0, err
	}
	return snapshot.PGID, nil
}

// ReadProcessGroupID reads the process group id directly from the kernel.
func ReadProcessGroupID(pid int) (int, error) {
	return ReadProcessPGID(pid)
}

// ReadProcessClaim reads the current kernel domain and one process identity.
// It is intended for tests and callers that need to create a durable claim from
// an already-live process.
func ReadProcessClaim(pid int) (ProcessClaim, error) {
	domain, err := CurrentKernelDomain()
	if err != nil {
		return ProcessClaim{}, errors.Join(errors.New("read current kernel domain"), err)
	}
	snapshot, err := nativeKernelReader{}.ReadProcess(pid)
	if err != nil {
		return ProcessClaim{}, errors.Join(errors.New("read process identity"), err)
	}
	return NewProcessClaim(snapshot.PID, snapshot.PGID, snapshot.StartToken, domain)
}

// ClassifyProcess re-reads the kernel and maps the result to the model
// observation enum. Any uncertain read maps to ProcessIdentityUnknown.
func ClassifyProcess(expected ProcessClaim) model.ProcessIdentityObservation {
	return ObserveProcess(expected).Identity
}

// ObserveProcess re-reads the kernel and returns both the legacy identity
// classification and the observed process run state when the process table entry
// can be read.
func ObserveProcess(expected ProcessClaim) ProcessObservation {
	return observeProcess(nativeKernelReader{}, expected)
}

func classifyProcess(reader kernelReader, expected ProcessClaim) model.ProcessIdentityObservation {
	return observeProcess(reader, expected).Identity
}

func observeProcess(reader kernelReader, expected ProcessClaim) ProcessObservation {
	if err := expected.validate(); err != nil {
		return processObservation(model.ProcessIdentityUnknown, ProcessRunStateUnknown)
	}
	currentDomain, err := reader.CurrentKernelDomain()
	if err != nil {
		return processObservation(model.ProcessIdentityUnknown, ProcessRunStateUnknown)
	}
	relation, err := compareKernelDomain(expected.KernelDomainID, currentDomain)
	if err != nil {
		return processObservation(model.ProcessIdentityUnknown, ProcessRunStateUnknown)
	}
	snapshot, err := reader.ReadProcess(expected.PID)
	if errors.Is(err, ErrProcessMissing) {
		return processObservation(model.ProcessIdentityMissing, ProcessRunStateUnknown)
	}
	if err != nil {
		return processObservation(model.ProcessIdentityUnknown, ProcessRunStateUnknown)
	}
	if err := snapshot.validate(); err != nil {
		return processObservation(model.ProcessIdentityUnknown, ProcessRunStateUnknown)
	}
	runState := snapshot.RunState.known()
	if snapshot.PID != expected.PID {
		return processObservation(model.ProcessIdentityUnknown, runState)
	}
	if relation == kernelDomainUnprovable {
		return processObservation(model.ProcessIdentityUnknown, runState)
	}
	if relation == kernelDomainDifferent {
		return processObservation(model.ProcessIdentityReused, runState)
	}
	if snapshot.StartToken != expected.StartToken {
		return processObservation(model.ProcessIdentityReused, runState)
	}
	if snapshot.PGID != expected.PGID {
		return processObservation(model.ProcessIdentityReused, runState)
	}
	return processObservation(model.ProcessIdentityMatching, runState)
}

func processObservation(identity model.ProcessIdentityObservation, runState ProcessRunState) ProcessObservation {
	observation := ProcessObservation{Identity: identity, RunState: runState.known()}
	if err := observation.validate(); err != nil {
		return ProcessObservation{Identity: model.ProcessIdentityUnknown, RunState: ProcessRunStateUnknown}
	}
	return observation
}

// ClassifyGroup re-reads the kernel and maps the process-group result to the
// model observation enum. Any uncertain read maps to GroupExistenceUnknown.
func ClassifyGroup(expected GroupClaim) model.GroupExistenceObservation {
	return classifyGroup(nativeKernelReader{}, expected)
}

func classifyGroup(reader kernelReader, expected GroupClaim) model.GroupExistenceObservation {
	if err := expected.validate(); err != nil {
		return model.GroupExistenceUnknown
	}
	currentDomain, err := reader.CurrentKernelDomain()
	if err != nil {
		return model.GroupExistenceUnknown
	}
	relation, err := compareKernelDomain(expected.KernelDomainID, currentDomain)
	if err != nil {
		return model.GroupExistenceUnknown
	}
	if relation == kernelDomainUnprovable {
		return model.GroupExistenceUnknown
	}
	if relation == kernelDomainDifferent {
		return model.GroupAbsent
	}
	members, err := reader.ProcessesInGroup(expected.PGID)
	if err != nil {
		return model.GroupExistenceUnknown
	}
	if len(members) == 0 {
		if reader.GroupExistenceProbe(expected.PGID) == groupExistenceDefinitelyAbsent {
			return model.GroupAbsent
		}
		return model.GroupExistenceUnknown
	}
	seen := make(map[int]processSnapshot, len(members))
	for _, member := range members {
		if err := member.validate(); err != nil {
			return model.GroupExistenceUnknown
		}
		if member.PGID != expected.PGID {
			return model.GroupExistenceContradictory
		}
		if previous, ok := seen[member.PID]; ok && previous != member {
			return model.GroupExistenceContradictory
		}
		seen[member.PID] = member
	}
	return model.GroupLive
}
