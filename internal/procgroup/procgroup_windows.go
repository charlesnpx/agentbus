//go:build windows

package procgroup

import (
	"errors"
	"fmt"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

// ErrPlatformUnsupported reports that this platform cannot prove Unix
// process-group custody. Windows has no process-group primitive with the
// identity and group-wide signal semantics this package requires.
var ErrPlatformUnsupported = errors.New("process-group custody unsupported on this platform")

func windowsProcessGroupUnsupported() error {
	return fmt.Errorf("%w: Windows does not provide Unix process-group identity and containment semantics", ErrPlatformUnsupported)
}

func nativeHostBootID() (string, error) {
	return "", windowsProcessGroupUnsupported()
}

func nativePIDNamespaceID() (string, error) {
	return "", windowsProcessGroupUnsupported()
}

func (nativeKernelReader) CurrentKernelDomain() (model.KernelDomainID, error) {
	return model.KernelDomainID{}, windowsProcessGroupUnsupported()
}

func (nativeKernelReader) ReadProcess(int) (processSnapshot, error) {
	return processSnapshot{}, windowsProcessGroupUnsupported()
}

func (nativeKernelReader) ProcessesInGroup(int) ([]processSnapshot, error) {
	return nil, windowsProcessGroupUnsupported()
}

func (nativeKernelReader) GroupExistenceProbe(int) groupExistenceProbeResult {
	return groupExistenceIndeterminate
}
