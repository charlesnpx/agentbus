package launch

import (
	"fmt"
	"sync"
)

type Failpoint string

const (
	FailAfterPrepare          Failpoint = "launch.prepare.after"
	FailAfterBindGroup        Failpoint = "launch.bind_group.after"
	FailAfterAllocateGrant    Failpoint = "launch.allocate_grant.after"
	FailAfterRelease          Failpoint = "launch.release.after"
	FailAfterRecordRelease    Failpoint = "launch.record_release.after"
	FailAfterWait             Failpoint = "launch.wait.after"
	FailAfterRecordQuiescence Failpoint = "launch.record_quiescence.after"
)

type InjectedFailure struct {
	Point Failpoint
}

func (failure InjectedFailure) Error() string {
	return fmt.Sprintf("injected failure at %s", failure.Point)
}

type FailureInjector struct {
	mu          sync.Mutex
	Target      Failpoint
	Script      []Failpoint
	ScriptIndex int
	Hit         bool
	Hits        map[Failpoint]int
}

func (injector *FailureInjector) Fail(point Failpoint) error {
	if injector == nil {
		return nil
	}
	injector.mu.Lock()
	defer injector.mu.Unlock()
	if injector.Hits == nil {
		injector.Hits = map[Failpoint]int{}
	}
	injector.Hits[point]++
	if len(injector.Script) != 0 {
		if injector.ScriptIndex >= len(injector.Script) || injector.Script[injector.ScriptIndex] != point {
			return nil
		}
		injector.ScriptIndex++
		injector.Hit = true
		return InjectedFailure{Point: point}
	}
	if injector.Hit || injector.Target != point {
		return nil
	}
	injector.Hit = true
	return InjectedFailure{Point: point}
}

func inject(injector *FailureInjector, point Failpoint) error {
	if injector == nil {
		return nil
	}
	return injector.Fail(point)
}
