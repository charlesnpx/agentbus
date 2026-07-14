package coordinator

import "fmt"

type Failpoint string

const (
	FailSupervisorPrepareBefore Failpoint = "supervisor.prepare.before"
	FailSupervisorPrepareAfter  Failpoint = "supervisor.prepare.after"
	FailGrantBeforeCommit       Failpoint = "grant.before_commit"
	FailGrantAfterCommit        Failpoint = "grant.after_commit"
	FailPermitSendBefore        Failpoint = "permit_send.before"
	FailPermitSendAfter         Failpoint = "permit_send.after"
	FailLaunchForked            Failpoint = "launch.forked"
	FailLaunchExeced            Failpoint = "launch.execed"
	FailLaunchQuiescent         Failpoint = "launch.quiescent"
	FailContainSignal           Failpoint = "contain.signal"
	FailContainVerified         Failpoint = "contain.verified"
	FailRetireClose             Failpoint = "retire.close"
	FailRetireFsync             Failpoint = "retire.fsync"
	FailResultTempWrite         Failpoint = "result.temp_write"
	FailResultFsync             Failpoint = "result.fsync"
	FailResultRename            Failpoint = "result.rename"
)

type InjectedFailure struct {
	Point Failpoint
}

func (failure InjectedFailure) Error() string {
	return fmt.Sprintf("injected failure at %s", failure.Point)
}

type FailureInjector struct {
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
