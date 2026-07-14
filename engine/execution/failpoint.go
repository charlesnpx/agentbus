package execution

import "fmt"

type Failpoint string

const (
	FailBeforeCommit                  Failpoint = "before_commit"
	FailAfterCommit                   Failpoint = "after_commit"
	FailBeforeCAS                     Failpoint = "before_cas"
	FailAfterCAS                      Failpoint = "after_cas"
	FailBeforeSideEffect              Failpoint = "before_side_effect"
	FailAfterSideEffect               Failpoint = "after_side_effect"
	FailPostCommitPreRunnable         Failpoint = "post_commit_pre_runnable"
	FailExecDeathBeforeFork           Failpoint = "exec_death_before_fork"
	FailExecDeathAfterForkBeforeExec  Failpoint = "exec_death_after_fork_before_exec"
	FailExecDeathAfterExecBeforeStart Failpoint = "exec_death_after_exec_before_started"
	FailExecDeathAfterStartBeforeCAS  Failpoint = "exec_death_after_started_before_record_started"
)

func AllFailpoints() []Failpoint {
	return []Failpoint{
		FailBeforeCommit,
		FailAfterCommit,
		FailBeforeCAS,
		FailAfterCAS,
		FailBeforeSideEffect,
		FailAfterSideEffect,
		FailPostCommitPreRunnable,
		FailExecDeathBeforeFork,
		FailExecDeathAfterForkBeforeExec,
		FailExecDeathAfterExecBeforeStart,
		FailExecDeathAfterStartBeforeCAS,
	}
}

type InjectedFailure struct {
	Point Failpoint
}

func (e InjectedFailure) Error() string {
	return fmt.Sprintf("injected failure at %s", e.Point)
}

type FailureInjector struct {
	Target Failpoint
	Hit    bool
}

func (f *FailureInjector) Fail(point Failpoint) error {
	if f == nil || f.Hit || f.Target != point {
		return nil
	}
	f.Hit = true
	return InjectedFailure{Point: point}
}
