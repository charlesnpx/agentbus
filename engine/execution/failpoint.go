package execution

import "fmt"

type Failpoint string

const (
	FailAdmissionBeforeCommit         Failpoint = "admission.before_commit"
	FailAdmissionAfterCommit          Failpoint = "admission.after_commit"
	FailAdmissionCommittedMark        Failpoint = "admission.committed_mark"
	FailPostCommitPreRunnable         Failpoint = "admission.post_commit_pre_runnable"
	FailAcknowledgeBeforeCAS          Failpoint = "acknowledge.before_cas"
	FailAcknowledgeAfterCAS           Failpoint = "acknowledge.after_cas"
	FailRejectBeforeCAS               Failpoint = "reject.before_cas"
	FailRejectAfterCAS                Failpoint = "reject.after_cas"
	FailSupervisorPrepareBefore       Failpoint = "supervisor_prepare.before_side_effect"
	FailSupervisorPrepareAfter        Failpoint = "supervisor_prepare.after_side_effect"
	FailSupervisorRecordBeforeCAS     Failpoint = "supervisor_record.before_cas"
	FailSupervisorRecordAfterCAS      Failpoint = "supervisor_record.after_cas"
	FailLegacySupervisorPrepareBefore Failpoint = "legacy_supervisor_prepare.before_side_effect"
	FailLegacySupervisorPrepareAfter  Failpoint = "legacy_supervisor_prepare.after_side_effect"
	FailCancelBeforeCAS               Failpoint = "cancel.before_cas"
	FailCancelAfterCAS                Failpoint = "cancel.after_cas"
	FailGrantPermitBeforeCAS          Failpoint = "permit_grant.before_cas"
	FailGrantPermitAfterCAS           Failpoint = "permit_grant.after_cas"
	FailPermitSendBeforeSideEffect    Failpoint = "permit_send.before_side_effect"
	FailPermitSendAfterSideEffect     Failpoint = "permit_send.after_side_effect"
	FailLegacyUnfencedPrepareBefore   Failpoint = "legacy_unfenced_prepare.before_side_effect"
	FailLegacyUnfencedPrepareAfter    Failpoint = "legacy_unfenced_prepare.after_side_effect"
	FailLegacyUnfencedStartBefore     Failpoint = "legacy_unfenced_start.before_side_effect"
	FailLegacyUnfencedStartAfter      Failpoint = "legacy_unfenced_start.after_side_effect"
	FailPermitMaybeSentBeforeCAS      Failpoint = "permit_maybe_sent.before_cas"
	FailPermitMaybeSentAfterCAS       Failpoint = "permit_maybe_sent.after_cas"
	FailExecDeathBeforeFork           Failpoint = "exec_death.before_fork"
	FailExecForkBeforeCAS             Failpoint = "exec_fork.before_cas"
	FailExecForkAfterCAS              Failpoint = "exec_fork.after_cas"
	FailExecDeathAfterForkBeforeExec  Failpoint = "exec_death.after_fork_before_exec"
	FailExecBeforeCAS                 Failpoint = "exec.before_cas"
	FailExecAfterCAS                  Failpoint = "exec.after_cas"
	FailExecDeathAfterExecBeforeStart Failpoint = "exec_death.after_exec_before_backend_started"
	FailBackendStartedBeforeCAS       Failpoint = "backend_started.before_cas"
	FailBackendStartedAfterCAS        Failpoint = "backend_started.after_cas"
	FailExecDeathAfterStartBeforeCAS  Failpoint = "exec_death.after_backend_started_before_record_started"
	FailRecordStartedBeforeCAS        Failpoint = "record_started.before_cas"
	FailRecordStartedAfterCAS         Failpoint = "record_started.after_cas"
	FailLaunchExitBeforeCAS           Failpoint = "launch_exit.before_cas"
	FailLaunchExitAfterCAS            Failpoint = "launch_exit.after_cas"
	FailLaunchQuiescentBeforeCAS      Failpoint = "launch_quiescent.before_cas"
	FailLaunchQuiescentAfterCAS       Failpoint = "launch_quiescent.after_cas"
	FailReconciliationBeforeCAS       Failpoint = "reconciliation.before_cas"
	FailReconciliationAfterCAS        Failpoint = "reconciliation.after_cas"
	FailContainmentSignalBefore       Failpoint = "containment_signal.before_side_effect"
	FailContainmentSignalAfter        Failpoint = "containment_signal.after_side_effect"
	FailContainmentSignalBeforeCAS    Failpoint = "containment_signal.before_cas"
	FailContainmentSignalAfterCAS     Failpoint = "containment_signal.after_cas"
	FailContainmentVerifyBefore       Failpoint = "containment_verify.before_side_effect"
	FailContainmentVerifyAfter        Failpoint = "containment_verify.after_side_effect"
	FailContainmentVerifyBeforeCAS    Failpoint = "containment_verify.before_cas"
	FailContainmentVerifyAfterCAS     Failpoint = "containment_verify.after_cas"
	FailContainmentRecordBeforeCAS    Failpoint = "containment_record.before_cas"
	FailContainmentRecordAfterCAS     Failpoint = "containment_record.after_cas"
	FailRetirementCloseBefore         Failpoint = "retirement_close.before_side_effect"
	FailRetirementCloseAfter          Failpoint = "retirement_close.after_side_effect"
	FailRetirementStartedBeforeCAS    Failpoint = "retirement_started.before_cas"
	FailRetirementStartedAfterCAS     Failpoint = "retirement_started.after_cas"
	FailRetirementWaitBefore          Failpoint = "retirement_wait.before_side_effect"
	FailRetirementWaitAfter           Failpoint = "retirement_wait.after_side_effect"
	FailRetirementWorkerBeforeCAS     Failpoint = "retirement_worker_exited.before_cas"
	FailRetirementWorkerAfterCAS      Failpoint = "retirement_worker_exited.after_cas"
	FailRetirementVerifyBefore        Failpoint = "retirement_verify.before_side_effect"
	FailRetirementVerifyAfter         Failpoint = "retirement_verify.after_side_effect"
	FailRetirementRecordBeforeCAS     Failpoint = "retirement_record.before_cas"
	FailRetirementRecordAfterCAS      Failpoint = "retirement_record.after_cas"
	FailOutcomeBeforeCAS              Failpoint = "outcome.before_cas"
	FailOutcomeAfterCAS               Failpoint = "outcome.after_cas"
	FailResultPublicationBeforeCAS    Failpoint = "result_publication.before_cas"
	FailResultPublicationAfterCAS     Failpoint = "result_publication.after_cas"
	FailResultTempWriteBefore         Failpoint = "result_temp_write.before_side_effect"
	FailResultTempWriteAfter          Failpoint = "result_temp_write.after_side_effect"
	FailResultFsyncTempBefore         Failpoint = "result_fsync_temp.before_side_effect"
	FailResultFsyncTempAfter          Failpoint = "result_fsync_temp.after_side_effect"
	FailResultCloseBefore             Failpoint = "result_close.before_side_effect"
	FailResultCloseAfter              Failpoint = "result_close.after_side_effect"
	FailResultRenameBefore            Failpoint = "result_rename.before_side_effect"
	FailResultRenameAfter             Failpoint = "result_rename.after_side_effect"
	FailResultDirFsyncBefore          Failpoint = "result_dir_fsync.before_side_effect"
	FailResultDirFsyncAfter           Failpoint = "result_dir_fsync.after_side_effect"
	FailTerminalBeforeCAS             Failpoint = "terminal.before_cas"
	FailTerminalAfterCAS              Failpoint = "terminal.after_cas"
	FailExpireBeforeCAS               Failpoint = "expire.before_cas"
	FailExpireAfterCAS                Failpoint = "expire.after_cas"
	FailCorruptBeforeCAS              Failpoint = "corrupt.before_cas"
	FailCorruptAfterCAS               Failpoint = "corrupt.after_cas"
	FailAnchorTempDBBefore            Failpoint = "anchor_temp_db.before_side_effect"
	FailAnchorTempDBAfter             Failpoint = "anchor_temp_db.after_side_effect"
	FailAnchorDBFsyncBefore           Failpoint = "anchor_db_fsync.before_side_effect"
	FailAnchorDBFsyncAfter            Failpoint = "anchor_db_fsync.after_side_effect"
	FailAnchorRenameBefore            Failpoint = "anchor_rename.before_side_effect"
	FailAnchorRenameAfter             Failpoint = "anchor_rename.after_side_effect"
	FailAnchorDirFsyncBefore          Failpoint = "anchor_dir_fsync.before_side_effect"
	FailAnchorDirFsyncAfter           Failpoint = "anchor_dir_fsync.after_side_effect"
	FailAnchorPublishBefore           Failpoint = "anchor_publish.before_side_effect"
	FailAnchorPublishAfter            Failpoint = "anchor_publish.after_side_effect"
	FailAnchorPublishDirFsyncBefore   Failpoint = "anchor_publish_dir_fsync.before_side_effect"
	FailAnchorPublishDirFsyncAfter    Failpoint = "anchor_publish_dir_fsync.after_side_effect"

	FailBeforeCommit     = FailAdmissionBeforeCommit
	FailAfterCommit      = FailAdmissionAfterCommit
	FailBeforeCAS        = FailGrantPermitBeforeCAS
	FailAfterCAS         = FailRecordStartedAfterCAS
	FailBeforeSideEffect = FailPermitSendBeforeSideEffect
	FailAfterSideEffect  = FailPermitSendAfterSideEffect
)

func AllFailpoints() []Failpoint {
	return []Failpoint{
		FailAdmissionBeforeCommit,
		FailAdmissionAfterCommit,
		FailAdmissionCommittedMark,
		FailPostCommitPreRunnable,
		FailAcknowledgeBeforeCAS,
		FailAcknowledgeAfterCAS,
		FailRejectBeforeCAS,
		FailRejectAfterCAS,
		FailSupervisorPrepareBefore,
		FailSupervisorPrepareAfter,
		FailSupervisorRecordBeforeCAS,
		FailSupervisorRecordAfterCAS,
		FailLegacySupervisorPrepareBefore,
		FailLegacySupervisorPrepareAfter,
		FailCancelBeforeCAS,
		FailCancelAfterCAS,
		FailGrantPermitBeforeCAS,
		FailGrantPermitAfterCAS,
		FailPermitSendBeforeSideEffect,
		FailPermitSendAfterSideEffect,
		FailLegacyUnfencedPrepareBefore,
		FailLegacyUnfencedPrepareAfter,
		FailLegacyUnfencedStartBefore,
		FailLegacyUnfencedStartAfter,
		FailPermitMaybeSentBeforeCAS,
		FailPermitMaybeSentAfterCAS,
		FailExecDeathBeforeFork,
		FailExecForkBeforeCAS,
		FailExecForkAfterCAS,
		FailExecDeathAfterForkBeforeExec,
		FailExecBeforeCAS,
		FailExecAfterCAS,
		FailExecDeathAfterExecBeforeStart,
		FailBackendStartedBeforeCAS,
		FailBackendStartedAfterCAS,
		FailExecDeathAfterStartBeforeCAS,
		FailRecordStartedBeforeCAS,
		FailRecordStartedAfterCAS,
		FailLaunchExitBeforeCAS,
		FailLaunchExitAfterCAS,
		FailLaunchQuiescentBeforeCAS,
		FailLaunchQuiescentAfterCAS,
		FailReconciliationBeforeCAS,
		FailReconciliationAfterCAS,
		FailContainmentSignalBefore,
		FailContainmentSignalAfter,
		FailContainmentSignalBeforeCAS,
		FailContainmentSignalAfterCAS,
		FailContainmentVerifyBefore,
		FailContainmentVerifyAfter,
		FailContainmentVerifyBeforeCAS,
		FailContainmentVerifyAfterCAS,
		FailContainmentRecordBeforeCAS,
		FailContainmentRecordAfterCAS,
		FailRetirementCloseBefore,
		FailRetirementCloseAfter,
		FailRetirementStartedBeforeCAS,
		FailRetirementStartedAfterCAS,
		FailRetirementWaitBefore,
		FailRetirementWaitAfter,
		FailRetirementWorkerBeforeCAS,
		FailRetirementWorkerAfterCAS,
		FailRetirementVerifyBefore,
		FailRetirementVerifyAfter,
		FailRetirementRecordBeforeCAS,
		FailRetirementRecordAfterCAS,
		FailOutcomeBeforeCAS,
		FailOutcomeAfterCAS,
		FailResultPublicationBeforeCAS,
		FailResultPublicationAfterCAS,
		FailResultTempWriteBefore,
		FailResultTempWriteAfter,
		FailResultFsyncTempBefore,
		FailResultFsyncTempAfter,
		FailResultCloseBefore,
		FailResultCloseAfter,
		FailResultRenameBefore,
		FailResultRenameAfter,
		FailResultDirFsyncBefore,
		FailResultDirFsyncAfter,
		FailTerminalBeforeCAS,
		FailTerminalAfterCAS,
		FailExpireBeforeCAS,
		FailExpireAfterCAS,
		FailCorruptBeforeCAS,
		FailCorruptAfterCAS,
		FailAnchorTempDBBefore,
		FailAnchorTempDBAfter,
		FailAnchorDBFsyncBefore,
		FailAnchorDBFsyncAfter,
		FailAnchorRenameBefore,
		FailAnchorRenameAfter,
		FailAnchorDirFsyncBefore,
		FailAnchorDirFsyncAfter,
		FailAnchorPublishBefore,
		FailAnchorPublishAfter,
		FailAnchorPublishDirFsyncBefore,
		FailAnchorPublishDirFsyncAfter,
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
	Hits   map[Failpoint]int
}

func (f *FailureInjector) Fail(point Failpoint) error {
	if f == nil {
		return nil
	}
	if f.Hits == nil {
		f.Hits = map[Failpoint]int{}
	}
	f.Hits[point]++
	if f.Hit || f.Target != point {
		return nil
	}
	f.Hit = true
	return InjectedFailure{Point: point}
}
