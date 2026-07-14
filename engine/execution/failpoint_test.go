package execution

import "testing"

func TestFailpointHarnessCoversLifecycle(t *testing.T) {
	t.Run("no failure", func(t *testing.T) {
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-fail", "owner")
		res, err := c.Submit(modelRequest("ws-fail", "req-fail", "fp-fail"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
			t.Fatal(err)
		}
		if err := c.GrantPermit(res.JobID, 1, "nonce-1", nil); err != nil {
			t.Fatal(err)
		}
		if err := c.Start(res.JobID, nil); err != nil {
			t.Fatal(err)
		}
		if err := c.Complete(res.JobID, OutcomeCompleted); err != nil {
			t.Fatal(err)
		}
		checkCoordinator(t, c)
	})

	scenarios := failpointScenarios()
	for _, point := range AllFailpoints() {
		point := point
		t.Run(string(point), func(t *testing.T) {
			run, ok := scenarios[point]
			if !ok {
				t.Fatalf("no coverage scenario for failpoint %s", point)
			}
			injector := &FailureInjector{Target: point}
			run(t, injector)
			if !injector.Hit {
				t.Fatalf("failpoint %s was not exercised", point)
			}
			if injector.Hits[point] == 0 {
				t.Fatalf("failpoint %s was not reached by coverage matrix", point)
			}
		})
	}
}

func TestFailpointCoverageMatrixReachesEveryEdgePoint(t *testing.T) {
	for _, edge := range failpointCoverageEdges() {
		edge := edge
		t.Run(edge.name, func(t *testing.T) {
			injector := &FailureInjector{}
			edge.run(t, injector)
			if injector.Hits[edge.before] == 0 {
				t.Fatalf("edge %s did not reach before point %s", edge.name, edge.before)
			}
			if injector.Hits[edge.after] == 0 {
				t.Fatalf("edge %s did not reach after point %s", edge.name, edge.after)
			}
		})
	}
}

func TestExecDeathFailpointsLandOnDistinctModeledStartStates(t *testing.T) {
	cases := []struct {
		name  string
		point Failpoint
	}{
		{name: "pre_fork", point: FailExecDeathBeforeFork},
		{name: "forked", point: FailExecDeathAfterForkBeforeExec},
		{name: "execed", point: FailExecDeathAfterExecBeforeStart},
		{name: "backend_started", point: FailExecDeathAfterStartBeforeCAS},
	}
	seen := map[string]Failpoint{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewCoordinator(NewMemoryAdmissionStore(), "boot-exec-death-"+tc.name, "owner")
			res := submitPreparedPermitted(t, c, "ws-exec-death-"+tc.name, "req-exec-death-"+tc.name, "fp")
			injector := &FailureInjector{Target: tc.point}
			if err := c.Start(res.JobID, injector); err == nil {
				t.Fatalf("Start returned nil error for %s", tc.point)
			}
			if !injector.Hit {
				t.Fatalf("exec death failpoint %s was not hit", tc.point)
			}
			job, ok := c.Store.GetJob(res.JobID)
			if !ok {
				t.Fatal("job missing after exec death")
			}
			if previous, ok := seen[job.StartPhase]; ok {
				t.Fatalf("exec death failpoint %s collapsed onto start phase %q already used by %s", tc.point, job.StartPhase, previous)
			}
			seen[job.StartPhase] = tc.point
		})
	}
}

type failpointCoverageEdge struct {
	name          string
	before, after Failpoint
	run           func(*testing.T, *FailureInjector)
}

func failpointCoverageEdges() []failpointCoverageEdge {
	scenarios := failpointScenarios()
	return []failpointCoverageEdge{
		{name: "admission_commit", before: FailAdmissionBeforeCommit, after: FailAdmissionAfterCommit, run: scenarios[FailAdmissionBeforeCommit]},
		{name: "acknowledge_cas", before: FailAcknowledgeBeforeCAS, after: FailAcknowledgeAfterCAS, run: scenarios[FailAcknowledgeBeforeCAS]},
		{name: "reject_cas", before: FailRejectBeforeCAS, after: FailRejectAfterCAS, run: scenarios[FailRejectBeforeCAS]},
		{name: "supervisor_prepare_side_effect", before: FailSupervisorPrepareBefore, after: FailSupervisorPrepareAfter, run: scenarios[FailSupervisorPrepareBefore]},
		{name: "supervisor_record_cas", before: FailSupervisorRecordBeforeCAS, after: FailSupervisorRecordAfterCAS, run: scenarios[FailSupervisorRecordBeforeCAS]},
		{name: "legacy_supervisor_prepare_side_effect", before: FailLegacySupervisorPrepareBefore, after: FailLegacySupervisorPrepareAfter, run: scenarios[FailLegacySupervisorPrepareBefore]},
		{name: "cancel_cas", before: FailCancelBeforeCAS, after: FailCancelAfterCAS, run: scenarios[FailCancelBeforeCAS]},
		{name: "permit_grant_cas", before: FailGrantPermitBeforeCAS, after: FailGrantPermitAfterCAS, run: scenarios[FailGrantPermitBeforeCAS]},
		{name: "permit_send_side_effect", before: FailPermitSendBeforeSideEffect, after: FailPermitSendAfterSideEffect, run: scenarios[FailPermitSendBeforeSideEffect]},
		{name: "legacy_unfenced_start_side_effect", before: FailLegacyUnfencedStartBefore, after: FailLegacyUnfencedStartAfter, run: scenarios[FailLegacyUnfencedStartBefore]},
		{name: "permit_maybe_sent_cas", before: FailPermitMaybeSentBeforeCAS, after: FailPermitMaybeSentAfterCAS, run: scenarios[FailPermitMaybeSentBeforeCAS]},
		{name: "exec_fork_cas", before: FailExecForkBeforeCAS, after: FailExecForkAfterCAS, run: scenarios[FailExecForkBeforeCAS]},
		{name: "exec_cas", before: FailExecBeforeCAS, after: FailExecAfterCAS, run: scenarios[FailExecBeforeCAS]},
		{name: "backend_started_cas", before: FailBackendStartedBeforeCAS, after: FailBackendStartedAfterCAS, run: scenarios[FailBackendStartedBeforeCAS]},
		{name: "record_started_cas", before: FailRecordStartedBeforeCAS, after: FailRecordStartedAfterCAS, run: scenarios[FailRecordStartedBeforeCAS]},
		{name: "launch_exit_cas", before: FailLaunchExitBeforeCAS, after: FailLaunchExitAfterCAS, run: scenarios[FailLaunchExitBeforeCAS]},
		{name: "launch_quiescent_cas", before: FailLaunchQuiescentBeforeCAS, after: FailLaunchQuiescentAfterCAS, run: scenarios[FailLaunchQuiescentBeforeCAS]},
		{name: "reconciliation_cas", before: FailReconciliationBeforeCAS, after: FailReconciliationAfterCAS, run: scenarios[FailReconciliationBeforeCAS]},
		{name: "containment_signal_side_effect", before: FailContainmentSignalBefore, after: FailContainmentSignalAfter, run: scenarios[FailContainmentSignalBefore]},
		{name: "containment_signal_cas", before: FailContainmentSignalBeforeCAS, after: FailContainmentSignalAfterCAS, run: scenarios[FailContainmentSignalBeforeCAS]},
		{name: "containment_verify_side_effect", before: FailContainmentVerifyBefore, after: FailContainmentVerifyAfter, run: scenarios[FailContainmentVerifyBefore]},
		{name: "containment_verify_cas", before: FailContainmentVerifyBeforeCAS, after: FailContainmentVerifyAfterCAS, run: scenarios[FailContainmentVerifyBeforeCAS]},
		{name: "containment_record_cas", before: FailContainmentRecordBeforeCAS, after: FailContainmentRecordAfterCAS, run: scenarios[FailContainmentRecordBeforeCAS]},
		{name: "retirement_close_side_effect", before: FailRetirementCloseBefore, after: FailRetirementCloseAfter, run: scenarios[FailRetirementCloseBefore]},
		{name: "retirement_started_cas", before: FailRetirementStartedBeforeCAS, after: FailRetirementStartedAfterCAS, run: scenarios[FailRetirementStartedBeforeCAS]},
		{name: "retirement_wait_side_effect", before: FailRetirementWaitBefore, after: FailRetirementWaitAfter, run: scenarios[FailRetirementWaitBefore]},
		{name: "retirement_worker_exited_cas", before: FailRetirementWorkerBeforeCAS, after: FailRetirementWorkerAfterCAS, run: scenarios[FailRetirementWorkerBeforeCAS]},
		{name: "retirement_verify_side_effect", before: FailRetirementVerifyBefore, after: FailRetirementVerifyAfter, run: scenarios[FailRetirementVerifyBefore]},
		{name: "retirement_record_cas", before: FailRetirementRecordBeforeCAS, after: FailRetirementRecordAfterCAS, run: scenarios[FailRetirementRecordBeforeCAS]},
		{name: "outcome_cas", before: FailOutcomeBeforeCAS, after: FailOutcomeAfterCAS, run: scenarios[FailOutcomeBeforeCAS]},
		{name: "result_publication_cas", before: FailResultPublicationBeforeCAS, after: FailResultPublicationAfterCAS, run: scenarios[FailResultPublicationBeforeCAS]},
		{name: "result_temp_write_side_effect", before: FailResultTempWriteBefore, after: FailResultTempWriteAfter, run: scenarios[FailResultTempWriteBefore]},
		{name: "result_fsync_temp_side_effect", before: FailResultFsyncTempBefore, after: FailResultFsyncTempAfter, run: scenarios[FailResultFsyncTempBefore]},
		{name: "result_close_side_effect", before: FailResultCloseBefore, after: FailResultCloseAfter, run: scenarios[FailResultCloseBefore]},
		{name: "result_rename_side_effect", before: FailResultRenameBefore, after: FailResultRenameAfter, run: scenarios[FailResultRenameBefore]},
		{name: "result_dir_fsync_side_effect", before: FailResultDirFsyncBefore, after: FailResultDirFsyncAfter, run: scenarios[FailResultDirFsyncBefore]},
		{name: "terminal_cas", before: FailTerminalBeforeCAS, after: FailTerminalAfterCAS, run: scenarios[FailTerminalBeforeCAS]},
		{name: "expire_cas", before: FailExpireBeforeCAS, after: FailExpireAfterCAS, run: scenarios[FailExpireBeforeCAS]},
		{name: "corrupt_cas", before: FailCorruptBeforeCAS, after: FailCorruptAfterCAS, run: scenarios[FailCorruptBeforeCAS]},
		{name: "anchor_temp_db_side_effect", before: FailAnchorTempDBBefore, after: FailAnchorTempDBAfter, run: scenarios[FailAnchorTempDBBefore]},
		{name: "anchor_db_fsync_side_effect", before: FailAnchorDBFsyncBefore, after: FailAnchorDBFsyncAfter, run: scenarios[FailAnchorDBFsyncBefore]},
		{name: "anchor_rename_side_effect", before: FailAnchorRenameBefore, after: FailAnchorRenameAfter, run: scenarios[FailAnchorRenameBefore]},
		{name: "anchor_dir_fsync_side_effect", before: FailAnchorDirFsyncBefore, after: FailAnchorDirFsyncAfter, run: scenarios[FailAnchorDirFsyncBefore]},
		{name: "anchor_publish_side_effect", before: FailAnchorPublishBefore, after: FailAnchorPublishAfter, run: scenarios[FailAnchorPublishBefore]},
		{name: "anchor_publish_dir_fsync_side_effect", before: FailAnchorPublishDirFsyncBefore, after: FailAnchorPublishDirFsyncAfter, run: scenarios[FailAnchorPublishDirFsyncBefore]},
	}
}

func failpointScenarios() map[Failpoint]func(*testing.T, *FailureInjector) {
	admission := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-admission", "owner")
		_, _ = c.Submit(modelRequest("ws-admission", "req-admission", "fp"), injector)
	}
	ack := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-ack-fp", "owner")
		res, err := c.SubmitLegacyFenced(modelRequest("ws-ack-fp", "", "fp"), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.AcknowledgeWithInjector(res.JobID, injector)
	}
	reject := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-reject-fp", "owner")
		res, err := c.SubmitLegacyFenced(modelRequest("ws-reject-fp", "", "fp"), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.RejectUnacknowledgedWithInjector(res.JobID, injector)
	}
	prepare := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-prepare-fp", "owner")
		res, err := c.Submit(modelRequest("ws-prepare-fp", "req-prepare-fp", "fp"), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.PrepareSupervisor(res.JobID, injector)
	}
	legacyPrepare := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-legacy-prepare-fp", "owner")
		_, _ = c.SubmitLegacyFenced(modelRequest("ws-legacy-prepare-fp", "", "fp"), injector)
	}
	legacyUnfenced := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-legacy-unfenced-fp", "owner")
		_, _ = c.SubmitLegacyUnfenced(modelRequest("ws-legacy-unfenced-fp", "", "fp"), true, injector)
	}
	cancel := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-cancel-fp", "owner")
		res, err := c.Submit(modelRequest("ws-cancel-fp", "req-cancel-fp", "fp"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
			t.Fatal(err)
		}
		_ = c.CancelWithInjector(res.JobID, injector)
	}
	grant := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-grant-fp", "owner")
		res, err := c.Submit(modelRequest("ws-grant-fp", "req-grant-fp", "fp"), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
			t.Fatal(err)
		}
		_ = c.GrantPermit(res.JobID, 1, "nonce", injector)
	}
	start := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-start-fp", "owner")
		res := submitPreparedPermitted(t, c, "ws-start-fp", "req-start-fp", "fp")
		_ = c.Start(res.JobID, injector)
	}
	complete := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-complete-fp", "owner")
		res := submitPreparedPermitted(t, c, "ws-complete-fp", "req-complete-fp", "fp")
		if err := c.Start(res.JobID, nil); err != nil {
			t.Fatal(err)
		}
		_ = c.CompleteWithInjector(res.JobID, OutcomeCompleted, injector)
	}
	reconcile := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-reconcile-fp", "owner")
		res := submitPreparedPermitted(t, c, "ws-reconcile-fp", "req-reconcile-fp", "fp")
		_ = c.LiveSupervisorLossWithInjector(res.JobID, injector)
		assertTerminalizedOrFailStopped(t, c, res.JobID)
	}
	expire := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-expire-fp", "owner")
		req := modelRequest("ws-expire-fp", "req-expire-fp", "fp")
		res, err := c.Submit(req, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.PrepareSupervisor(res.JobID, nil); err != nil {
			t.Fatal(err)
		}
		if err := c.Cancel(res.JobID); err != nil {
			t.Fatal(err)
		}
		_, _ = c.Expire(req.WorkspaceKey, req.RequestID, injector)
	}
	corrupt := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-corrupt-fp", "owner")
		res, err := c.Submit(modelRequest("ws-corrupt-fp", "req-corrupt-fp", "fp"), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = c.MarkCorrupt(res.JobID, false, true, "checksum", injector)
	}
	anchor := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		_, _ = RunAnchorInitialization(1, 1, injector)
	}

	matrix := map[Failpoint]func(*testing.T, *FailureInjector){}
	for _, point := range []Failpoint{FailAdmissionBeforeCommit, FailAdmissionAfterCommit, FailPostCommitPreRunnable} {
		matrix[point] = admission
	}
	for _, point := range []Failpoint{FailAcknowledgeBeforeCAS, FailAcknowledgeAfterCAS} {
		matrix[point] = ack
	}
	for _, point := range []Failpoint{FailRejectBeforeCAS, FailRejectAfterCAS} {
		matrix[point] = reject
	}
	for _, point := range []Failpoint{FailSupervisorPrepareBefore, FailSupervisorPrepareAfter, FailSupervisorRecordBeforeCAS, FailSupervisorRecordAfterCAS} {
		matrix[point] = prepare
	}
	for _, point := range []Failpoint{FailLegacySupervisorPrepareBefore, FailLegacySupervisorPrepareAfter} {
		matrix[point] = legacyPrepare
	}
	for _, point := range []Failpoint{FailCancelBeforeCAS, FailCancelAfterCAS} {
		matrix[point] = cancel
	}
	for _, point := range []Failpoint{
		FailGrantPermitBeforeCAS,
		FailGrantPermitAfterCAS,
		FailPermitSendBeforeSideEffect,
		FailPermitSendAfterSideEffect,
		FailPermitMaybeSentBeforeCAS,
		FailPermitMaybeSentAfterCAS,
	} {
		matrix[point] = grant
	}
	for _, point := range []Failpoint{FailLegacyUnfencedStartBefore, FailLegacyUnfencedStartAfter} {
		matrix[point] = legacyUnfenced
	}
	for _, point := range []Failpoint{
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
	} {
		matrix[point] = start
	}
	for _, point := range []Failpoint{
		FailLaunchExitBeforeCAS,
		FailLaunchExitAfterCAS,
		FailLaunchQuiescentBeforeCAS,
		FailLaunchQuiescentAfterCAS,
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
	} {
		matrix[point] = complete
	}
	for _, point := range []Failpoint{
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
	} {
		matrix[point] = reconcile
	}
	for _, point := range []Failpoint{FailExpireBeforeCAS, FailExpireAfterCAS} {
		matrix[point] = expire
	}
	for _, point := range []Failpoint{FailCorruptBeforeCAS, FailCorruptAfterCAS} {
		matrix[point] = corrupt
	}
	for _, point := range []Failpoint{
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
	} {
		matrix[point] = anchor
	}
	return matrix
}
