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
	}
	expire := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-expire-fp", "owner")
		req := modelRequest("ws-expire-fp", "req-expire-fp", "fp")
		if _, err := c.Submit(req, nil); err != nil {
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
	for _, point := range []Failpoint{FailSupervisorRecordBeforeCAS, FailSupervisorRecordAfterCAS} {
		matrix[point] = prepare
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
		FailRetirementWaitBefore,
		FailRetirementWaitAfter,
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
		FailContainmentVerifyBefore,
		FailContainmentVerifyAfter,
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
