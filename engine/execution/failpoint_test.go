package execution

import "testing"

func TestFailpointHarnessCoversLifecycle(t *testing.T) {
	runLifecycle := func(t *testing.T, injector *FailureInjector) {
		t.Helper()
		c := NewCoordinator(NewMemoryAdmissionStore(), "boot-fail", "owner")
		res, err := c.Submit(modelRequest("ws-fail", "req-fail", "fp-fail"), injector)
		checkCoordinator(t, c)
		if err != nil {
			return
		}
		if err := c.PrepareSupervisor(res.JobID, injector); err != nil {
			checkCoordinator(t, c)
			return
		}
		checkCoordinator(t, c)
		if err := c.GrantPermit(res.JobID, 1, "nonce-1", injector); err != nil {
			checkCoordinator(t, c)
			return
		}
		checkCoordinator(t, c)
		if err := c.Start(res.JobID, injector); err != nil {
			checkCoordinator(t, c)
			return
		}
		checkCoordinator(t, c)
		if err := c.Complete(res.JobID, OutcomeCompleted); err != nil {
			t.Fatal(err)
		}
		checkCoordinator(t, c)
	}

	t.Run("no failure", func(t *testing.T) {
		runLifecycle(t, nil)
	})
	for _, point := range AllFailpoints() {
		point := point
		t.Run(string(point), func(t *testing.T) {
			injector := &FailureInjector{Target: point}
			runLifecycle(t, injector)
			if !injector.Hit {
				t.Fatalf("failpoint %s was not exercised", point)
			}
		})
	}
}
