package model

import (
	"errors"
	"testing"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/internal/protocol"
)

var errNotImplemented = errors.New("task spec identity skeleton: implemented in a later commit")

type TaskSpecIdentity struct {
	Algorithm string
	Value     string
}

func (i TaskSpecIdentity) Equal(other TaskSpecIdentity) bool {
	return i.Algorithm == other.Algorithm && i.Value == other.Value
}

func CanonicalTaskSpecIdentity(protocol.TaskSpec) (TaskSpecIdentity, error) {
	return TaskSpecIdentity{}, errNotImplemented
}

func TestEveryActualTaskSpecFieldAffectsIdentity(t *testing.T) {
	timeout := int64(30000)
	base := protocol.TaskSpec{
		Backend: "codex",
		CWD:     "/workspace/a",
		Write:   true,
		Model:   "gpt-5",
		Effort:  "medium",
		Prompt:  "do the work",
		Policy: &engine.TurnPolicy{
			Prologue: "return receipts",
			Retry: &engine.RetryPolicy{
				Max:      1,
				Template: "try again with evidence",
			},
		},
		Tags:      map[string]string{"suite": "architecture"},
		TimeoutMs: &timeout,
	}

	t.Skip("implemented in Commit 2: canonical whole-taskSpec identity")

	baseID, err := CanonicalTaskSpecIdentity(base)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*protocol.TaskSpec)
	}{
		{name: "backend", mutate: func(spec *protocol.TaskSpec) { spec.Backend = "claude" }},
		{name: "cwd", mutate: func(spec *protocol.TaskSpec) { spec.CWD = "/workspace/b" }},
		{name: "write", mutate: func(spec *protocol.TaskSpec) { spec.Write = false }},
		{name: "model", mutate: func(spec *protocol.TaskSpec) { spec.Model = "gpt-5-mini" }},
		{name: "effort", mutate: func(spec *protocol.TaskSpec) { spec.Effort = "high" }},
		{name: "prompt", mutate: func(spec *protocol.TaskSpec) { spec.Prompt = "do different work" }},
		{name: "policy", mutate: func(spec *protocol.TaskSpec) { spec.Policy.Prologue = "return json" }},
		{name: "tags", mutate: func(spec *protocol.TaskSpec) { spec.Tags["suite"] = "changed" }},
		{name: "timeout", mutate: func(spec *protocol.TaskSpec) {
			changed := int64(60000)
			spec.TimeoutMs = &changed
		}},
	} {
		variant := cloneTaskSpec(base)
		tt.mutate(&variant)
		id, err := CanonicalTaskSpecIdentity(variant)
		if err != nil {
			t.Fatalf("%s identity: %v", tt.name, err)
		}
		if id.Equal(baseID) {
			t.Fatalf("%s did not affect identity", tt.name)
		}
	}
}

func cloneTaskSpec(spec protocol.TaskSpec) protocol.TaskSpec {
	if spec.Policy != nil {
		policy := *spec.Policy
		if policy.Retry != nil {
			retry := *policy.Retry
			policy.Retry = &retry
		}
		spec.Policy = &policy
	}
	if spec.Tags != nil {
		tags := make(map[string]string, len(spec.Tags))
		for key, value := range spec.Tags {
			tags[key] = value
		}
		spec.Tags = tags
	}
	if spec.TimeoutMs != nil {
		timeout := *spec.TimeoutMs
		spec.TimeoutMs = &timeout
	}
	return spec
}
