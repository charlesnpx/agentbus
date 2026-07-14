package model

import (
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/charlesnpx/agentbus/internal/protocol"
)

func TestTaskIdentityFieldMatrix(t *testing.T) {
	base := baseRawTaskSpec()
	baseID := mustTaskIdentity(t, base)
	matrix := []struct {
		name  string
		field string
		raw   string
	}{
		{name: "backend", field: "backend", raw: strings.Replace(base, `"backend":"codex"`, `"backend":"claude"`, 1)},
		{name: "cwd", field: "cwd", raw: strings.Replace(base, `"cwd":"/workspace/a"`, `"cwd":"/workspace/b"`, 1)},
		{name: "write", field: "write", raw: strings.Replace(base, `"write":true`, `"write":false`, 1)},
		{name: "model", field: "model", raw: strings.Replace(base, `"model":"gpt-5"`, `"model":"gpt-5-mini"`, 1)},
		{name: "effort", field: "effort", raw: strings.Replace(base, `"effort":"medium"`, `"effort":"high"`, 1)},
		{name: "prompt", field: "prompt", raw: strings.Replace(base, `"prompt":"do the work"`, `"prompt":"do different work"`, 1)},
		{name: "timeout", field: "timeoutMs", raw: strings.Replace(base, `"timeoutMs":30000`, `"timeoutMs":60000`, 1)},
		{name: "tag key", field: "tags", raw: strings.Replace(base, `"priority":"high"`, `"urgency":"high"`, 1)},
		{name: "tag value", field: "tags", raw: strings.Replace(base, `"suite":"architecture"`, `"suite":"identity"`, 1)},
		{name: "tag addition", field: "tags", raw: strings.Replace(base, `"tags":{"priority":"high","suite":"architecture"}`, `"tags":{"owner":"admission","priority":"high","suite":"architecture"}`, 1)},
		{name: "tag removal", field: "tags", raw: strings.Replace(base, `"tags":{"priority":"high","suite":"architecture"}`, `"tags":{"suite":"architecture"}`, 1)},
		{name: "policy prologue", field: "policy", raw: strings.Replace(base, `"prologue":"return receipts"`, `"prologue":"return json"`, 1)},
		{name: "retry max", field: "policy", raw: strings.Replace(base, `"max":1`, `"max":0`, 1)},
		{name: "retry template", field: "policy", raw: strings.Replace(base, `"template":"retry with {{missing}}; emit the corrected report only and make no further changes"`, `"template":"retry with evidence"`, 1)},
		{name: "contract variant", field: "policy", raw: strings.Replace(base, `"contract":{"jsonSchema":{"properties":{"status":{"const":"ok"}},"required":["status"],"type":"object"}}`, `"contract":{"shape":{"requiredSections":["Findings"]}}`, 1)},
		{name: "contract nested field", field: "policy", raw: strings.Replace(base, `"required":["status"]`, `"required":["status","receipt"]`, 1)},
		{name: "json schema content", field: "policy", raw: strings.Replace(base, `"const":"ok"`, `"const":"done"`, 1)},
	}

	covered := map[string]bool{}
	for _, tt := range matrix {
		t.Run(tt.name, func(t *testing.T) {
			covered[tt.field] = true
			id := mustTaskIdentity(t, tt.raw)
			if id.Equal(baseID) {
				t.Fatalf("%s did not affect identity", tt.name)
			}
		})
	}
	assertTaskSpecFieldsCovered(t, covered, base)
}

func TestTaskIdentityCanonicalEquivalence(t *testing.T) {
	a := `{
		"backend":"codex",
		"cwd":"/workspace/a",
		"write":true,
		"prompt":"do the work",
		"tags":{"suite":"architecture","priority":"high"},
		"policy":{"contract":{"jsonSchema":{"required":["status","receipt"],"type":"object"}}}
	}`
	b := `{"policy":{"contract":{"jsonSchema":{"type":"object","required":["status","receipt"]}}},"tags":{"priority":"high","suite":"architecture"},"prompt":"do the work","write":true,"cwd":"/workspace/a","backend":"codex"}`
	if got, want := mustTaskIdentity(t, a), mustTaskIdentity(t, b); !got.Equal(want) {
		t.Fatalf("canonically equivalent task specs hashed differently:\n%+v\n%+v", got, want)
	}

	arrayOrder := strings.Replace(b, `["status","receipt"]`, `["receipt","status"]`, 1)
	if got, changed := mustTaskIdentity(t, b), mustTaskIdentity(t, arrayOrder); got.Equal(changed) {
		t.Fatal("array order did not affect identity")
	}
}

func TestTaskIdentityCanonicalRendering(t *testing.T) {
	raw := json.RawMessage(`{"z":-0,"a":[true,null,"<tag>"],"n":9007199254740993}`)
	canonical, err := CanonicalTaskSpecJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"a":[true,null,"\u003ctag\u003e"],"n":9007199254740993,"z":-0}`
	if string(canonical) != want {
		t.Fatalf("canonical = %s, want %s", canonical, want)
	}

	one := mustTaskIdentity(t, `{"timeoutMs":1}`)
	onePointZero := mustTaskIdentity(t, `{"timeoutMs":1.0}`)
	if one.Equal(onePointZero) {
		t.Fatal("different legal number spellings normalized to the same identity")
	}
}

func TestTaskIdentityRejectsDuplicateKeys(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "taskSpec top level", raw: `{"backend":"codex","backend":"claude"}`},
		{name: "taskSpec escaped equivalent", raw: `{"tags":{},"\u0074ags":{}}`},
		{name: "nested tags", raw: `{"tags":{"suite":"a","suite":"b"}}`},
		{name: "deep json schema", raw: `{"policy":{"contract":{"jsonSchema":{"properties":{"status":{},"status":{"const":"ok"}}}}}}`},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := TaskIdentityFromRawTaskSpec(json.RawMessage(tt.raw)); err == nil {
				t.Fatal("duplicate keys were accepted")
			}
		})
	}
}

func TestExtractRawTaskSpecEnvelope(t *testing.T) {
	envelope := []byte(`{"jsonrpc":"2.0","id":"1","params":{"workspaceKey":"workspace-a","taskSpec":{"prompt":"do the work","write":true,"cwd":"/workspace/a","backend":"codex"}}}`)
	raw, err := ExtractRawTaskSpec(envelope)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := CanonicalTaskSpecJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"backend":"codex","cwd":"/workspace/a","prompt":"do the work","write":true}`
	if string(canonical) != want {
		t.Fatalf("extracted canonical taskSpec = %s, want %s", canonical, want)
	}

	duplicateParams := []byte(`{"params":{"taskSpec":{"backend":"codex"},"taskSpec":{"backend":"claude"}}}`)
	if _, err := ExtractRawTaskSpec(duplicateParams); err == nil {
		t.Fatal("duplicate params.taskSpec was accepted")
	}
	duplicateOuter := []byte(`{"params":{"taskSpec":{}},"params":{"taskSpec":{}}}`)
	if _, err := ExtractRawTaskSpec(duplicateOuter); err == nil {
		t.Fatal("duplicate outer params was accepted")
	}
	if _, err := ExtractRawTaskSpec([]byte(`{"params":{"taskSpec":null}}`)); err == nil {
		t.Fatal("non-object taskSpec was accepted")
	}
	if _, err := ExtractRawTaskSpec([]byte(`{"params":{}}`)); err == nil {
		t.Fatal("missing taskSpec was accepted")
	}
}

func TestTaskIdentityParserStrictness(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{name: "invalid utf8", raw: []byte{'{', '"', 'p', '"', ':', '"', 0xff, '"', '}'}},
		{name: "malformed escape", raw: []byte(`{"prompt":"\uZZZZ"}`)},
		{name: "multiple json values", raw: []byte(`{"prompt":"one"} {"prompt":"two"}`)},
		{name: "array not object", raw: []byte(`[]`)},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := TaskIdentityFromRawTaskSpec(tt.raw); err == nil {
				t.Fatal("invalid raw taskSpec was accepted")
			}
		})
	}
}

func TestTaskIdentityOmittedNullEmptyAndZeroAreDistinct(t *testing.T) {
	omitted := mustTaskIdentity(t, `{"backend":"codex"}`)
	explicitNull := mustTaskIdentity(t, `{"backend":"codex","timeoutMs":null}`)
	emptyTags := mustTaskIdentity(t, `{"backend":"codex","tags":{}}`)
	zeroTimeout := mustTaskIdentity(t, `{"backend":"codex","timeoutMs":0}`)
	for _, other := range []TaskIdentity{explicitNull, emptyTags, zeroTimeout} {
		if omitted.Equal(other) {
			t.Fatalf("omitted field normalized to %+v", other)
		}
	}
}

func TestRecordedTaskIdentityFailsClosed(t *testing.T) {
	valid := mustTaskIdentity(t, baseRawTaskSpec())
	if err := TaskIdentityMatchesRawTaskSpec(valid, json.RawMessage(baseRawTaskSpec())); err != nil {
		t.Fatalf("valid recorded identity rejected: %v", err)
	}

	unknownVersion := valid
	unknownVersion.Version = CurrentTaskIdentityVersion + 1
	if err := TaskIdentityMatchesRawTaskSpec(unknownVersion, json.RawMessage(baseRawTaskSpec())); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("unknown version error = %v, want ErrInvalidValue", err)
	}
	if _, err := TaskIdentityCodecFor(TaskIdentityAlgorithmSHA256, CurrentTaskIdentityVersion+1); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("codec lookup error = %v, want ErrInvalidValue", err)
	}

	for _, identity := range []TaskIdentity{
		{},
		{Algorithm: TaskIdentityAlgorithmSHA256, Version: CurrentTaskIdentityVersion},
		{Algorithm: TaskIdentityAlgorithmSHA256, Version: 0, Value: valid.Value},
	} {
		if err := identity.Validate(); !errors.Is(err, ErrInvalidValue) {
			t.Fatalf("incomplete identity %+v error = %v, want ErrInvalidValue", identity, err)
		}
	}

	changed := mustTaskIdentity(t, strings.Replace(baseRawTaskSpec(), `"prompt":"do the work"`, `"prompt":"do something else"`, 1))
	if err := TaskIdentityMatchesRawTaskSpec(valid, json.RawMessage(strings.Replace(baseRawTaskSpec(), `"prompt":"do the work"`, `"prompt":"do something else"`, 1))); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("mismatched raw taskSpec error = %v, want ErrInvalidValue", err)
	}
	if valid.Equal(changed) {
		t.Fatal("test setup failed: changed prompt produced identical identity")
	}
}

func mustTaskIdentity(t *testing.T, raw string) TaskIdentity {
	t.Helper()
	identity, err := TaskIdentityFromRawTaskSpec(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("TaskIdentityFromRawTaskSpec(%s): %v", raw, err)
	}
	return identity
}

func baseRawTaskSpec() string {
	return `{"backend":"codex","cwd":"/workspace/a","write":true,"model":"gpt-5","effort":"medium","prompt":"do the work","timeoutMs":30000,"tags":{"priority":"high","suite":"architecture"},"policy":{"prologue":"return receipts","retry":{"max":1,"template":"retry with {{missing}}; emit the corrected report only and make no further changes"},"contract":{"jsonSchema":{"properties":{"status":{"const":"ok"}},"required":["status"],"type":"object"}}}}`
}

func assertTaskSpecFieldsCovered(t *testing.T, covered map[string]bool, raw string) {
	t.Helper()
	want := protocolTaskSpecJSONFields(t)
	for _, field := range want {
		if !covered[field] {
			t.Fatalf("field matrix does not cover protocol.TaskSpec field %q", field)
		}
	}

	var rawFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &rawFields); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(rawFields))
	for field := range rawFields {
		got = append(got, field)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("base raw taskSpec fields = %v, want protocol.TaskSpec fields %v", got, want)
	}
}

func protocolTaskSpecJSONFields(t *testing.T) []string {
	t.Helper()
	typ := reflect.TypeOf(protocol.TaskSpec{})
	fields := make([]string, 0, typ.NumField())
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		name := strings.Split(field.Tag.Get("json"), ",")[0]
		if name == "" || name == "-" {
			t.Fatalf("protocol.TaskSpec field %s has no JSON name", field.Name)
		}
		fields = append(fields, name)
	}
	sort.Strings(fields)
	return fields
}
