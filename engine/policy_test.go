package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"
)

const correctiveRetryTemplate = "Your response missed: {{missing}}. Emit the corrected report only; make no further changes."

func TestShapeContracts(t *testing.T) {
	t.Parallel()
	contract := ContractSpec{Shape: &ShapeSpec{
		FirstLineEnum:        []string{"PASS", "FAIL"},
		RequiredSections:     []string{"Findings", "Tests"},
		RequiredAttestations: []string{"I inspected the diff."},
	}}
	tests := []struct {
		name        string
		text        string
		wantMissing string
	}{
		{
			name: "compliant headings",
			text: "PASS\n\n## Findings\n\n## Tests\n\nI inspected the diff.",
		},
		{
			name: "case insensitive labels and ansi stripped",
			text: "\x1b[32mPASS\x1b[0m\nfindings:\nTests:\nI inspected the diff.",
		},
		{
			name:        "first line exact no trim",
			text:        " PASS\n\n## Findings\n## Tests\nI inspected the diff.",
			wantMissing: "firstLineEnum",
		},
		{
			name:        "missing findings",
			text:        "PASS\n\n## Tests\nI inspected the diff.",
			wantMissing: "section:Findings",
		},
		{
			name:        "missing tests",
			text:        "PASS\n\n## Findings\nI inspected the diff.",
			wantMissing: "section:Tests",
		},
		{
			name:        "missing attestation",
			text:        "PASS\n\n## Findings\n\n## Tests\n",
			wantMissing: "attestation:I inspected the diff.",
		},
		{
			name:        "fenced sections and attestations excluded",
			text:        "PASS\n```md\n## Findings\n## Tests\nI inspected the diff.\n```\n",
			wantMissing: "section:Findings",
		},
		{
			name:        "indented tilde fenced sections and attestations excluded",
			text:        "PASS\n   ~~~md\n## Findings\n## Tests\nI inspected the diff.\n   ~~~\n",
			wantMissing: "section:Findings",
		},
		{
			name:        "backtick fence not closed by tilde fence",
			text:        "PASS\n```md\n~~~\n## Findings\n## Tests\nI inspected the diff.\n```\n",
			wantMissing: "section:Findings",
		},
		{
			name:        "long backtick fence not closed by shorter run",
			text:        "PASS\n````md\n```\n## Findings\n## Tests\nI inspected the diff.\n````\n",
			wantMissing: "section:Findings",
		},
		{
			name:        "tilde fence not closed by backtick fence",
			text:        "PASS\n~~~md\n```\n## Findings\n## Tests\nI inspected the diff.\n~~~\n",
			wantMissing: "section:Findings",
		},
		{
			name: "duplicates allowed first wins empty section satisfies",
			text: "PASS\nFindings:\nfindings: later\nTests:\nI inspected the diff.",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateContract(tt.text, contract)
			if err != nil {
				t.Fatal(err)
			}
			if tt.wantMissing == "" && !got.Valid {
				t.Fatalf("valid = false, missing = %v", got.Missing)
			}
			if tt.wantMissing != "" && !contains(got.Missing, tt.wantMissing) {
				t.Fatalf("missing = %v, want %q", got.Missing, tt.wantMissing)
			}
		})
	}
}

func TestShapeEvidenceHeuristic(t *testing.T) {
	t.Parallel()
	contract := ContractSpec{Shape: &ShapeSpec{RequiredSections: []string{"Findings"}, EvidenceHeuristic: true}}
	tests := []struct {
		name  string
		text  string
		valid bool
	}{
		{name: "no findings does not require evidence", text: "Findings:\nNo findings", valid: true},
		{name: "not applicable does not require evidence", text: "## Findings\nnot applicable", valid: true},
		{name: "path line evidence", text: "Findings:\nBug in engine/run.go:42", valid: true},
		{name: "diff hunk evidence", text: "Findings:\nRegression observed\n@@ -1,2 +1,2 @@", valid: true},
		{name: "priority label finding without evidence", text: "Findings:\nP1: missing validation", valid: false},
		{name: "fenced command adjacent exit evidence before", text: "Findings:\nIt failed\nexit code 1\n```sh\ngo test ./...\n```", valid: true},
		{name: "fenced command adjacent exit evidence after", text: "Findings:\nIt failed\n```sh\ngo test ./...\n```\nexit 1", valid: true},
		{name: "fenced command evidence inside backtick fence not closed by tilde run", text: "Findings:\nIt failed\n```md\nexit code 1\n~~~sh\ngo test ./...\n~~~\n```", valid: false},
		{name: "fenced command evidence inside longer backtick fence not closed by shorter run", text: "Findings:\nIt failed\n````md\nexit code 1\n```sh\ngo test ./...\n```\n````", valid: false},
		{name: "properly closed fence followed by command exit evidence", text: "Findings:\nIt failed\n```md\nexample transcript\n```\nexit code 1\n```sh\ngo test ./...\n```", valid: true},
		{name: "fenced path line excluded", text: "Findings:\nIt failed\n```\nengine/run.go:42\n```", valid: false},
		{name: "claimed finding without evidence", text: "Findings:\nThere is a real issue", valid: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateContract(tt.text, contract)
			if err != nil {
				t.Fatal(err)
			}
			if got.Valid != tt.valid {
				t.Fatalf("valid = %v, missing = %v, want %v", got.Valid, got.Missing, tt.valid)
			}
		})
	}
}

func TestShapeEvidenceFencedFindingsDoNotTrigger(t *testing.T) {
	t.Parallel()
	contract := ContractSpec{Shape: &ShapeSpec{EvidenceHeuristic: true}}
	got, err := ValidateContract("```\nFindings:\nThere is a real issue\n```", contract)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Valid {
		t.Fatalf("valid = false, missing = %v", got.Missing)
	}
}

func TestShapeIndentedFencesExcludeSectionsAttestationsAndEvidence(t *testing.T) {
	t.Parallel()
	contract := ContractSpec{Shape: &ShapeSpec{
		RequiredSections:     []string{"Findings"},
		RequiredAttestations: []string{"I inspected the diff."},
		EvidenceHeuristic:    true,
	}}
	got, err := ValidateContract("   ```md\n## Findings\nI inspected the diff.\nengine/policy.go:461\n   ```\nFindings:\nThere is a real issue", contract)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"attestation:I inspected the diff.", "evidence"} {
		if !contains(got.Missing, want) {
			t.Fatalf("missing = %v, want %q", got.Missing, want)
		}
	}
}

func TestJSONSchemaContracts(t *testing.T) {
	t.Parallel()
	contract := ContractSpec{JSONSchema: json.RawMessage(`{
		"$schema":"https://json-schema.org/draft/2020-12/schema",
		"type":"object",
		"required":["status","items"],
		"properties":{
			"status":{"enum":["ok"]},
			"items":{"type":"array","minItems":1,"items":{"type":"string"}}
		},
		"additionalProperties":false
	}`)}
	tests := []struct {
		name  string
		text  string
		valid bool
	}{
		{name: "valid", text: `{"status":"ok","items":["a"]}`, valid: true},
		{name: "invalid json", text: `not json`, valid: false},
		{name: "missing required", text: `{"status":"ok","items":[]}`, valid: false},
		{name: "additional properties", text: `{"status":"ok","items":["a"],"extra":true}`, valid: false},
		{name: "enum", text: `{"status":"bad","items":["a"]}`, valid: false},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateContract(tt.text, contract)
			if err != nil {
				t.Fatal(err)
			}
			if got.Valid != tt.valid {
				t.Fatalf("valid = %v, missing = %v, want %v", got.Valid, got.Missing, tt.valid)
			}
			if got.ContractSHA256 == "" || !strings.HasPrefix(got.ContractSHA256, "sha256:") {
				t.Fatalf("contract hash = %q", got.ContractSHA256)
			}
		})
	}
}

func TestJSONSchemaViolationDetails(t *testing.T) {
	t.Parallel()
	contract := ContractSpec{JSONSchema: json.RawMessage(`{
		"type":"object",
		"required":["schema_version","mode","payload"],
		"properties":{
			"schema_version":{"const":"v1"},
			"mode":{"enum":["summary","detail"]},
			"payload":{
				"type":"object",
				"required":["title"],
				"additionalProperties":false,
				"properties":{"title":{"type":"string"}}
			}
		}
	}`)}
	result, err := ValidateContract(`{"schema_version":"v0","mode":"invented","payload":{"title":7,"wrong/key~name":true}}`, contract)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/mode: value must be one of 'summary', 'detail'",
		"/payload/title: got number, want string",
		"/payload: additional properties 'wrong/key~name' not allowed",
		"/schema_version: value must be 'v1'",
	}
	if result.Valid || !equalStringSlices(result.Missing, want) {
		t.Fatalf("valid = %v, missing = %#v, want %#v", result.Valid, result.Missing, want)
	}
	stamp := StampValidation(1, false, "", result, time.Now())
	if !equalStringSlices(stamp.Missing, result.Missing) {
		t.Fatalf("stamp missing = %#v, want %#v", stamp.Missing, result.Missing)
	}
	if retryPrompt := RenderRetryTemplate("{{missing}}", result.Missing); retryPrompt != strings.Join(result.Missing, ", ") {
		t.Fatalf("retry prompt = %q, want missing = %#v", retryPrompt, result.Missing)
	}
}

func TestJSONSchemaViolationRequiredAndEscapedPointer(t *testing.T) {
	t.Parallel()
	contract := ContractSpec{JSONSchema: json.RawMessage(`{
		"type":"object",
		"properties":{"payload":{
			"type":"object",
			"required":["title"],
			"additionalProperties":false,
			"properties":{"slash/key~name":{"type":"string"}}
		}},
		"required":["payload"]
	}`)}
	result, err := ValidateContract(`{"payload":{"slash/key~name":7,"wrong/key~name":true}}`, contract)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"/payload/slash~1key~0name: got number, want string",
		"/payload: additional properties 'wrong/key~name' not allowed",
		"/payload: missing property 'title'",
	}
	if result.Valid || !equalStringSlices(result.Missing, want) {
		t.Fatalf("valid = %v, missing = %#v, want %#v", result.Valid, result.Missing, want)
	}
}

func TestJSONSchemaViolationLimitAndMessageTruncation(t *testing.T) {
	t.Parallel()
	var required []string
	for i := 0; i < 25; i++ {
		required = append(required, fmt.Sprintf("field-%02d", i))
	}
	var allOf []json.RawMessage
	for _, field := range required {
		allOf = append(allOf, json.RawMessage(`{"required":["`+field+`"]}`))
	}
	schema, err := json.Marshal(map[string]any{"allOf": allOf})
	if err != nil {
		t.Fatal(err)
	}
	result, err := ValidateContract(`{}`, ContractSpec{JSONSchema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Missing) != maxSchemaViolationEntries {
		t.Fatalf("missing count = %d, want %d: %#v", len(result.Missing), maxSchemaViolationEntries, result.Missing)
	}
	if got, want := result.Missing[len(result.Missing)-1], "+6 more schema violations"; got != want {
		t.Fatalf("last missing = %q, want %q", got, want)
	}
	if !strings.HasPrefix(result.Missing[0], ": ") {
		t.Fatalf("root violation = %q, want empty JSON pointer root", result.Missing[0])
	}

	longName := strings.Repeat("x", maxSchemaViolationMessageRunes+20)
	longSchema, err := json.Marshal(map[string]any{"required": []string{longName}})
	if err != nil {
		t.Fatal(err)
	}
	longResult, err := ValidateContract(`{}`, ContractSpec{JSONSchema: longSchema})
	if err != nil {
		t.Fatal(err)
	}
	if len(longResult.Missing) != 1 || !strings.HasSuffix(longResult.Missing[0], "...") {
		t.Fatalf("truncated missing = %#v", longResult.Missing)
	}

	largeMissing := make([]string, maxSchemaViolationEntries)
	for i := range largeMissing {
		largeMissing[i] = strings.Repeat("x", maxSchemaViolationMessageRunes)
	}
	if prompt := RenderRetryTemplate("{{missing}}{{missing}}", largeMissing); len(prompt) > maxRetryViolationTextBytes {
		t.Fatalf("retry violation text is %d bytes, want at most %d", len(prompt), maxRetryViolationTextBytes)
	}
}

func TestJSONPointerRoundTripsPrintableBackslashAndQuote(t *testing.T) {
	t.Parallel()
	for _, key := range []string{`a\b`, `a"b`} {
		t.Run(strconv.Quote(key), func(t *testing.T) {
			pointer := jsonPointer([]string{key})
			segment := strings.TrimPrefix(pointer, "/")
			roundTripped := strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")
			if roundTripped != key {
				t.Fatalf("pointer %q round-trips to %q, want %q", pointer, roundTripped, key)
			}
		})
	}
}

func TestJSONSchemaViolationsEscapeAndBoundUntrustedPropertyNames(t *testing.T) {
	t.Parallel()
	propertyNames := []string{
		"new\nline",
		"return\rline",
		"ansi" + string(rune(0x1b)) + "escape",
		strings.Repeat("界", maxSchemaViolationMessageRunes+20),
	}
	for _, propertyName := range propertyNames {
		t.Run(strconv.Quote(propertyName), func(t *testing.T) {
			pointer := jsonPointer([]string{propertyName})
			assertSingleLinePrintableViolation(t, pointer, 0)
			pointerSchema, err := json.Marshal(map[string]any{
				"type": "object",
				"properties": map[string]any{
					propertyName: map[string]any{"type": "string"},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			messageSchema, err := json.Marshal(map[string]any{
				"type":                 "object",
				"additionalProperties": false,
			})
			if err != nil {
				t.Fatal(err)
			}
			instance, err := json.Marshal(map[string]any{propertyName: 7})
			if err != nil {
				t.Fatal(err)
			}

			for schemaIndex, schema := range []json.RawMessage{pointerSchema, messageSchema} {
				result, err := ValidateContract(string(instance), ContractSpec{JSONSchema: schema})
				if err != nil {
					t.Fatal(err)
				}
				if result.Valid || len(result.Missing) == 0 {
					t.Fatalf("valid = %v, missing = %#v", result.Valid, result.Missing)
				}

				stamp := StampValidation(1, false, "", result, time.Now())
				for _, missing := range stamp.Missing {
					assertSingleLinePrintableViolation(t, missing, maxSchemaViolationPointerRunes+len(": ")+maxSchemaViolationMessageRunes)
				}
				if schemaIndex == 0 && utf8.RuneCountInString(pointer) <= maxSchemaViolationPointerRunes {
					if !strings.HasPrefix(stamp.Missing[0], pointer+": ") {
						t.Fatalf("pointer violation = %q, want prefix %q", stamp.Missing[0], pointer+": ")
					}
				} else if schemaIndex == 0 && !strings.Contains(stamp.Missing[0], ": got number, want string") {
					t.Fatalf("truncated pointer violation = %q, want diagnostic to survive", stamp.Missing[0])
				}
				prompt := RenderRetryTemplate("Correct these violations: {{missing}}", stamp.Missing)
				assertSingleLinePrintableViolation(t, prompt, len("Correct these violations: ")+maxRetryViolationTextBytes)
			}
		})
	}
}

func assertSingleLinePrintableViolation(t *testing.T, value string, maxRunes int) {
	t.Helper()
	if maxRunes > 0 && utf8.RuneCountInString(value) > maxRunes {
		t.Fatalf("violation has %d runes, want at most %d: %q", utf8.RuneCountInString(value), maxRunes, value)
	}
	if strings.ContainsAny(value, "\r\n") {
		t.Fatalf("violation contains a line break: %q", value)
	}
	for _, r := range value {
		if !unicode.IsPrint(r) {
			t.Fatalf("violation contains non-printable %U: %q", r, value)
		}
	}
}

func TestJSONSchemaParseAndCompileFailureDetails(t *testing.T) {
	t.Parallel()
	schema := json.RawMessage(`{"type":"object"}`)
	if got := validateJSONSchema(`{"value":`, schema); len(got) != 1 || !strings.HasPrefix(got[0], "json: ") || len(got[0]) == len("json: ") {
		t.Fatalf("parse missing = %#v", got)
	}
	if got := validateJSONSchema(`{}`, json.RawMessage(`{"type":42}`)); len(got) != 1 || !strings.HasPrefix(got[0], "jsonSchema: ") || len(got[0]) == len("jsonSchema: ") {
		t.Fatalf("compile missing = %#v", got)
	}
}

func TestContractSHA256PreservesLargeJSONNumbers(t *testing.T) {
	t.Parallel()
	a := ContractSpec{JSONSchema: json.RawMessage(`{"const":9007199254740992}`)}
	b := ContractSpec{JSONSchema: json.RawMessage(`{"const":9007199254740993}`)}
	hashA, err := ContractSHA256(a)
	if err != nil {
		t.Fatal(err)
	}
	hashB, err := ContractSHA256(b)
	if err != nil {
		t.Fatal(err)
	}
	if hashA == hashB {
		t.Fatalf("hashes matched for distinct large numeric literals: %s", hashA)
	}
}

func TestJSONSchemaDraft202012Features(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		schema string
		text   string
		valid  bool
	}{
		{
			name: "allOf applies every subschema",
			schema: `{
				"$schema":"https://json-schema.org/draft/2020-12/schema",
				"allOf":[
					{"type":"object","required":["status"]},
					{"properties":{"status":{"const":"ok"}}}
				]
			}`,
			text:  `{"status":"bad"}`,
			valid: false,
		},
		{
			name: "anyOf accepts one matching branch",
			schema: `{
				"$schema":"https://json-schema.org/draft/2020-12/schema",
				"anyOf":[
					{"required":["alpha"]},
					{"required":["beta"]}
				]
			}`,
			text:  `{"beta":true}`,
			valid: true,
		},
		{
			name: "oneOf rejects multiple matching branches",
			schema: `{
				"$schema":"https://json-schema.org/draft/2020-12/schema",
				"oneOf":[
					{"required":["alpha"]},
					{"required":["beta"]}
				]
			}`,
			text:  `{"alpha":true,"beta":true}`,
			valid: false,
		},
		{
			name: "not rejects forbidden shape",
			schema: `{
				"$schema":"https://json-schema.org/draft/2020-12/schema",
				"not":{"required":["debug"]}
			}`,
			text:  `{"debug":true}`,
			valid: false,
		},
		{
			name: "local ref resolves definitions",
			schema: `{
				"$schema":"https://json-schema.org/draft/2020-12/schema",
				"$defs":{"nonEmptyString":{"type":"string","minLength":1}},
				"type":"object",
				"properties":{"name":{"$ref":"#/$defs/nonEmptyString"}},
				"required":["name"]
			}`,
			text:  `{"name":""}`,
			valid: false,
		},
		{
			name: "format remains annotation by default",
			schema: `{
				"$schema":"https://json-schema.org/draft/2020-12/schema",
				"type":"object",
				"properties":{"email":{"type":"string","format":"email"}},
				"required":["email"]
			}`,
			text:  `{"email":"not an email address"}`,
			valid: true,
		},
		{
			name: "nested conditionals apply selected branch",
			schema: `{
				"$schema":"https://json-schema.org/draft/2020-12/schema",
				"type":"object",
				"required":["kind","payload"],
				"properties":{
					"kind":{"enum":["metric","message"]},
					"payload":{"type":"object"}
				},
				"if":{"properties":{"kind":{"const":"metric"}}},
				"then":{
					"properties":{
						"payload":{
							"required":["unit","value"],
							"if":{"properties":{"unit":{"const":"count"}}},
							"then":{"properties":{"value":{"type":"integer"}}},
							"else":{"properties":{"value":{"type":"number","exclusiveMinimum":0}}}
						}
					}
				},
				"else":{
					"properties":{"payload":{"required":["text"],"properties":{"text":{"type":"string","minLength":1}}}}
				}
			}`,
			text:  `{"kind":"metric","payload":{"unit":"count","value":1.5}}`,
			valid: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ValidateContract(tt.text, ContractSpec{JSONSchema: json.RawMessage(tt.schema)})
			if err != nil {
				t.Fatal(err)
			}
			if got.Valid != tt.valid {
				t.Fatalf("valid = %v, missing = %v, want %v", got.Valid, got.Missing, tt.valid)
			}
		})
	}
}

func TestPolicyRegistry(t *testing.T) {
	t.Parallel()
	registry := NewPolicyRegistry()
	spec := ContractSpec{Shape: &ShapeSpec{RequiredSections: []string{"Findings"}}}
	hash, err := registry.Register("delegate/delegate-report@1", spec)
	if err != nil {
		t.Fatal(err)
	}
	hash2, err := registry.Register("delegate/delegate-report@1", spec)
	if err != nil {
		t.Fatal(err)
	}
	if hash != hash2 {
		t.Fatalf("idempotent hash = %q, want %q", hash2, hash)
	}
	_, err = registry.Register("delegate/delegate-report@1", ContractSpec{Shape: &ShapeSpec{RequiredSections: []string{"Tests"}}})
	var conflict NameConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("conflict err = %v", err)
	}
	resolved, name, resolvedHash, err := ResolveContract(ContractSpec{Named: "delegate/delegate-report@1"}, registry)
	if err != nil {
		t.Fatal(err)
	}
	if name != "delegate/delegate-report@1" || resolvedHash != hash || resolved.Shape == nil {
		t.Fatalf("resolved = %#v name=%q hash=%q", resolved, name, resolvedHash)
	}
}

func TestPolicyRegistryDefensiveCopies(t *testing.T) {
	t.Parallel()
	registry := NewPolicyRegistry()
	spec := ContractSpec{Shape: &ShapeSpec{RequiredSections: []string{"Findings"}}}
	hash, err := registry.Register("delegate/delegate-report@1", spec)
	if err != nil {
		t.Fatal(err)
	}
	spec.Shape.RequiredSections[0] = "MutatedAfterRegister"
	resolved, resolvedHash, err := registry.Resolve("delegate/delegate-report@1")
	if err != nil {
		t.Fatal(err)
	}
	if resolvedHash != hash || resolved.Shape.RequiredSections[0] != "Findings" {
		t.Fatalf("resolved = %#v hash=%q, want immutable Findings hash=%q", resolved, resolvedHash, hash)
	}
	resolved.Shape.RequiredSections[0] = "MutatedAfterResolve"
	resolvedAgain, resolvedAgainHash, err := registry.Resolve("delegate/delegate-report@1")
	if err != nil {
		t.Fatal(err)
	}
	if resolvedAgainHash != hash || resolvedAgain.Shape.RequiredSections[0] != "Findings" {
		t.Fatalf("resolved again = %#v hash=%q, want immutable Findings hash=%q", resolvedAgain, resolvedAgainHash, hash)
	}
}

func TestResolveContractRejectsMultipleVariants(t *testing.T) {
	t.Parallel()
	registry := NewPolicyRegistry()
	spec := ContractSpec{Shape: &ShapeSpec{RequiredSections: []string{"Findings"}}}
	if _, err := registry.Register("delegate/delegate-report@1", spec); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := ResolveContract(ContractSpec{Shape: spec.Shape, Named: "delegate/delegate-report@1"}, registry)
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("mixed shape/named err = %v, want exactly one", err)
	}
}

func TestPolicyPersistenceFieldsAndStamps(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	store := newTestStore(t, now, fakeProcessTable{entries: map[int]ProcessInfo{}})
	spec := ContractSpec{Shape: &ShapeSpec{RequiredSections: []string{"Findings"}}}
	result, err := ValidateContract("## Findings\nNone", spec)
	if err != nil {
		t.Fatal(err)
	}
	stamp := StampValidation(2, true, "delegate/delegate-report@1", result, now)
	job := &JobRecord{
		JobID:            "job_policy",
		State:            StateCompleted,
		Policy:           &TurnPolicy{Contract: &ContractSpec{Named: "delegate/delegate-report@1"}, Retry: &RetryPolicy{Max: 1, Template: correctiveRetryTemplate}},
		ResolvedContract: &spec,
		Contract:         &stamp,
	}
	if err := store.Save(job); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(job.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ResolvedContract == nil || loaded.ResolvedContract.Shape == nil {
		t.Fatalf("resolved contract was not persisted: %#v", loaded.ResolvedContract)
	}
	if loaded.Contract == nil || loaded.Contract.Status != ContractRetried || !loaded.Contract.RetryUsed || loaded.Contract.Attempts != 2 {
		t.Fatalf("stamp = %#v", loaded.Contract)
	}
	if loaded.Contract.ValidatedAt.Format(time.RFC3339) != "2026-07-09T12:00:00Z" {
		t.Fatalf("validatedAt = %s", loaded.Contract.ValidatedAt.Format(time.RFC3339))
	}
}

func TestValidatePolicyTextHelper(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	registry := NewPolicyRegistry()
	spec := ContractSpec{Shape: &ShapeSpec{RequiredSections: []string{"Findings"}}}
	if _, err := registry.Register("delegate/delegate-report@1", spec); err != nil {
		t.Fatal(err)
	}
	got, err := ValidatePolicyText("## Findings\nNone", &TurnPolicy{
		Contract: &ContractSpec{Named: "delegate/delegate-report@1"},
		Retry:    &RetryPolicy{Max: 1, Template: correctiveRetryTemplate},
	}, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	if got.Stamp == nil || got.Stamp.Status != ContractCompliant || got.Stamp.ContractName != "delegate/delegate-report@1" || got.Stamp.Attempts != 1 {
		t.Fatalf("stamp = %#v", got.Stamp)
	}
	if got.ResolvedContract == nil || got.ResolvedContract.Named != "" || got.ResolvedContract.Shape == nil {
		t.Fatalf("resolved contract = %#v", got.ResolvedContract)
	}
	passthrough, err := ValidatePolicyText("anything", nil, registry, now)
	if err != nil {
		t.Fatal(err)
	}
	if passthrough.Stamp != nil || passthrough.ResolvedContract != nil {
		t.Fatalf("passthrough = %#v", passthrough)
	}
	_, err = ValidatePolicyText("## Findings\nNone", &TurnPolicy{
		Contract: &spec,
		Retry:    &RetryPolicy{Max: 1, Template: "Missing"},
	}, registry, now)
	if err == nil {
		t.Fatal("invalid retry template succeeded")
	}
	_, err = ValidatePolicyText("## Findings\nNone", &TurnPolicy{
		Contract: &spec,
		Retry:    &RetryPolicy{Max: 1, Template: "Missing {{missing}}"},
	}, registry, now)
	if err == nil {
		t.Fatal("retry template without corrective-only instruction succeeded")
	}
}

func TestRetryTemplateAndSkippedDisabledStamps(t *testing.T) {
	t.Parallel()
	if got := RenderRetryTemplate("missing: {{missing}}", []string{"section:Findings", "evidence"}); got != "missing: Add this top-level heading with its content: # Findings, Findings were claimed without receipts. Add file:line references, commands with exit codes, or output fragments supporting each finding." {
		t.Fatalf("rendered = %q", got)
	}
	retry := RetryPolicy{Max: 1, Template: correctiveRetryTemplate}
	if retry.Max != 1 || !strings.Contains(retry.Template, "{{missing}}") {
		t.Fatalf("retry policy invalid in test")
	}
	noRetry := RetryPolicy{Max: 0}
	if noRetry.Max != 0 {
		t.Fatalf("retry max zero changed")
	}
	for _, reason := range []SkippedReason{SkipTimeout, SkipInterrupt, SkipNoFinalMessage, SkipBackendError, SkipResultUnavailable} {
		stamp := SkippedContractStamp(reason, 1, false, "", "sha256:abc")
		if stamp.Status != ContractSkipped || stamp.Reason != string(reason) || len(stamp.Missing) != 0 {
			t.Fatalf("skipped stamp = %#v", stamp)
		}
	}
	disabled := DisabledContractStamp()
	if disabled.Status != ContractDisabled || disabled.Attempts != 0 || len(disabled.Missing) != 0 {
		t.Fatalf("disabled stamp = %#v", disabled)
	}
}

func TestRenderRetryTemplateTranslatesShapeViolations(t *testing.T) {
	t.Parallel()
	missing := []string{
		"firstLineEnum",
		"section:Criteria scored",
		"attestation:I validated the criteria.",
		"evidence",
		"json: invalid",
	}
	want := "missing: " + strings.Join([]string{
		"Line 1 must be exactly one lowercase word: complete, partial, or blocked — nothing else on the line.",
		"Add this top-level heading with its content: # Criteria scored",
		"Include this exact attestation text outside code fences: I validated the criteria.",
		"Findings were claimed without receipts. Add file:line references, commands with exit codes, or output fragments supporting each finding.",
		"json: invalid",
	}, ", ")
	got := RenderRetryTemplate("missing: {{missing}}", missing)
	if got != want {
		t.Fatalf("rendered = %q, want %q", got, want)
	}
	for _, raw := range []string{"firstLineEnum", "section:"} {
		if strings.Contains(got, raw) {
			t.Fatalf("rendered = %q, should not contain raw token %q", got, raw)
		}
	}

	largeMissing := make([]string, maxRetryViolationTextBytes)
	for i := range largeMissing {
		largeMissing[i] = "firstLineEnum"
	}
	capped := RenderRetryTemplate("{{missing}}{{missing}}", largeMissing)
	if len(capped) > maxRetryViolationTextBytes {
		t.Fatalf("retry violation text is %d bytes, want at most %d", len(capped), maxRetryViolationTextBytes)
	}
	if strings.Contains(capped, "firstLineEnum") {
		t.Fatalf("capped retry prompt leaked raw firstLineEnum token: %q", capped)
	}
}

func TestNoPolicyPassthrough(t *testing.T) {
	t.Parallel()
	var policy *TurnPolicy
	if policy != nil {
		t.Fatal("nil policy should be passthrough with no stamp")
	}
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
