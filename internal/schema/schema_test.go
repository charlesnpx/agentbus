package schema

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode"
	"unicode/utf8"
)

func TestValidateCompliantDocument(t *testing.T) {
	result, err := Validate(`{"status":"ok","items":["one"]}`, json.RawMessage(`{
		"type":"object",
		"required":["status","items"],
		"properties":{
			"status":{"const":"ok"},
			"items":{"type":"array","minItems":1,"items":{"type":"string"}}
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Evaluated || !result.Compliant || len(result.Violations) != 0 {
		t.Fatalf("result = %+v, want evaluated compliant result without violations", result)
	}
}

func TestValidateNoncompliantDocumentReportsJSONPointers(t *testing.T) {
	result, err := Validate(`{"payload":{"name":7}}`, json.RawMessage(`{
		"type":"object",
		"required":["payload"],
		"properties":{"payload":{
			"type":"object",
			"required":["name"],
			"properties":{"name":{"type":"string"}}
		}}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if !result.Evaluated || result.Compliant || len(result.Violations) == 0 {
		t.Fatalf("result = %+v, want evaluated noncompliant result with violations", result)
	}
	for _, violation := range result.Violations {
		if !strings.HasPrefix(violation, "/") || !strings.Contains(violation, ": ") {
			t.Fatalf("violation = %q, want JSON Pointer followed by diagnostic", violation)
		}
	}
}

func TestValidateBoundsViolationOutput(t *testing.T) {
	required := make([]string, 25)
	allOf := make([]any, len(required))
	for i := range required {
		required[i] = fmt.Sprintf("field-%02d", i)
		allOf[i] = map[string]any{"required": []string{required[i]}}
	}
	manySchema, err := json.Marshal(map[string]any{"allOf": allOf})
	if err != nil {
		t.Fatal(err)
	}
	many, err := Validate(`{}`, manySchema)
	if err != nil {
		t.Fatal(err)
	}
	if len(many.Violations) != maxSchemaViolationEntries {
		t.Fatalf("violation count = %d, want %d: %#v", len(many.Violations), maxSchemaViolationEntries, many.Violations)
	}
	if tail := many.Violations[len(many.Violations)-1]; !strings.HasPrefix(tail, "+") || !strings.Contains(tail, " more schema violations") {
		t.Fatalf("tail = %q, want +N more schema violations", tail)
	}

	longPointerName := strings.Repeat("p", maxSchemaViolationPointerRunes+20)
	pointerSchema, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			longPointerName: map[string]any{"type": "string"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	pointerDocument, err := json.Marshal(map[string]any{longPointerName: 7})
	if err != nil {
		t.Fatal(err)
	}
	pointerResult, err := Validate(string(pointerDocument), pointerSchema)
	if err != nil {
		t.Fatal(err)
	}
	if len(pointerResult.Violations) != 1 {
		t.Fatalf("pointer violations = %#v, want one", pointerResult.Violations)
	}
	pointer, _, found := strings.Cut(pointerResult.Violations[0], ": ")
	if !found || utf8.RuneCountInString(pointer) > maxSchemaViolationPointerRunes || !strings.HasSuffix(pointer, "...") {
		t.Fatalf("pointer = %q, want truncated pointer of at most %d runes", pointer, maxSchemaViolationPointerRunes)
	}

	longMessageName := strings.Repeat("m", maxSchemaViolationMessageRunes+20)
	messageSchema, err := json.Marshal(map[string]any{"type": "object", "required": []string{longMessageName}})
	if err != nil {
		t.Fatal(err)
	}
	messageResult, err := Validate(`{}`, messageSchema)
	if err != nil {
		t.Fatal(err)
	}
	if len(messageResult.Violations) != 1 {
		t.Fatalf("message violations = %#v, want one", messageResult.Violations)
	}
	_, diagnostic, found := strings.Cut(messageResult.Violations[0], ": ")
	if !found || utf8.RuneCountInString(diagnostic) > maxSchemaViolationMessageRunes || !strings.HasSuffix(diagnostic, "...") {
		t.Fatalf("diagnostic = %q, want truncated message of at most %d runes", diagnostic, maxSchemaViolationMessageRunes)
	}
}

func TestValidateEscapesNonPrintableViolationText(t *testing.T) {
	propertyName := "new\nline"
	schemaRaw, err := json.Marshal(map[string]any{
		"type": "object",
		"properties": map[string]any{
			propertyName: map[string]any{"type": "string"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	document, err := json.Marshal(map[string]any{propertyName: 7})
	if err != nil {
		t.Fatal(err)
	}
	result, err := Validate(string(document), schemaRaw)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Violations) != 1 {
		t.Fatalf("violations = %#v, want one", result.Violations)
	}
	violation := result.Violations[0]
	if strings.Contains(violation, "\n") || !strings.Contains(violation, `\u000A`) {
		t.Fatalf("violation = %q, want escaped newline", violation)
	}
	for _, r := range violation {
		if !unicode.IsPrint(r) {
			t.Fatalf("violation contains non-printable %U: %q", r, violation)
		}
	}
}

func TestDigestIsCanonicalAndDistinct(t *testing.T) {
	first, err := Digest(json.RawMessage(`{
		"type": "object",
		"properties": {"name": {"type": "string"}},
		"required": ["name"]
	}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := Digest(json.RawMessage(`{"required":["name"],"properties":{"name":{"type":"string"}},"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	different, err := Digest(json.RawMessage(`{"type":"object","properties":{"name":{"type":"number"}},"required":["name"]}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first, "sha256:") || first != second || first == different {
		t.Fatalf("digests = first %q second %q different %q", first, second, different)
	}
}

func TestCorrectionPromptIsFixedAndReadOnly(t *testing.T) {
	violations := []string{"/payload/name: got number, want string"}
	first := CorrectionPrompt(violations)
	second := CorrectionPrompt(violations)
	if first != second {
		t.Fatalf("prompts differ: first %q second %q", first, second)
	}
	for _, want := range []string{"READ-ONLY", "NO further changes", violations[0]} {
		if !strings.Contains(first, want) {
			t.Fatalf("prompt = %q, want %q", first, want)
		}
	}
}

func TestValidateInvalidSchemaFailsClosed(t *testing.T) {
	result, err := Validate(`{}`, json.RawMessage(`{"type":42}`))
	if err == nil {
		t.Fatalf("result = %+v, want invalid schema error", result)
	}
	if !result.Evaluated || result.Compliant {
		t.Fatalf("result = %+v, want evaluated noncompliant result", result)
	}
}
