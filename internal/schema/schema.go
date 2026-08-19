// Package schema validates JSON documents against optional inline JSON Schema.
package schema

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

const (
	maxSchemaViolationEntries      = 20
	maxSchemaViolationPointerRunes = 120
	maxSchemaViolationMessageRunes = 200
	draft2020SchemaURI             = "https://json-schema.org/draft/2020-12/schema"
)

// Result is the outcome of validating one document against an optional schema.
// A missing schema is not evaluated and leaves the document compliant.
type Result struct {
	Evaluated  bool     `json:"evaluated"`
	Compliant  bool     `json:"compliant"`
	Violations []string `json:"violations,omitempty"`
}

// Validate validates document against schemaRaw. An absent schema is optional and
// therefore does not evaluate the document. A supplied invalid schema fails closed:
// the result is noncompliant and the compilation error is returned.
func Validate(document string, schemaRaw json.RawMessage) (Result, error) {
	if schemaAbsent(schemaRaw) {
		return Result{Compliant: true}, nil
	}

	violations, err := validateJSONSchema(document, schemaRaw)
	if err != nil {
		return Result{Evaluated: true}, fmt.Errorf("compile JSON Schema: %w", err)
	}
	return Result{
		Evaluated:  true,
		Compliant:  len(violations) == 0,
		Violations: violations,
	}, nil
}

// Digest returns sha256:<hex> over canonical JSON for raw.
func Digest(raw json.RawMessage) (string, error) {
	canonical, err := canonicalJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return fmt.Sprintf("sha256:%x", sum), nil
}

func schemaAbsent(schemaRaw json.RawMessage) bool {
	return len(schemaRaw) == 0
}

func canonicalJSON(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.UseNumber()
	if err := decodeJSONDocument(decoder, &decoded); err != nil {
		return nil, err
	}
	return json.Marshal(decoded)
}

func validateJSONSchema(text string, schemaRaw json.RawMessage) ([]string, error) {
	schema, err := compileJSONSchema(schemaRaw)
	if err != nil {
		return nil, err
	}

	var value any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decodeJSONDocument(decoder, &value); err != nil {
		return []string{"json: " + truncateRunes(err.Error(), maxSchemaViolationMessageRunes)}, nil
	}
	if err := schema.Validate(value); err != nil {
		return jsonSchemaViolations(err), nil
	}
	return nil, nil
}

func jsonSchemaViolations(err error) []string {
	var validationErr *jsonschema.ValidationError
	if !errors.As(err, &validationErr) {
		return []string{"jsonSchema: " + truncateRunes(err.Error(), maxSchemaViolationMessageRunes)}
	}

	leaves := make([]*jsonschema.ValidationError, 0)
	collectJSONSchemaLeaves(validationErr, &leaves)
	violations := make([]string, 0, len(leaves))
	seen := make(map[string]struct{}, len(leaves))
	printer := message.NewPrinter(language.English)
	for _, leaf := range leaves {
		violationMessage := leaf.Error()
		if leaf.ErrorKind != nil {
			violationMessage = leaf.ErrorKind.LocalizedString(printer)
		}
		violation := schemaViolation(jsonPointer(leaf.InstanceLocation), violationMessage)
		if _, ok := seen[violation]; ok {
			continue
		}
		seen[violation] = struct{}{}
		violations = append(violations, violation)
	}
	sort.Strings(violations)
	if len(violations) <= maxSchemaViolationEntries {
		return violations
	}
	more := len(violations) - (maxSchemaViolationEntries - 1)
	violations = append([]string(nil), violations[:maxSchemaViolationEntries-1]...)
	return append(violations, fmt.Sprintf("+%d more schema violations", more))
}

func collectJSONSchemaLeaves(err *jsonschema.ValidationError, leaves *[]*jsonschema.ValidationError) {
	if len(err.Causes) == 0 {
		*leaves = append(*leaves, err)
		return
	}
	for _, cause := range err.Causes {
		collectJSONSchemaLeaves(cause, leaves)
	}
}

func jsonPointer(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	var pointer strings.Builder
	for _, part := range parts {
		pointer.WriteByte('/')
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~", "~0"), "/", "~1")
		pointer.WriteString(escapeNonPrintable(part))
	}
	return pointer.String()
}

// schemaViolation formats a schema validation leaf for persistence and correction
// prompts. Both components must be printable because property names and schema
// diagnostics originate from untrusted JSON.
func schemaViolation(pointer, diagnostic string) string {
	pointer = truncateRunesToLimit(escapeNonPrintable(pointer), maxSchemaViolationPointerRunes)
	diagnostic = truncateRunesToLimit(escapeNonPrintable(diagnostic), maxSchemaViolationMessageRunes)
	return pointer + ": " + diagnostic
}

// escapeNonPrintable returns value as printable text suitable for a single-line
// diagnostic. It deliberately preserves printable punctuation so JSON Pointer
// segments retain their meaning.
func escapeNonPrintable(value string) string {
	var escaped strings.Builder
	escaped.Grow(len(value))
	for _, r := range value {
		if unicode.IsPrint(r) && r != 0x7f {
			escaped.WriteRune(r)
			continue
		}
		fmt.Fprintf(&escaped, "\\u%04X", r)
	}
	return escaped.String()
}

func truncateRunes(value string, limit int) string {
	if limit < 1 {
		return ""
	}
	count := 0
	for index := range value {
		if count == limit {
			return value[:index] + "..."
		}
		count++
	}
	return value
}

// truncateRunesToLimit truncates value to at most limit runes, including an
// ellipsis when the value does not fit.
func truncateRunesToLimit(value string, limit int) string {
	if limit < 1 {
		return ""
	}
	if utf8.RuneCountInString(value) <= limit {
		return value
	}
	if limit <= len("...") {
		return strings.Repeat(".", limit)
	}
	return truncateRunes(value, limit-len("..."))
}

func boundedViolationText(violations []string, maxBytes int) string {
	if maxBytes < 1 {
		return ""
	}
	var text strings.Builder
	for _, violation := range violations {
		separator := ""
		if text.Len() > 0 {
			separator = ", "
		}
		if text.Len()+len(separator)+len(violation) <= maxBytes {
			text.WriteString(separator)
			text.WriteString(violation)
			continue
		}
		if text.Len() == 0 {
			return truncateUTF8Bytes(violation, maxBytes)
		}
		if text.Len()+len(", ...") <= maxBytes {
			text.WriteString(", ...")
		}
		break
	}
	return text.String()
}

func truncateUTF8Bytes(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	if maxBytes <= len("...") {
		return strings.Repeat(".", maxBytes)
	}
	prefixLength := maxBytes - len("...")
	for prefixLength > 0 && !utf8.RuneStart(value[prefixLength]) {
		prefixLength--
	}
	return value[:prefixLength] + "..."
}

func compileJSONSchema(schemaRaw json.RawMessage) (*jsonschema.Schema, error) {
	var schemaDoc any
	decoder := json.NewDecoder(bytes.NewReader(schemaRaw))
	decoder.UseNumber()
	if err := decodeJSONDocument(decoder, &schemaDoc); err != nil {
		return nil, err
	}
	switch schemaDoc := schemaDoc.(type) {
	case bool:
	case map[string]any:
		if declaredDialect, ok := schemaDoc["$schema"]; ok {
			dialect, ok := declaredDialect.(string)
			if !ok || dialect != draft2020SchemaURI {
				return nil, fmt.Errorf("JSON Schema $schema must be %q (Draft 2020-12)", draft2020SchemaURI)
			}
		}
	default:
		return nil, errors.New("JSON Schema root must be an object or boolean")
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	const schemaURL = "agentbus://inline-schema.json"
	if err := compiler.AddResource(schemaURL, schemaDoc); err != nil {
		return nil, err
	}
	return compiler.Compile(schemaURL)
}

func decodeJSONDocument(decoder *json.Decoder, dst *any) error {
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}
