package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// TurnPolicy describes optional structural validation for a backend turn.
type TurnPolicy struct {
	Prologue string        `json:"prologue,omitempty"`
	Contract *ContractSpec `json:"contract,omitempty"`
	Retry    *RetryPolicy  `json:"retry,omitempty"`
}

// RetryPolicy describes bounded corrective validation retry settings.
type RetryPolicy struct {
	Max      int    `json:"max"`
	Template string `json:"template"`
}

// ContractSpec is exactly one policy contract variant.
type ContractSpec struct {
	JSONSchema json.RawMessage `json:"jsonSchema,omitempty"`
	Shape      *ShapeSpec      `json:"shape,omitempty"`
	Named      string          `json:"named,omitempty"`
}

// ShapeSpec is the protocol v1 shape contract.
type ShapeSpec struct {
	FirstLineEnum        []string `json:"firstLineEnum,omitempty"`
	RequiredSections     []string `json:"requiredSections,omitempty"`
	RequiredAttestations []string `json:"requiredAttestations,omitempty"`
	EvidenceHeuristic    bool     `json:"evidenceHeuristic,omitempty"`
}

// ContractStatus is the contract stamp status enum.
type ContractStatus string

const (
	ContractCompliant    ContractStatus = "compliant"
	ContractRetried      ContractStatus = "retried"
	ContractNoncompliant ContractStatus = "noncompliant"
	ContractSkipped      ContractStatus = "skipped"
	ContractDisabled     ContractStatus = "disabled"
)

// SkippedReason is the protocol v1 contract skipped reason enum.
type SkippedReason string

const (
	SkipTimeout           SkippedReason = "timeout"
	SkipInterrupt         SkippedReason = "interrupt"
	SkipNoFinalMessage    SkippedReason = "no_final_message"
	SkipBackendError      SkippedReason = "backend_error"
	SkipResultUnavailable SkippedReason = "result_unavailable"
)

// ContractStamp records structural compliance for a turn result.
type ContractStamp struct {
	Status         ContractStatus `json:"status"`
	Missing        []string       `json:"missing"`
	Reason         string         `json:"reason"`
	ContractName   string         `json:"contractName,omitempty"`
	ContractSHA256 string         `json:"contractSha256,omitempty"`
	Attempts       int            `json:"attempts"`
	RetryUsed      bool           `json:"retryUsed"`
	ValidatedAt    time.Time      `json:"validatedAt,omitempty"`
}

// ValidationResult is returned by standalone contract validation.
type ValidationResult struct {
	Valid          bool     `json:"valid"`
	Missing        []string `json:"missing"`
	ContractName   string   `json:"contractName,omitempty"`
	ContractSHA256 string   `json:"contractSha256"`
}

// PolicyValidation contains a validation stamp plus the resolved contract for persistence.
type PolicyValidation struct {
	Stamp            *ContractStamp `json:"stamp,omitempty"`
	ResolvedContract *ContractSpec  `json:"resolvedContract,omitempty"`
}

// NameConflictError is returned when a registered policy name is reused for a different spec.
type NameConflictError struct {
	Name string
}

func (e NameConflictError) Error() string {
	return "name_conflict: policy name already registered with different spec: " + e.Name
}

// PolicyRegistry stores immutable named contract specs.
type PolicyRegistry struct {
	mu    sync.RWMutex
	specs map[string]registeredSpec
}

type registeredSpec struct {
	spec ContractSpec
	hash string
}

// NewPolicyRegistry returns an empty in-memory policy registry.
func NewPolicyRegistry() *PolicyRegistry {
	return &PolicyRegistry{specs: make(map[string]registeredSpec)}
}

// Register stores a named immutable contract spec. Identical re-registration is idempotent.
func (r *PolicyRegistry) Register(name string, spec ContractSpec) (string, error) {
	if r == nil {
		return "", errors.New("nil policy registry")
	}
	if err := validatePolicyName(name); err != nil {
		return "", err
	}
	if err := validateConcreteContract(spec); err != nil {
		return "", err
	}
	hash, err := ContractSHA256(spec)
	if err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.specs[name]; ok {
		if existing.hash != hash {
			return "", NameConflictError{Name: name}
		}
		return hash, nil
	}
	r.specs[name] = registeredSpec{spec: cloneContractSpec(spec), hash: hash}
	return hash, nil
}

// Resolve returns the concrete spec for a named contract reference.
func (r *PolicyRegistry) Resolve(name string) (ContractSpec, string, error) {
	if r == nil {
		return ContractSpec{}, "", errors.New("nil policy registry")
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	spec, ok := r.specs[name]
	if !ok {
		return ContractSpec{}, "", fmt.Errorf("policy name not registered: %s", name)
	}
	return cloneContractSpec(spec.spec), spec.hash, nil
}

// ResolveContract resolves named contracts and returns the concrete spec to persist.
// Contract variants are exclusive.
func ResolveContract(contract ContractSpec, registry *PolicyRegistry) (ContractSpec, string, string, error) {
	if err := validateContractVariant(contract); err != nil {
		return ContractSpec{}, "", "", err
	}
	if contract.JSONSchema != nil {
		concrete := ContractSpec{JSONSchema: append(json.RawMessage(nil), contract.JSONSchema...)}
		if err := validateConcreteContract(concrete); err != nil {
			return ContractSpec{}, "", "", err
		}
		hash, err := ContractSHA256(concrete)
		return concrete, "", hash, err
	}
	if contract.Shape != nil {
		concrete := ContractSpec{Shape: cloneShapeSpec(contract.Shape)}
		if err := validateConcreteContract(concrete); err != nil {
			return ContractSpec{}, "", "", err
		}
		hash, err := ContractSHA256(concrete)
		return concrete, "", hash, err
	}
	if contract.Named == "" {
		return ContractSpec{}, "", "", errors.New("contract must include jsonSchema, shape, or named")
	}
	spec, hash, err := registry.Resolve(contract.Named)
	if err != nil {
		return ContractSpec{}, "", "", err
	}
	return spec, contract.Named, hash, nil
}

// ValidateContract validates text against a concrete contract.
func ValidateContract(text string, contract ContractSpec) (ValidationResult, error) {
	if err := validateConcreteContract(contract); err != nil {
		return ValidationResult{}, err
	}
	hash, err := ContractSHA256(contract)
	if err != nil {
		return ValidationResult{}, err
	}
	var missing []string
	switch {
	case contract.Shape != nil:
		missing = validateShape(text, *contract.Shape)
	case contract.JSONSchema != nil:
		missing = validateJSONSchema(text, contract.JSONSchema)
	default:
		return ValidationResult{}, errors.New("named contract must be resolved before validation")
	}
	return ValidationResult{Valid: len(missing) == 0, Missing: missing, ContractSHA256: hash}, nil
}

// ValidatePolicyText validates complete final result text and returns the stamp and resolved spec.
// A nil policy or a policy without a contract is pass-through and returns no stamp.
func ValidatePolicyText(text string, policy *TurnPolicy, registry *PolicyRegistry, now time.Time) (PolicyValidation, error) {
	if policy == nil || policy.Contract == nil {
		return PolicyValidation{}, nil
	}
	if err := validateRetryPolicy(policy.Retry); err != nil {
		return PolicyValidation{}, err
	}
	resolved, name, _, err := ResolveContract(*policy.Contract, registry)
	if err != nil {
		return PolicyValidation{}, err
	}
	result, err := ValidateContract(text, resolved)
	if err != nil {
		return PolicyValidation{}, err
	}
	stamp := StampValidation(1, false, name, result, now)
	return PolicyValidation{Stamp: &stamp, ResolvedContract: &resolved}, nil
}

const (
	maxSchemaViolationEntries      = 20
	maxSchemaViolationPointerRunes = 120
	maxSchemaViolationMessageRunes = 200
	maxRetryViolationTextBytes     = 2 * 1024
)

// RenderRetryTemplate renders the corrective retry template. The substituted
// violation text is capped so invalid responses cannot inflate a retry prompt.
func RenderRetryTemplate(template string, missing []string) string {
	const missingToken = "{{missing}}"
	occurrences := strings.Count(template, missingToken)
	if occurrences == 0 {
		return template
	}
	return strings.ReplaceAll(template, missingToken, boundedViolationText(missing, maxRetryViolationTextBytes/occurrences))
}

// StampValidation builds a contract stamp for a completed validation pipeline.
func StampValidation(attempts int, retryUsed bool, contractName string, result ValidationResult, now time.Time) ContractStamp {
	status := ContractCompliant
	reason := ""
	if retryUsed && result.Valid {
		status = ContractRetried
		reason = "initial response missed structural requirements; retry satisfied contract"
	} else if !result.Valid {
		status = ContractNoncompliant
		reason = "response missed structural requirements"
	}
	return ContractStamp{
		Status:         status,
		Missing:        append([]string(nil), result.Missing...),
		Reason:         reason,
		ContractName:   contractName,
		ContractSHA256: result.ContractSHA256,
		Attempts:       attempts,
		RetryUsed:      retryUsed,
		ValidatedAt:    now.UTC(),
	}
}

// SkippedContractStamp builds a skipped contract stamp for an allowed skipped reason.
func SkippedContractStamp(reason SkippedReason, attempts int, retryUsed bool, contractName string, contractSHA256 string) ContractStamp {
	return ContractStamp{
		Status:         ContractSkipped,
		Missing:        []string{},
		Reason:         string(reason),
		ContractName:   contractName,
		ContractSHA256: contractSHA256,
		Attempts:       attempts,
		RetryUsed:      retryUsed,
	}
}

// DisabledContractStamp builds a client-requested disabled stamp.
func DisabledContractStamp() ContractStamp {
	return ContractStamp{Status: ContractDisabled, Missing: []string{}, Attempts: 0}
}

// ContractSHA256 returns sha256:<hex> over canonical JSON for the resolved spec.
func ContractSHA256(contract ContractSpec) (string, error) {
	canonical, err := canonicalJSON(contract)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func validateConcreteContract(contract ContractSpec) error {
	if err := validateContractVariant(contract); err != nil {
		return err
	}
	if contract.JSONSchema != nil {
		if !json.Valid(contract.JSONSchema) {
			return errors.New("jsonSchema must be valid JSON")
		}
		if _, err := compileJSONSchema(contract.JSONSchema); err != nil {
			return fmt.Errorf("jsonSchema must be valid Draft 2020-12 schema: %w", err)
		}
	}
	if contract.Named != "" {
		return errors.New("named contract must be resolved before validation")
	}
	return nil
}

func validateContractVariant(contract ContractSpec) error {
	count := 0
	if contract.JSONSchema != nil {
		count++
	}
	if contract.Shape != nil {
		count++
	}
	if contract.Named != "" {
		count++
	}
	if count != 1 {
		return errors.New("contract must include exactly one of jsonSchema, shape, or named")
	}
	return nil
}

func validateRetryPolicy(retry *RetryPolicy) error {
	if retry == nil {
		return nil
	}
	if retry.Max != 0 && retry.Max != 1 {
		return errors.New("retry.max must be 0 or 1")
	}
	if retry.Max == 1 {
		if !strings.Contains(retry.Template, "{{missing}}") {
			return errors.New("retry.template must include {{missing}} when retry.max is 1")
		}
		normalized := strings.ToLower(retry.Template)
		if !strings.Contains(normalized, "emit the corrected report only") ||
			!strings.Contains(normalized, "make no further changes") {
			return errors.New("retry.template must instruct the backend to emit the corrected report only and make no further changes when retry.max is 1")
		}
	}
	return nil
}

func cloneContractSpec(spec ContractSpec) ContractSpec {
	clone := ContractSpec{Named: spec.Named}
	if spec.JSONSchema != nil {
		clone.JSONSchema = append(json.RawMessage(nil), spec.JSONSchema...)
	}
	clone.Shape = cloneShapeSpec(spec.Shape)
	return clone
}

func cloneShapeSpec(spec *ShapeSpec) *ShapeSpec {
	if spec == nil {
		return nil
	}
	return &ShapeSpec{
		FirstLineEnum:        append([]string(nil), spec.FirstLineEnum...),
		RequiredSections:     append([]string(nil), spec.RequiredSections...),
		RequiredAttestations: append([]string(nil), spec.RequiredAttestations...),
		EvidenceHeuristic:    spec.EvidenceHeuristic,
	}
}

func validatePolicyName(name string) error {
	if strings.HasPrefix(name, "sha256:") {
		hash := strings.TrimPrefix(name, "sha256:")
		if len(hash) != 64 {
			return fmt.Errorf("invalid sha256 policy name: %s", name)
		}
		for _, r := range hash {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
				return fmt.Errorf("invalid sha256 policy name: %s", name)
			}
		}
		return nil
	}
	if ok, _ := regexp.MatchString(`^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+@[A-Za-z0-9._-]+$`, name); !ok {
		return fmt.Errorf("invalid policy name: %s", name)
	}
	return nil
}

var ansiRE = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)
var findingItemLabelRE = regexp.MustCompile(`(?i)^p[0-9]+$`)

func stripANSI(s string) string {
	return ansiRE.ReplaceAllString(s, "")
}

func validateShape(text string, spec ShapeSpec) []string {
	raw := stripANSI(text)
	var missing []string
	if len(spec.FirstLineEnum) > 0 {
		first := firstLine(raw)
		found := false
		for _, allowed := range spec.FirstLineEnum {
			if first == allowed {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, "firstLineEnum")
		}
	}

	outside := outsideFences(raw)
	for _, section := range spec.RequiredSections {
		if !hasSection(outside, section) {
			missing = append(missing, "section:"+section)
		}
	}
	for _, attestation := range spec.RequiredAttestations {
		if !strings.Contains(outside, attestation) {
			missing = append(missing, "attestation:"+attestation)
		}
	}
	if spec.EvidenceHeuristic && claimsFindings(outside) && !hasEvidence(raw, outside) {
		missing = append(missing, "evidence")
	}
	return missing
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		line := s[:i]
		return strings.TrimSuffix(line, "\r")
	}
	return strings.TrimSuffix(s, "\r")
}

func outsideFences(raw string) string {
	lines := splitLines(raw)
	var b strings.Builder
	var fence *fenceMarker
	for _, line := range lines {
		if fence != nil {
			if isClosingFence(line, *fence) {
				fence = nil
			}
			continue
		}
		if marker, ok := openingFence(line); ok {
			fence = &marker
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func splitLines(s string) []string {
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\r", "\n")
	return strings.Split(s, "\n")
}

type fenceMarker struct {
	char byte
	run  int
}

func openingFence(line string) (fenceMarker, bool) {
	spaces := 0
	for spaces < len(line) && line[spaces] == ' ' {
		spaces++
	}
	if spaces > 3 {
		return fenceMarker{}, false
	}
	line = line[spaces:]
	if len(line) < 3 || (line[0] != '`' && line[0] != '~') {
		return fenceMarker{}, false
	}
	run := fenceRun(line, line[0])
	if run < 3 {
		return fenceMarker{}, false
	}
	return fenceMarker{char: line[0], run: run}, true
}

func isClosingFence(line string, opener fenceMarker) bool {
	spaces := 0
	for spaces < len(line) && line[spaces] == ' ' {
		spaces++
	}
	if spaces > 3 {
		return false
	}
	line = line[spaces:]
	run := fenceRun(line, opener.char)
	if run < opener.run {
		return false
	}
	return strings.TrimSpace(line[run:]) == ""
}

func fenceRun(line string, char byte) int {
	run := 0
	for run < len(line) && line[run] == char {
		run++
	}
	return run
}

type sectionMarker struct {
	index int
	name  string
	rest  string
}

func hasSection(outside string, section string) bool {
	want := strings.ToLower(strings.TrimSpace(section))
	for _, marker := range sectionMarkers(outside) {
		if strings.ToLower(strings.TrimSpace(marker.name)) == want {
			return true
		}
	}
	return false
}

func sectionMarkers(outside string) []sectionMarker {
	lines := splitLines(outside)
	markers := make([]sectionMarker, 0)
	for i, line := range lines {
		if name, ok := headingName(line); ok {
			markers = append(markers, sectionMarker{index: i, name: name})
			continue
		}
		if name, rest, ok := labelName(line); ok {
			markers = append(markers, sectionMarker{index: i, name: name, rest: rest})
		}
	}
	return markers
}

func headingName(line string) (string, bool) {
	n := 0
	for n < len(line) && line[n] == '#' {
		n++
	}
	if n == 0 || n > 4 || len(line) == n || (line[n] != ' ' && line[n] != '\t') {
		return "", false
	}
	name := strings.TrimSpace(line[n:])
	name = strings.TrimSpace(strings.TrimRight(name, "#"))
	return name, name != ""
}

func labelName(line string) (string, string, bool) {
	if len(line) == 0 || line[0] == ' ' || line[0] == '\t' {
		return "", "", false
	}
	i := strings.IndexByte(line, ':')
	if i <= 0 {
		return "", "", false
	}
	name := strings.TrimSpace(line[:i])
	if name == "" {
		return "", "", false
	}
	return name, strings.TrimSpace(line[i+1:]), true
}

func claimsFindings(outside string) bool {
	lines := splitLines(outside)
	markers := sectionMarkers(outside)
	for mi, marker := range markers {
		if !strings.EqualFold(strings.TrimSpace(marker.name), "Findings") {
			continue
		}
		body := []string{}
		if marker.rest != "" {
			body = append(body, marker.rest)
		}
		end := findingsBodyEnd(lines, markers, mi)
		if marker.index+1 < end {
			body = append(body, lines[marker.index+1:end]...)
		}
		for _, line := range body {
			normalized := strings.ToLower(strings.TrimSpace(line))
			if normalized == "" || normalized == "none" || normalized == "no findings" || normalized == "n/a" || normalized == "not applicable" {
				continue
			}
			return true
		}
		return false
	}
	return false
}

func findingsBodyEnd(lines []string, markers []sectionMarker, markerIndex int) int {
	for i := markerIndex + 1; i < len(markers); i++ {
		marker := markers[i]
		if _, ok := headingName(lines[marker.index]); ok {
			return marker.index
		}
		if marker.rest == "" && !isFindingItemLabel(marker.name) {
			return marker.index
		}
	}
	return len(lines)
}

func isFindingItemLabel(name string) bool {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return false
	}
	if findingItemLabelRE.MatchString(normalized) {
		return true
	}
	switch normalized {
	case "critical", "high", "medium", "low", "info", "informational":
		return true
	default:
		return false
	}
}

var (
	pathLineRE = regexp.MustCompile(`(?:^|[[:space:]])[^[:space:]:]+:\d+(?:$|[[:space:].,;)])`)
	exitCodeRE = regexp.MustCompile(`(?i)\bexit(?:\s+code)?\s+\d+\b`)
	diffHunkRE = regexp.MustCompile(`(?m)^@@\s`)
)

func hasEvidence(raw, outside string) bool {
	if pathLineRE.MatchString(outside) || diffHunkRE.MatchString(outside) {
		return true
	}
	lines := splitLines(raw)
	for i := 0; i < len(lines); i++ {
		marker, ok := openingFence(lines[i])
		if !ok {
			continue
		}
		start := i
		i++
		for i < len(lines) && !isClosingFence(lines[i], marker) {
			i++
		}
		if i >= len(lines) {
			break
		}
		before := ""
		after := ""
		if start > 0 {
			before = lines[start-1]
		}
		if i+1 < len(lines) {
			after = lines[i+1]
		}
		if exitCodeRE.MatchString(before) || exitCodeRE.MatchString(after) {
			return true
		}
	}
	return false
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
	var out bytes.Buffer
	writeCanonical(&out, decoded)
	return out.Bytes(), nil
}

func writeCanonical(out *bytes.Buffer, v any) {
	switch x := v.(type) {
	case nil:
		out.WriteString("null")
	case bool:
		if x {
			out.WriteString("true")
		} else {
			out.WriteString("false")
		}
	case float64:
		out.WriteString(strconv.FormatFloat(x, 'f', -1, 64))
	case json.Number:
		out.WriteString(x.String())
	case string:
		b, _ := json.Marshal(x)
		out.Write(b)
	case []any:
		out.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				out.WriteByte(',')
			}
			writeCanonical(out, item)
		}
		out.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				out.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			out.Write(kb)
			out.WriteByte(':')
			writeCanonical(out, x[k])
		}
		out.WriteByte('}')
	}
}

func validateJSONSchema(text string, schemaRaw json.RawMessage) []string {
	var value any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decodeJSONDocument(decoder, &value); err != nil {
		return []string{"json: " + truncateRunes(err.Error(), maxSchemaViolationMessageRunes)}
	}
	schema, err := compileJSONSchema(schemaRaw)
	if err != nil {
		return []string{"jsonSchema: " + truncateRunes(err.Error(), maxSchemaViolationMessageRunes)}
	}
	if err := schema.Validate(value); err != nil {
		return jsonSchemaViolations(err)
	}
	return nil
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

// schemaViolation formats a schema validation leaf for persistence and retry
// prompts. Both components must be printable because property names and schema
// diagnostics originate from untrusted JSON.
func schemaViolation(pointer, message string) string {
	pointer = truncateRunesToLimit(escapeNonPrintable(pointer), maxSchemaViolationPointerRunes)
	message = truncateRunesToLimit(escapeNonPrintable(message), maxSchemaViolationMessageRunes)
	return pointer + ": " + message
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
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	const schemaURL = "agentbus://policy.schema.json"
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
