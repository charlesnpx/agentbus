package schema

const (
	maxCorrectionViolationTextBytes = 2 * 1024

	correctionPromptText = "The preceding final result did not satisfy the required JSON Schema. Return a\n" +
		"replacement final result that satisfies it. This is the one permitted\n" +
		"correction attempt. It is read-only: make no further changes. Do not edit\n" +
		"files, write data, invoke tools, or alter the workspace or external systems."
)

// CorrectionPrompt returns the fixed, agentbus-owned prompt for the one allowed
// correction attempt. canonicalSchema is the canonical submitted JSON Schema.
// The rendered violations are bounded to keep the prompt finite.
func CorrectionPrompt(canonicalSchema string, violations []string) string {
	return correctionPromptText + "\n\nSchema:\n" + canonicalSchema + "\n\nSchema violations:\n" +
		boundedViolationText(violations, maxCorrectionViolationTextBytes)
}
