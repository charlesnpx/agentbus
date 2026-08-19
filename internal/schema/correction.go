package schema

import "fmt"

const (
	maxCorrectionViolationTextBytes = 2 * 1024

	correctionPromptBody = "Your previous output did not satisfy the required JSON Schema.\n\n" +
		"This turn is READ-ONLY. The worker must make NO further changes — only re-emit the corrected output.\n\n" +
		"Schema violations:\n%s"
)

// CorrectionPrompt returns the fixed, agentbus-owned prompt for the one allowed
// correction attempt. The rendered violations are bounded to keep the prompt finite.
func CorrectionPrompt(violations []string) string {
	return fmt.Sprintf(correctionPromptBody, boundedViolationText(violations, maxCorrectionViolationTextBytes))
}
