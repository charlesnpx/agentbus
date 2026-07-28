package served

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

const servedTestSandboxBindDeniedEnv = "AGENTBUS_TEST_SANDBOX_BIND_DENIED"

func servedTestBindDeniedOutput(output string) bool {
	output = strings.ToLower(output)
	return strings.Contains(output, "bind: operation not permitted") ||
		strings.Contains(output, "bind: permission denied") ||
		strings.Contains(output, "unix socket bind denied by sandbox")
}

func servedTestSkipOrFailBindDenied(t *testing.T, context string, detail any) {
	t.Helper()
	if os.Getenv(servedTestSandboxBindDeniedEnv) == "1" {
		t.Skipf("Unix socket bind denied by sandbox in %s (%s=1): %v", context, servedTestSandboxBindDeniedEnv, detail)
	}
	t.Fatalf("Unix socket bind denied in %s without %s=1; failing to expose daemon bind regressions: %v", context, servedTestSandboxBindDeniedEnv, detail)
}

func servedTestSkipOrFailBindDeniedf(t *testing.T, context, format string, args ...any) {
	t.Helper()
	servedTestSkipOrFailBindDenied(t, context, fmt.Sprintf(format, args...))
}
