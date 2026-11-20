package duplex

import (
	"strings"
	"unicode/utf8"
)

const (
	stderrDiagnosticLimit  = 64 * 1024
	stderrDiagnosticMarker = "[agentbus: stderr truncated]\n"
)

// diagnosticTail retains bounded raw bytes; rendering repairs invalid UTF-8 and
// keeps the most recent evidence even if replacements expand the message.
type diagnosticTail struct {
	data    []byte
	omitted bool
}

func (tail *diagnosticTail) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) >= stderrDiagnosticLimit {
		if len(tail.data) > 0 || len(p) > stderrDiagnosticLimit {
			tail.omitted = true
		}
		tail.data = append(tail.data[:0], p[len(p)-stderrDiagnosticLimit:]...)
		return n, nil
	}
	if overflow := len(tail.data) + len(p) - stderrDiagnosticLimit; overflow > 0 {
		copy(tail.data, tail.data[overflow:])
		tail.data = tail.data[:len(tail.data)-overflow]
		tail.omitted = true
	}
	tail.data = append(tail.data, p...)
	return n, nil
}

func (tail *diagnosticTail) String() string {
	text := strings.ToValidUTF8(string(tail.data), "\uFFFD")
	if !tail.omitted && len(text) <= stderrDiagnosticLimit {
		return text
	}
	allowance := stderrDiagnosticLimit - len(stderrDiagnosticMarker)
	if len(text) > allowance {
		text = text[len(text)-allowance:]
		for len(text) > 0 && !utf8.RuneStart(text[0]) {
			text = text[1:]
		}
	}
	return stderrDiagnosticMarker + text
}
