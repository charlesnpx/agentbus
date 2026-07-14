package model

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var ErrInvalidValue = errors.New("invalid model value")

type ValidationError struct {
	Field  string
	Reason string
}

func (e ValidationError) Error() string {
	if e.Field == "" {
		return e.Reason
	}
	return fmt.Sprintf("%s: %s", e.Field, e.Reason)
}

func (e ValidationError) Unwrap() error {
	return ErrInvalidValue
}

func invalid(field, reason string) error {
	return ValidationError{Field: field, Reason: reason}
}

func validateToken(field, value string) error {
	const maxTokenBytes = 256
	if value == "" {
		return invalid(field, "is required")
	}
	if len(value) > maxTokenBytes {
		return invalid(field, "is too long")
	}
	if !utf8.ValidString(value) {
		return invalid(field, "must be valid UTF-8")
	}
	for _, r := range value {
		if r <= ' ' || r == 0x7f {
			return invalid(field, "must not contain whitespace or control characters")
		}
	}
	return nil
}

func validateOptionalToken(field, value string) error {
	if value == "" {
		return nil
	}
	return validateToken(field, value)
}

func validateText(field, value string, maxBytes int) error {
	if value == "" {
		return invalid(field, "is required")
	}
	if len(value) > maxBytes {
		return invalid(field, "is too long")
	}
	if !utf8.ValidString(value) {
		return invalid(field, "must be valid UTF-8")
	}
	if strings.IndexFunc(value, func(r rune) bool {
		return r == 0 || r == '\r' || r == '\n'
	}) >= 0 {
		return invalid(field, "must not contain NUL or line breaks")
	}
	return nil
}

func validateNonNegativeInt64(field string, value int64) error {
	if value < 0 {
		return invalid(field, "must be non-negative")
	}
	return nil
}

func validatePositiveInt(field string, value int) error {
	if value <= 0 {
		return invalid(field, "must be positive")
	}
	return nil
}

func validatePositiveUint64(field string, value uint64) error {
	if value == 0 {
		return invalid(field, "must be positive")
	}
	return nil
}

func validatePositiveUint16(field string, value uint16) error {
	if value == 0 {
		return invalid(field, "must be positive")
	}
	return nil
}

func validateAttemptField(field string, got, want AttemptRef) error {
	if !got.Equal(want) {
		return invalid(field, "attempt identity mismatch")
	}
	return nil
}

func validateJobField(field string, got, want JobID) error {
	if got != want {
		return invalid(field, "job identity mismatch")
	}
	return nil
}
