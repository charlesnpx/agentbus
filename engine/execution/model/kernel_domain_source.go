package model

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// CanonicalHostBootID derives the model host-boot token from the raw kernel
// source value, such as /proc/sys/kernel/random/boot_id.
func CanonicalHostBootID(raw string) (string, error) {
	return canonicalKernelDomainToken("kernel_domain.host_boot_id", "boot", raw)
}

// CanonicalPIDNamespaceID derives the model PID namespace token from the raw
// kernel namespace link target, such as /proc/<pid>/ns/pid.
func CanonicalPIDNamespaceID(raw string) (string, error) {
	return canonicalKernelDomainToken("kernel_domain.pid_namespace_id", "pidns", raw)
}

func canonicalKernelDomainToken(field, prefix, raw string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", invalid(field, "is required")
	}
	if !kernelDomainTokenSafe(value) {
		value = tokenizeKernelDomainSource(prefix, value)
	}
	if err := validateToken(field, value); err != nil {
		return "", err
	}
	return value, nil
}

func kernelDomainTokenSafe(value string) bool {
	if len(value) == 0 || len(value) > 256 {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '-' || r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
}

func tokenizeKernelDomainSource(prefix, value string) string {
	var builder strings.Builder
	builder.WriteString(prefix)
	builder.WriteByte('-')
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			builder.WriteRune(r)
		default:
			builder.WriteByte('-')
		}
	}
	out := builder.String()
	if len(out) <= 256 {
		return out
	}
	sum := sha256.Sum256([]byte(value))
	return prefix + "-" + hex.EncodeToString(sum[:16])
}
