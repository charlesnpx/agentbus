// Package cliadapter adapts backend CLI streams to engine sessions.
//
// cliadapter is process-free: it validates in-memory backend descriptors, builds
// command specs, and parses streams. Executable probing and command launch are
// delegated through engine/command runner interfaces.
package cliadapter
