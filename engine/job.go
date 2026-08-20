package engine

import (
	"encoding/json"
	"math"
)

// FailureReasonMaxRunes is the maximum length retained by the served backend-
// error sanitizer.
const FailureReasonMaxRunes = 512

// TransportFrameDrops records bounded metadata about backend JSONL frames
// discarded because they exceeded the configured transport limit. The prefix
// is a redacted frame-type summary, never backend payload text.
type TransportFrameDrops struct {
	Count          uint64 `json:"count,omitempty"`
	Bytes          uint64 `json:"bytes,omitempty"`
	RedactedPrefix string `json:"redactedPrefix,omitempty"`
}

// Empty reports whether no backend transport frame was discarded.
func (drops TransportFrameDrops) Empty() bool {
	return drops.Count == 0
}

// Merge adds independently observed dropped-frame counters. It saturates on
// overflow because transport diagnostics must remain bounded and non-fatal.
func (drops *TransportFrameDrops) Merge(other TransportFrameDrops) {
	if drops == nil || other.Empty() {
		return
	}
	if math.MaxUint64-drops.Count < other.Count {
		drops.Count = math.MaxUint64
	} else {
		drops.Count += other.Count
	}
	if math.MaxUint64-drops.Bytes < other.Bytes {
		drops.Bytes = math.MaxUint64
	} else {
		drops.Bytes += other.Bytes
	}
	if other.RedactedPrefix != "" {
		drops.RedactedPrefix = other.RedactedPrefix
	}
}

// TransportFrameDropsMetadataKey identifies dropped-frame metadata carried on
// an in-process adapter warning event.
const TransportFrameDropsMetadataKey = "agentbusTransportFrameDrops"

// EventMetadata returns the bounded transport-drop metadata carried with an
// adapter warning event.
func (drops TransportFrameDrops) EventMetadata() map[string]any {
	if drops.Empty() {
		return nil
	}
	return map[string]any{
		TransportFrameDropsMetadataKey: map[string]any{
			"count":          drops.Count,
			"bytes":          drops.Bytes,
			"redactedPrefix": drops.RedactedPrefix,
		},
	}
}

// TransportFrameDropsFromMetadata reads transport-drop metadata from an
// in-process adapter warning event. Invalid metadata is ignored.
func TransportFrameDropsFromMetadata(metadata map[string]any) (TransportFrameDrops, bool) {
	if len(metadata) == 0 {
		return TransportFrameDrops{}, false
	}
	raw, ok := metadata[TransportFrameDropsMetadataKey].(map[string]any)
	if !ok {
		return TransportFrameDrops{}, false
	}
	count, ok := metadataUint64(raw["count"])
	if !ok || count == 0 {
		return TransportFrameDrops{}, false
	}
	bytes, ok := metadataUint64(raw["bytes"])
	if !ok || bytes == 0 {
		return TransportFrameDrops{}, false
	}
	prefix, _ := raw["redactedPrefix"].(string)
	return TransportFrameDrops{Count: count, Bytes: bytes, RedactedPrefix: prefix}, true
}

func metadataUint64(value any) (uint64, bool) {
	switch value := value.(type) {
	case uint64:
		return value, true
	case uint:
		return uint64(value), true
	case uint32:
		return uint64(value), true
	case int:
		if value >= 0 {
			return uint64(value), true
		}
	case int64:
		if value >= 0 {
			return uint64(value), true
		}
	case float64:
		if value >= 0 && value <= math.MaxUint64 && value == math.Trunc(value) {
			return uint64(value), true
		}
	case json.Number:
		parsed, err := value.Int64()
		if err == nil && parsed >= 0 {
			return uint64(parsed), true
		}
	}
	return 0, false
}

// ProcessRef records enough process identity to detect PID reuse.
type ProcessRef struct {
	PID       int    `json:"pid,omitempty"`
	PGID      int    `json:"pgid,omitempty"`
	StartTime string `json:"startTime,omitempty"`
}

// LogPaths identifies captured backend log files.
type LogPaths struct {
	Stdout string `json:"stdout,omitempty"`
	Stderr string `json:"stderr,omitempty"`
}

// ResultInfo describes the authoritative spilled final result.
type ResultInfo struct {
	Text          string `json:"text,omitempty"`
	TextElided    bool   `json:"textElided,omitempty"`
	ResultPath    string `json:"resultPath"`
	SHA256        string `json:"sha256"`
	Bytes         int64  `json:"bytes"`
	ModelReported string `json:"modelReported,omitempty"`
}

const (
	// TimeoutSourceClient means the effective timeout came from taskSpec.timeoutMs.
	TimeoutSourceClient = "client"
	// TimeoutSourceDaemonDefault means the effective timeout came from the daemon default.
	TimeoutSourceDaemonDefault = "daemon_default"
)

// TimeoutResolution records the timeout applied to a job in milliseconds.
// Requested is present only when the client supplied taskSpec.timeoutMs;
// Effective is the duration used for each runAttempt invocation; Source
// identifies whether that duration came from the client or the daemon default.
type TimeoutResolution struct {
	Requested *int64 `json:"requested,omitempty"`
	Effective int64  `json:"effective"`
	Source    string `json:"source"`
}

// CloneTimeoutResolution returns an independent copy suitable for a durable
// record or wire response.
func CloneTimeoutResolution(resolution *TimeoutResolution) *TimeoutResolution {
	if resolution == nil {
		return nil
	}
	copy := *resolution
	if resolution.Requested != nil {
		requested := *resolution.Requested
		copy.Requested = &requested
	}
	return &copy
}
