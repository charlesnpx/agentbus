package served

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/execution/model"
)

// partialResultArtifactMaxBytes bounds a synthesized transcript excerpt while
// retaining its most recent assistant-visible messages.
const partialResultArtifactMaxBytes = 256 * 1024

const (
	partialResultTimeoutHeader     = "[agentbus: partial result: job timed out; this transcript excerpt is not the worker's final report]\n\n"
	partialResultInterruptedHeader = "[agentbus: partial result: job was interrupted; this transcript excerpt is not the worker's final report]\n\n"
	partialResultElisionNotice     = "[agentbus: earlier transcript content was elided]\n\n"
)

type partialTranscriptEvent struct {
	Method string `json:"method"`
	Params struct {
		Item struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"item"`
	} `json:"params"`
}

func (s *Server) synthesizeAdmissionPartialResult(ctx context.Context, jobID model.JobID, record model.SafetyRecord, stdoutPath string, state engine.JobState) (*model.ResultReceipt, error) {
	if record.Result != nil || record.Terminal != nil {
		return nil, nil
	}
	reason, _, ok := partialResultMetadataForState(state)
	if !ok || stdoutPath == "" {
		return nil, nil
	}
	layout, err := authorityResultLayout(s.stateRoot, record)
	if err != nil {
		return nil, err
	}
	path, err := engine.ResultPathForLayout(layout, jobID.String())
	if err != nil {
		return nil, err
	}
	missing, err := partialResultArtifactMissing(path)
	if err != nil {
		return nil, err
	}
	if !missing {
		return nil, nil
	}
	payload, recovered, err := partialResultArtifact(stdoutPath, state)
	if err != nil || !recovered {
		return nil, err
	}
	publisher := servedResultPublisher{server: s}
	published, err := publisher.Publish(ctx, jobID, payload)
	if err != nil {
		return nil, err
	}
	verified, err := publisher.Verify(ctx, published.Result)
	if err != nil {
		return nil, err
	}
	verified.Result.Partial = true
	verified.Result.PartialReason = reason
	return &verified, nil
}

func (s *Server) synthesizeLegacyPartialResult(run jobRun, record *engine.JobRecord, state engine.JobState) (*engine.ResultInfo, error) {
	if run.store == nil || record == nil || record.Result != nil {
		return nil, nil
	}
	reason, _, ok := partialResultMetadataForState(state)
	if !ok || run.logPaths.Stdout == "" {
		return nil, nil
	}
	path, err := engine.ResultPathForLayout(run.store.Layout(), run.jobID)
	if err != nil {
		return nil, err
	}
	missing, err := partialResultArtifactMissing(path)
	if err != nil {
		return nil, err
	}
	if !missing {
		return nil, nil
	}
	payload, recovered, err := partialResultArtifact(run.logPaths.Stdout, state)
	if err != nil || !recovered {
		return nil, err
	}
	info, err := run.store.WriteResult(run.jobID, payload, s.inlineResultCap)
	if err != nil {
		return nil, err
	}
	info.Partial = true
	info.PartialReason = reason
	return &info, nil
}

func partialResultArtifact(stdoutPath string, state engine.JobState) ([]byte, bool, error) {
	_, header, ok := partialResultMetadataForState(state)
	if !ok {
		return nil, false, nil
	}
	bodyLimit := partialResultArtifactMaxBytes - len(header) - len(partialResultElisionNotice)
	if bodyLimit <= 0 {
		return nil, false, fmt.Errorf("partial result artifact cap %d is too small for its header", partialResultArtifactMaxBytes)
	}
	file, err := os.Open(stdoutPath)
	if err != nil {
		return nil, false, err
	}
	defer file.Close()

	tail := partialTranscriptTail{limit: bodyLimit}
	reader := bufio.NewReader(file)
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) != 0 {
			var event partialTranscriptEvent
			if err := json.Unmarshal(line, &event); err == nil &&
				event.Method == "item/completed" &&
				event.Params.Item.Type == "agentMessage" &&
				event.Params.Item.Text != "" {
				tail.append(event.Params.Item.Text)
			}
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		return nil, false, readErr
	}
	if !tail.hasText {
		return nil, false, nil
	}

	artifact := make([]byte, 0, len(header)+len(partialResultElisionNotice)+len(tail.bytes))
	artifact = append(artifact, header...)
	if tail.elided {
		artifact = append(artifact, partialResultElisionNotice...)
	}
	artifact = append(artifact, tail.bytes...)
	return artifact, true, nil
}

func partialResultArtifactMissing(path string) (bool, error) {
	_, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return false, nil
}

func partialResultMetadataForState(state engine.JobState) (reason, header string, ok bool) {
	switch state {
	case engine.StateTimedOut:
		return model.PartialResultReasonTimeout, partialResultTimeoutHeader, true
	case engine.StateInterrupted:
		return model.PartialResultReasonInterrupted, partialResultInterruptedHeader, true
	default:
		return "", "", false
	}
}

type partialTranscriptTail struct {
	bytes   []byte
	limit   int
	elided  bool
	hasText bool
}

func (tail *partialTranscriptTail) append(text string) {
	if tail.hasText {
		tail.appendBytes([]byte("\n\n"))
	}
	tail.appendBytes([]byte(text))
	tail.hasText = true
}

func (tail *partialTranscriptTail) appendBytes(raw []byte) {
	if len(raw) == 0 {
		return
	}
	if len(tail.bytes)+len(raw) <= tail.limit {
		tail.bytes = append(tail.bytes, raw...)
		return
	}
	tail.elided = true
	merged := make([]byte, 0, len(tail.bytes)+len(raw))
	merged = append(merged, tail.bytes...)
	merged = append(merged, raw...)
	tail.bytes = partialResultTail(merged, tail.limit)
}

func partialResultTail(raw []byte, limit int) []byte {
	if len(raw) <= limit {
		return append([]byte(nil), raw...)
	}
	start := len(raw) - limit
	for start < len(raw) && raw[start]&0xc0 == 0x80 {
		start++
	}
	return append([]byte(nil), raw[start:]...)
}
