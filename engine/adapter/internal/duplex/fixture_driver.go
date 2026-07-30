package duplex

import (
	"context"
	"errors"

	"github.com/charlesnpx/agentbus/engine"
	"github.com/charlesnpx/agentbus/engine/command"
)

// FixtureDriver is a tiny JSONL echo protocol used by duplex runtime tests.
type FixtureDriver struct {
	Spec command.ExecSpec
}

func (d FixtureDriver) ExecSpec(string, engine.SessionOpts, engine.TurnInput) (command.ExecSpec, error) {
	if len(d.Spec.Argv) == 0 {
		return command.ExecSpec{}, errors.New("fixture exec spec argv is required")
	}
	return d.Spec, nil
}

func (d FixtureDriver) RunTurn(ctx context.Context, conn *Conn, resumeID string, _ engine.SessionOpts, input engine.TurnInput, emit EmitFunc) (string, error) {
	prompt := map[string]any{
		"type":   "prompt",
		"prompt": input.Prompt,
		"write":  input.Write,
	}
	if resumeID != "" {
		prompt["resumeId"] = resumeID
	}
	if err := conn.WriteJSON(prompt); err != nil {
		return "", err
	}

	for {
		select {
		case frame, ok := <-conn.Frames():
			if !ok {
				if err := pendingDecodeError(conn); err != nil {
					return "", err
				}
				return "", ErrBackendExitedBeforeTerminal
			}
			kind, _ := frame.Object["type"].(string)
			switch kind {
			case "message":
				text, _ := frame.Object["text"].(string)
				emit(engine.Event{Type: engine.EventAgentText, Text: text, Metadata: frame.Object})
			case "complete":
				text, _ := frame.Object["text"].(string)
				nextID, _ := frame.Object["resumeId"].(string)
				if nextID == "" {
					nextID = resumeID
				}
				emit(engine.Event{Type: engine.EventResultMessage, Text: text, Metadata: frame.Object})
				return nextID, nil
			case "warning":
				text, _ := frame.Object["text"].(string)
				emit(engine.Event{Type: engine.EventWarning, Text: text, Metadata: frame.Object})
			}
		case err, ok := <-conn.DecodeErrors():
			if ok && err != nil {
				return "", err
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func (d FixtureDriver) Interrupt(ctx context.Context, conn *Conn) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return conn.WriteJSON(map[string]any{"type": "interrupt"})
}

func pendingDecodeError(conn *Conn) error {
	select {
	case err, ok := <-conn.DecodeErrors():
		if ok {
			return err
		}
	default:
	}
	return nil
}
