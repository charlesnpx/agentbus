// Package claudecli adapts Claude Code to the agentbus engine backend interface.
// Steering uses interrupt_continue on the existing stream-json connection:
// it waits for the interrupt control_response success, drains the interrupted
// result, then writes the follow-up user message. Confirmed means both the
// interrupt acknowledgement and follow-up write completed; it does not mean
// same-turn injection or that Claude followed the instruction.
package claudecli
