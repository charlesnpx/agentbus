// Package cursorcli adapts Cursor's ACP stdio endpoint to the agentbus engine.
//
// Steering uses ACP interrupt-and-continue on the same session: the adapter
// sends session/cancel, drains the active session/prompt response, then writes
// another session/prompt containing the follow-up text. It answers permission
// requests received after cancellation with the cancelled outcome. ACP has no
// separate acceptance acknowledgement before a prompt's terminal response, so
// Confirmed means the continuation prompt was written after the cancelled
// prompt drained; it does not mean the model followed the instruction.
package cursorcli
