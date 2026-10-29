// Package codexcli adapts the Codex CLI to the agentbus engine backend interface.
// Active app-server turns support native turn/steer requests. A steer is
// confirmed only after RunTurn reads its successful JSON-RPC response; if the
// caller context ends first, delivery remains unconfirmed.
package codexcli
