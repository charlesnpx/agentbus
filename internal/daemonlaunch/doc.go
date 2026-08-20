// Package daemonlaunch starts foreground agentbus daemon children with a
// private readiness handshake.
//
// The parent passes a pipe write end to the child as fd 3 and sets
// AGENTBUS_READY_FD=3. The child writes exactly one newline-framed JSON object
// to that fd and closes it:
//
//	{"ready":{"protocolVersion":1,"pid":123,"canonicalStateRoot":"/...","socketPath":"/.../agentbus.sock"}}
//	{"failed":{"code":"error","message":"..."}}
//
// The readiness protocol version is private to this launcher. It is not the
// JSON-RPC protocol version spoken on the daemon socket.
package daemonlaunch
