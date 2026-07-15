// Package cliadapter adapts backend CLI streams to engine sessions.
//
// P0A moves the turn command runner seam into engine/command, but it does not
// close every cliadapter-reachable subprocess path. The direct os/exec sites
// intentionally left for P0C/S4 post-accept-probe removal are:
//   - adapter.go Preflight, DiscoverModels, SetupProbe, and validationSets call
//     exec.LookPath.
//   - adapter.go commandOutput calls exec.CommandContext and is reached by
//     Preflight, SetupProbe, and validateOptions -> validationSets for
//     --version checks.
//   - direct_runner.go DirectCommandRunner.Start calls exec.CommandContext for
//     the legacy direct turn runner.
//   - backend discovery callbacks invoked from DiscoverModels may run their own
//     direct probes, such as claudecli's --help probe.
package cliadapter
