package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/charlesnpx/agentbus/internal/cgroup"
)

func main() {
	if runtime.GOOS != "linux" {
		fatalf("strict cgroup preflight requires linux, got %s", runtime.GOOS)
	}

	controllersRaw, err := os.ReadFile("/sys/fs/cgroup/cgroup.controllers")
	if err != nil {
		fatalf("read /sys/fs/cgroup/cgroup.controllers: %v", err)
	}
	controllers := controllerSet(string(controllersRaw))
	for _, required := range []string{"cpu", "pids"} {
		if !controllers[required] {
			fatalf("/sys/fs/cgroup/cgroup.controllers missing %q controller; controllers=%q", required, strings.TrimSpace(string(controllersRaw)))
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	manager, err := cgroup.New("")
	if err != nil {
		fatalf("cgroup.New(\"\") unavailable: %v", err)
	}
	defer func() {
		if err := manager.Close(); err != nil {
			fatalf("close cgroup manager: %v", err)
		}
	}()

	support := manager.Probe(ctx)
	if !support.Strict() {
		fatalf("strict cgroup support unavailable: supported=%t runtimeProbePassed=%t degraded=%t platform=%s reason=%v", support.Supported, support.RuntimeProbePassed, support.Degraded, support.Platform, support.Reason)
	}

	fmt.Printf("strict-cgroup-preflight: ok controllers=%s platform=%s\n", sortedControllerList(controllers), support.Platform)
}

func controllerSet(raw string) map[string]bool {
	controllers := make(map[string]bool)
	for _, field := range strings.Fields(raw) {
		controllers[field] = true
	}
	return controllers
}

func sortedControllerList(controllers map[string]bool) string {
	list := make([]string, 0, len(controllers))
	for controller := range controllers {
		list = append(list, controller)
	}
	sort.Strings(list)
	return strings.Join(list, ",")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "strict-cgroup-preflight: "+format+"\n", args...)
	os.Exit(1)
}
