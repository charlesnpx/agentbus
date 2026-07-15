//go:build linux

package custodian

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/charlesnpx/agentbus/engine/execution/model"
)

func groupHasNoMembersExceptLeader(group model.GroupRef) (bool, error) {
	if err := group.Validate(); err != nil {
		return false, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() && !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		state, pgid, err := linuxProcessStateGroup(pid)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		if pgid == group.PGID && pid != group.Leader.PID && state != "Z" {
			return false, nil
		}
	}
	return true, nil
}

func groupHasNoLiveMembers(group model.GroupRef) (bool, error) {
	if err := group.Validate(); err != nil {
		return false, err
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false, err
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() && !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		state, pgid, err := linuxProcessStateGroup(pid)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		if pgid == group.PGID && state != "Z" {
			return false, nil
		}
	}
	return true, nil
}

func linuxProcessStateGroup(pid int) (string, int, error) {
	raw, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return "", 0, err
	}
	text := string(raw)
	end := strings.LastIndex(text, ")")
	if end < 0 || end+2 >= len(text) {
		return "", 0, fmt.Errorf("malformed proc stat for pid %d", pid)
	}
	fields := strings.Fields(text[end+2:])
	if len(fields) < 3 {
		return "", 0, fmt.Errorf("short proc stat for pid %d", pid)
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return "", 0, fmt.Errorf("parse proc stat pgrp for pid %d: %w", pid, err)
	}
	return fields[0], pgid, nil
}
