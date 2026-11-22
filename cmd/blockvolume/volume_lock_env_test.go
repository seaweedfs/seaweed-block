package main

import (
	"os"
	"os/exec"
	"strings"
)

// stripSystemdForTestLockDir gives a blockvolume child started with
// --volume-lock-dir-test-only an environment without INVOCATION_ID.
// blockvolume refuses the test override under systemd; a test runner that is
// itself a systemd service (a CI runner unit) would otherwise pass its
// INVOCATION_ID to every child. Other children are left as they are.
func stripSystemdForTestLockDir(cmd *exec.Cmd) *exec.Cmd {
	hasFlag := false
	for _, a := range cmd.Args[1:] {
		if a == "--"+volumeLockDirTestOnlyFlag || strings.HasPrefix(a, "--"+volumeLockDirTestOnlyFlag+"=") {
			hasFlag = true
			break
		}
	}
	if !hasFlag {
		return cmd
	}
	env := cmd.Env
	if env == nil {
		env = os.Environ()
	}
	kept := make([]string, 0, len(env))
	for _, kv := range env {
		if !strings.HasPrefix(kv, "INVOCATION_ID=") {
			kept = append(kept, kv)
		}
	}
	cmd.Env = kept
	return cmd
}
