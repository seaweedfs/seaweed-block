package main_test

import (
	"os"
	"os/exec"
	"strings"
)

// stripSystemdForTestLockDir is the external test package's copy of the
// helper in volume_lock_env_test.go (package main_test cannot use it): a
// child started with --volume-lock-dir-test-only gets an environment without
// INVOCATION_ID, which a systemd-run test runner would otherwise pass on.
func stripSystemdForTestLockDir(cmd *exec.Cmd) *exec.Cmd {
	const flag = "--volume-lock-dir-test-only"
	hasFlag := false
	for _, a := range cmd.Args[1:] {
		if a == flag || strings.HasPrefix(a, flag+"=") {
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
