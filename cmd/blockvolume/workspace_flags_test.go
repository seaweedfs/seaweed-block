package main

import "testing"

func TestWorkspaceLeaseFlagIsExplicitAndChunkOnly(t *testing.T) {
	base := []string{"--master", "127.0.0.1:1", "--server-id", "node-a", "--volume-id", "v1", "--replica-id", "r1", "--data-addr", "127.0.0.1:9100", "--ctrl-addr", "127.0.0.1:9101"}
	legacy, err := parseFlags(base)
	if err != nil || legacy.workspaceLease {
		t.Fatal("legacy mode changed", legacy, err)
	}
	if _, err := parseFlags(append(append([]string{}, base...), "--workspace-lease")); err == nil {
		t.Fatal("workspace lease accepted without walstore-chunk")
	}
	chunk := append(append([]string{}, base...), "--durable-root", t.TempDir(), "--durable-impl", "walstore-chunk", "--chunk-filer", "127.0.0.1:18888")
	disabled, err := parseFlags(chunk)
	if err != nil || disabled.workspaceLease {
		t.Fatal("ordinary chunk volume opted in", err)
	}
	enabled, err := parseFlags(append(chunk, "--workspace-lease"))
	if err != nil || !enabled.workspaceLease {
		t.Fatal("explicit workspace flag not installed", err)
	}
}
