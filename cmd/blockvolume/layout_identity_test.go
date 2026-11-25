package main

import (
	"path/filepath"
	"testing"
)

func TestBlockvolumeLayoutIdentitySelectsTheLocalRoot(t *testing.T) {
	base := []string{"--master=127.0.0.1:9333", "--server-id=s1", "--volume-id=v", "--replica-id=r1", "--data-addr=127.0.0.1:19101", "--ctrl-addr=127.0.0.1:19102", "--durable-root=" + t.TempDir()}
	first, err := parseFlags(append(append([]string{}, base...), "--layout-id=1234567890abcdef1234567890abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(first.durableRoot) != "v-12345678" {
		t.Fatal(first.durableRoot)
	}
	second, err := parseFlags(append(append([]string{}, base...), "--layout-id=abcdef1290abcdef1234567890abcdef"))
	if err != nil || second.durableRoot == first.durableRoot {
		t.Fatalf("different incarnation reused local root: %v", err)
	}
	if _, err := parseFlags(append(append([]string{}, base...), "--layout-id=bad")); err == nil {
		t.Fatal("malformed identity accepted")
	}
}
