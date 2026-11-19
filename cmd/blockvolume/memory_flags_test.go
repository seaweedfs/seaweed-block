package main

import "testing"

func TestMemoryLimitFlagAndEnvironment(t *testing.T) {
	base := []string{"--master", "127.0.0.1:1", "--server-id", "h1", "--volume-id", "v1", "--replica-id", "r1", "--data-addr", "127.0.0.1:2", "--ctrl-addr", "127.0.0.1:3"}
	t.Setenv("GOMEMLIMIT", "")
	f, err := parseFlags(base)
	if err != nil || f.memoryLimitMiB != 256 {
		t.Fatalf("default memory limit: %d %v", f.memoryLimitMiB, err)
	}
	t.Setenv("GOMEMLIMIT", "128MiB")
	f, err = parseFlags(base)
	if err != nil || f.memoryLimitMiB != 0 {
		t.Fatalf("explicit runtime limit overridden: %d %v", f.memoryLimitMiB, err)
	}
	f, err = parseFlags(append(append([]string{}, base...), "--memory-limit-mib", "192"))
	if err != nil || f.memoryLimitMiB != 192 {
		t.Fatalf("explicit flag: %d %v", f.memoryLimitMiB, err)
	}
	for _, value := range []string{"-1", "9223372036854775807"} {
		if _, err = parseFlags(append(append([]string{}, base...), "--memory-limit-mib", value)); err == nil {
			t.Fatalf("invalid limit %s accepted", value)
		}
	}
}
