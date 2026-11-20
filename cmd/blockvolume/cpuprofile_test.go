package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The profile directory comes from --cpu-profile-dir, defaulting to
// $SWBLOCK_CPU_PROFILE_DIR; unset means off.
func TestCPUProfileDirFlagDefaultsFromEnv(t *testing.T) {
	base := []string{"--master", "127.0.0.1:1", "--server-id", "h1", "--volume-id", "v1", "--replica-id", "r1", "--data-addr", "127.0.0.1:2", "--ctrl-addr", "127.0.0.1:3"}
	t.Setenv(cpuProfileEnv, "")
	if f, err := parseFlags(base); err != nil || f.cpuProfileDir != "" {
		t.Fatalf("default: %q %v; want off", f.cpuProfileDir, err)
	}
	dir := t.TempDir()
	t.Setenv(cpuProfileEnv, dir)
	f, err := parseFlags(base)
	other := t.TempDir()
	fx, errx := parseFlags(append(append([]string{}, base...), "--cpu-profile-dir", other))
	if !cpuProfileSupported {
		// No SIGUSR1 here: a set directory is refused in flag parsing.
		if err == nil || errx == nil {
			t.Fatalf("unsupported platform accepted a profile dir: env %v, flag %v", err, errx)
		}
		return
	}
	if err != nil || f.cpuProfileDir != dir {
		t.Fatalf("from env: %q %v; want %q", f.cpuProfileDir, err, dir)
	}
	if errx != nil || fx.cpuProfileDir != other {
		t.Fatalf("explicit flag: %q %v; want %q", fx.cpuProfileDir, errx, other)
	}
}

// A profile directory that is not an existing directory is refused before
// any signal arrives.
func TestCPUProfileDirMustBeAnExistingDirectory(t *testing.T) {
	if err := checkCPUProfileDir(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("a missing directory was accepted")
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := checkCPUProfileDir(file); err == nil {
		t.Fatal("a file was accepted as the profile directory")
	}
	if err := checkCPUProfileDir(t.TempDir()); err != nil {
		t.Fatalf("an existing directory was refused: %v", err)
	}
	// The refusal happens in parseFlags, before run() activates anything.
	base := []string{"--master", "127.0.0.1:1", "--server-id", "h1", "--volume-id", "v1", "--replica-id", "r1", "--data-addr", "127.0.0.1:2", "--ctrl-addr", "127.0.0.1:3"}
	if _, err := parseFlags(append(append([]string{}, base...), "--cpu-profile-dir", file)); err == nil {
		t.Fatal("parseFlags accepted a file as the profile directory")
	}
}
