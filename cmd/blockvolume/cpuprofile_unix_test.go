//go:build unix

package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"syscall"
	"testing"
	"time"
)

func profiles(t *testing.T, dir string) []string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, "blockvolume-*.pprof"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(m)
	return m
}

func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	end := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(end) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func burn(d time.Duration) {
	x := 0
	for end := time.Now().Add(d); time.Now().Before(end); {
		x++
	}
	_ = x
}

// With a profile directory, SIGUSR1 starts a CPU profile and the next
// SIGUSR1 writes it out (a gzip pprof); a second pair writes a second file.
// Stopping the handler ends a running profile and keeps what it recorded.
func TestCPUProfileSIGUSR1TogglesAProfile(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := startCPUProfileSignal(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	usr1 := func() {
		if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
			t.Fatal(err)
		}
	}
	complete := func(path string) bool {
		b, err := os.ReadFile(path)
		return err == nil && len(b) > 2 && bytes.HasPrefix(b, []byte{0x1f, 0x8b})
	}
	for cycle := 1; cycle <= 2; cycle++ {
		usr1()
		waitUntil(t, "profile file created", func() bool { return len(profiles(t, dir)) == cycle })
		burn(200 * time.Millisecond)
		usr1()
		path := profiles(t, dir)[cycle-1]
		waitUntil(t, "profile "+path+" written", func() bool { return complete(path) })
	}
	// A third profile left running is written out by stop.
	usr1()
	waitUntil(t, "third profile file created", func() bool { return len(profiles(t, dir)) == 3 })
	burn(100 * time.Millisecond)
	stop()
	stopped = true
	if p := profiles(t, dir)[2]; !complete(p) {
		t.Fatalf("stop did not write out the running profile %s", p)
	}
}

// Off (no directory): nothing is installed and nothing is written.
func TestCPUProfileOffInstallsNothing(t *testing.T) {
	stop, err := startCPUProfileSignal(context.Background(), "")
	if err != nil || stop == nil {
		t.Fatalf("off: %v", err)
	}
	stop()
}
