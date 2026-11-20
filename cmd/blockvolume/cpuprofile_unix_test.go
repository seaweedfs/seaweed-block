//go:build unix

package main

import (
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"runtime/pprof"
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

// complete reports whether path holds a whole gzip stream (a written-out
// pprof profile): a truncated or still-open profile fails to decompress.
func complete(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		return false
	}
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return false
	}
	out, err := io.ReadAll(zr)
	return err == nil && len(out) > 0
}

// noProfileRunning reports whether no CPU profile is active in this process.
func noProfileRunning(t *testing.T) bool {
	t.Helper()
	if err := pprof.StartCPUProfile(io.Discard); err != nil {
		return false
	}
	pprof.StopCPUProfile()
	return true
}

func usr1(t *testing.T) {
	t.Helper()
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
}

// With a profile directory, SIGUSR1 starts a CPU profile and the next
// SIGUSR1 writes it out as a complete pprof; a second pair writes a second
// file. stop writes out a profile still running.
func TestCPUProfileSIGUSR1TogglesAProfile(t *testing.T) {
	dir := t.TempDir()
	stop, err := startCPUProfileSignal(dir)
	if err != nil {
		t.Fatal(err)
	}
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	for cycle := 1; cycle <= 2; cycle++ {
		usr1(t)
		waitUntil(t, "profile file created", func() bool { return len(profiles(t, dir)) == cycle })
		burn(200 * time.Millisecond)
		usr1(t)
		path := profiles(t, dir)[cycle-1]
		waitUntil(t, "profile "+path+" written", func() bool { return complete(path) })
	}
	usr1(t)
	waitUntil(t, "third profile file created", func() bool { return len(profiles(t, dir)) == 3 })
	burn(100 * time.Millisecond)
	stop()
	stopped = true
	if p := profiles(t, dir)[2]; !complete(p) {
		t.Fatalf("stop did not write out the running profile %s", p)
	}
	if !noProfileRunning(t) {
		t.Fatal("a CPU profile is still running after stop")
	}
}

// A SIGUSR1 delivered just before stop cannot leave a profile running after
// it: stop joins the worker before the final write-out.
func TestCPUProfileStopJoinsTheWorker(t *testing.T) {
	for i := 0; i < 20; i++ {
		dir := t.TempDir()
		stop, err := startCPUProfileSignal(dir)
		if err != nil {
			t.Fatal(err)
		}
		usr1(t)
		stop()
		if !noProfileRunning(t) {
			t.Fatalf("round %d: a profile was running after stop returned", i)
		}
		for _, p := range profiles(t, dir) {
			if !complete(p) {
				t.Fatalf("round %d: %s left incomplete after stop", i, p)
			}
		}
	}
}

// Off (no directory): nothing is installed and nothing is written.
func TestCPUProfileOffInstallsNothing(t *testing.T) {
	stop, err := startCPUProfileSignal("")
	if err != nil || stop == nil {
		t.Fatalf("off: %v", err)
	}
	stop()
}
