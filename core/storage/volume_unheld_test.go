package storage

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func createClosedV3Volume(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v.bin")
	s, err := CreateWALStore(path, 64, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// A closed v3 volume is unheld, and the probe leaves it openable (it
// released its own lock).
func TestProbeVolumeUnheldClosedVolume(t *testing.T) {
	path := createClosedV3Volume(t)
	if err := ProbeVolumeUnheld(path); err != nil {
		t.Fatalf("closed volume: %v; want unheld", err)
	}
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatalf("open after the probe: %v", err)
	}
	_ = s.Close()
}

// An open store holds the volume: the probe says held (also from another
// descriptor in the same process), and unheld once the store is closed.
func TestProbeVolumeUnheldOpenStoreIsHeld(t *testing.T) {
	path := createClosedV3Volume(t)
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ProbeVolumeUnheld(path); !errors.Is(err, ErrVolumeHeld) {
		_ = s.Close()
		t.Fatalf("open store: %v; want ErrVolumeHeld", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ProbeVolumeUnheld(path); err != nil {
		t.Fatalf("after close: %v; want unheld", err)
	}
}

// Unknown is neither unheld nor held: a missing file and a file that is not
// a v3 volume (its owner would not take the lock).
func TestProbeVolumeUnheldUnknown(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.bin")
	junk := filepath.Join(t.TempDir(), "junk.bin")
	if err := os.WriteFile(junk, make([]byte, 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]string{"missing": missing, "not v3": junk} {
		if err := ProbeVolumeUnheld(path); err == nil || errors.Is(err, ErrVolumeHeld) {
			t.Fatalf("%s: %v; want an unknown (non-nil, not held) result", name, err)
		}
	}
}

const unheldHelperEnv = "SWBLOCK_UNHELD_HELPER_PATH"

// TestProbeVolumeUnheldHelper is the holder process of the test below: it
// opens the store, says so, and waits to be killed.
func TestProbeVolumeUnheldHelper(t *testing.T) {
	path := os.Getenv(unheldHelperEnv)
	if path == "" {
		t.Skip("helper process only")
	}
	s, err := OpenWALStore(path)
	if err != nil {
		os.Stdout.WriteString("OPEN-FAILED " + err.Error() + "\n")
		os.Exit(3)
	}
	defer s.Close()
	os.Stdout.WriteString("HOLDING\n")
	time.Sleep(time.Minute)
	os.Exit(4)
}

// The property cleanup relies on: a volume held by another process is held,
// and becomes unheld when that process dies without closing it (the kernel
// releases the lock).
func TestProbeVolumeUnheldHolderProcessDies(t *testing.T) {
	path := createClosedV3Volume(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestProbeVolumeUnheldHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), unheldHelperEnv+"="+path)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	// Reaped on every path; the cleanup kills the holder and joins its reaper.
	go func() { _ = cmd.Wait(); close(exited) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-exited:
		case <-time.After(10 * time.Second):
			t.Error("holder process not reaped")
		}
	})
	ready := make(chan string, 1)
	go func() {
		buf := make([]byte, 256)
		n, _ := out.Read(buf)
		ready <- string(buf[:n])
	}()
	select {
	case line := <-ready:
		if !strings.Contains(line, "HOLDING") {
			t.Fatalf("holder: %q", line)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("holder never opened the volume")
	}
	if err := ProbeVolumeUnheld(path); !errors.Is(err, ErrVolumeHeld) {
		t.Fatalf("held by a live process: %v; want ErrVolumeHeld", err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-exited:
	case <-time.After(10 * time.Second):
		t.Fatal("holder did not exit after kill")
	}
	if err := ProbeVolumeUnheld(path); err != nil {
		t.Fatalf("after the holder died: %v; want unheld", err)
	}
}

// The answer is about the file the probe opened: a held volume whose path is
// replaced (by an unlocked non-v3 file) between the probe's open and its lock
// is still reported held, never unheld.
func TestProbeVolumeUnheldDecidesOnTheOpenedFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("an open file cannot be renamed over on Windows")
	}
	path := createClosedV3Volume(t)
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	junk := filepath.Join(filepath.Dir(path), "junk.bin")
	if err := os.WriteFile(junk, make([]byte, 64<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	replaced := false
	testProbeUnheldAfterOpen = func() {
		replaced = os.Rename(junk, path) == nil
	}
	t.Cleanup(func() { testProbeUnheldAfterOpen = nil })
	err = ProbeVolumeUnheld(path)
	if !replaced {
		t.Fatal("fixture: the path was not replaced after the open")
	}
	if !errors.Is(err, ErrVolumeHeld) {
		t.Fatalf("held volume replaced at its path mid-probe: %v; want ErrVolumeHeld", err)
	}
}
