//go:build linux

package storage

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPrezeroCreatesInitializedVolume(t *testing.T) {
	t.Setenv("SWBLOCK_SYNC", "fdatasync")
	t.Setenv("SWBLOCK_PREZERO", "1")
	path := filepath.Join(t.TempDir(), "volume")
	s, e := CreateWALStore(path, 32, 4096)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	f, e := os.Open(path)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if e = verifyInitializedFile(f); e != nil {
		t.Fatalf("prezero must eliminate holes and unwritten extents: %v", e)
	}
	if _, e = s.Recover(); e != nil {
		t.Fatal(e)
	}
	b, e := s.Read(31)
	if e != nil || !bytes.Equal(b, make([]byte, 4096)) {
		t.Fatalf("new extent bytes: %v", e)
	}
}

func TestInitializedFileRejectsSparseAndUnwritten(t *testing.T) {
	f, e := os.CreateTemp(t.TempDir(), "volume")
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if e = f.Truncate(1 << 20); e != nil {
		t.Fatal(e)
	}
	if e = verifyInitializedFile(f); e == nil {
		t.Fatal("sparse file accepted")
	}
	if e = unix.Fallocate(int(f.Fd()), 0, 0, 1<<20); e != nil {
		t.Fatal(e)
	}
	if e = verifyInitializedFile(f); e == nil {
		t.Fatal("unwritten extents accepted")
	}
}

func TestFdatasyncAcknowledgedWritesSurviveSIGKILL(t *testing.T) {
	if path := os.Getenv("SWBLOCK_TEST_SYNC_CRASH"); path != "" {
		if os.Getenv("SWBLOCK_TEST_SYNC_CHILD_FAILURE") == "1" {
			_ = os.Stdout.Close()
			time.Sleep(30 * time.Millisecond)
			fmt.Fprintln(os.Stderr, "SYNC_CHILD_FAILURE_SETTLED")
			os.Exit(7)
		}
		opts := WALStoreOptions{}
		if os.Getenv("SWBLOCK_TEST_STRICT") == "1" {
			opts.WMode = WModeStrict
		}
		s, e := CreateWALStoreWithOptions(path, 32, 4096, opts)
		if e != nil {
			panic(e)
		}
		if os.Getenv("SWBLOCK_PREZERO") == "0" && verifyInitializedFile(s.fd) == nil {
			panic("sparse crash arm was silently prezeroed")
		}
		s.DisableAutoFlushForRecoveryTest()
		if _, e = s.Recover(); e != nil {
			panic(e)
		}
		for _, lba := range []uint32{0, 13, 31} {
			if _, e = s.Write(lba, bytes.Repeat([]byte{byte(lba + 17)}, 4096)); e != nil {
				panic(e)
			}
		}
		if n, e := s.Sync(); e != nil || n < 3 {
			panic(fmt.Sprintf("Sync=%d %v", n, e))
		}
		if os.Getenv("SWBLOCK_TEST_EXTENT") == "1" {
			if e = s.flusher.flushOnce(); e != nil {
				panic(e)
			}
		}
		fmt.Println("SYNC_ACKNOWLEDGED")
		for {
			time.Sleep(time.Hour)
		}
	}
	for _, strict := range []string{"0", "1"} {
		for _, prezero := range []string{"0", "1"} {
			for _, extent := range []string{"0", "1"} {
				t.Run("strict"+strict+"/prezero"+prezero+"/extent"+extent, func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "volume")
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()
					cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFdatasyncAcknowledgedWritesSurviveSIGKILL$")
					cmd.Env = append(os.Environ(), "SWBLOCK_SYNC=fdatasync", "SWBLOCK_PREZERO="+prezero, "SWBLOCK_TEST_SYNC_CRASH="+path, "SWBLOCK_TEST_EXTENT="+extent, "SWBLOCK_TEST_STRICT="+strict)
					out, e := cmd.StdoutPipe()
					if e != nil {
						t.Fatal(e)
					}
					var stderr bytes.Buffer
					cmd.Stderr = &stderr
					if e = cmd.Start(); e != nil {
						t.Fatal(e)
					}
					defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
					scan := bufio.NewScanner(out)
					ready := false
					for scan.Scan() {
						if scan.Text() == "SYNC_ACKNOWLEDGED" {
							ready = true
							break
						}
					}
					if !ready {
						t.Fatalf("no sync ACK: %v %s", scan.Err(), stderr.String())
					}
					if e = cmd.Process.Kill(); e != nil {
						t.Fatal(e)
					}
					e = cmd.Wait()
					exit, ok := e.(*exec.ExitError)
					if !ok {
						t.Fatalf("not killed: %v", e)
					}
					status, ok := exit.Sys().(syscall.WaitStatus)
					if !ok || status.Signal() != syscall.SIGKILL {
						t.Fatalf("wrong exit: %v", e)
					}
					t.Setenv("SWBLOCK_SYNC", "fdatasync")
					t.Setenv("SWBLOCK_PREZERO", prezero)
					s, e := OpenWALStore(path)
					if e != nil {
						t.Fatal(e)
					}
					defer s.Close()
					if _, e = s.Recover(); e != nil {
						t.Fatal(e)
					}
					for _, lba := range []uint32{0, 13, 31} {
						b, e := s.Read(lba)
						if e != nil || !bytes.Equal(b, bytes.Repeat([]byte{byte(lba + 17)}, 4096)) {
							t.Fatalf("acknowledged block%d differs: %v", lba, e)
						}
					}
				})
			}
		}

	}
}

// Run under strace with the sync environment unset/set to verify the
// syscall boundary independently of the selected function's Go identity.
func TestDataSyncSyscallProbe(t *testing.T) {
	opts := WALStoreOptions{}
	if os.Getenv("SWBLOCK_TEST_STRICT") == "1" {
		opts.WMode = WModeStrict
	}
	s, e := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), 32, 4096, opts)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	s.DisableAutoFlushForRecoveryTest()
	if _, e = s.Recover(); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Write(7, bytes.Repeat([]byte{0x6d}, 4096)); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Sync(); e != nil {
		t.Fatal(e)
	}
	if e = s.flusher.flushOnce(); e != nil {
		t.Fatal(e)
	}
	b, e := s.Read(7)
	if e != nil || !bytes.Equal(b, bytes.Repeat([]byte{0x6d}, 4096)) {
		t.Fatalf("readback: %v", e)
	}
}

func TestFallocateAllocationIsRefused(t *testing.T) {
	t.Setenv("SWBLOCK_PREZERO", "0")
	t.Setenv("SWBLOCK_ALLOCATION", "fallocate")
	s, err := CreateWALStore(filepath.Join(t.TempDir(), "volume"), 32, 4096)
	if err == nil {
		s.Close()
		t.Fatal("fallocate allocation must be refused")
	}
}

func TestDefaultCreatedVolumeStaysSparse(t *testing.T) {
	t.Setenv("SWBLOCK_SYNC", "")
	t.Setenv("SWBLOCK_PREZERO", "")
	t.Setenv("SWBLOCK_ALLOCATION", "")
	path := filepath.Join(t.TempDir(), "volume")
	s, err := CreateWALStore(path, 262144, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat := info.Sys().(*syscall.Stat_t)
	if allocated := stat.Blocks * 512; allocated >= info.Size()/4 {
		t.Fatalf("default volume lost sparse allocation: allocated=%d size=%d", allocated, info.Size())
	}
}

func TestInvalidCreatePolicyLeavesNoFileAndAllowsRetry(t *testing.T) {
	for _, tc := range []struct{ name, sync, allocation, prezero string }{
		{"sync-before-prezero", "bad", "prezero", "1"},
		{"allocation", "fdatasync", "bad", "0"},
		{"fallocate", "fdatasync", "fallocate", "0"},
		{"prezero", "fdatasync", "", "bad"},
		{"conflict", "fdatasync", "sparse", "1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SWBLOCK_SYNC", tc.sync)
			t.Setenv("SWBLOCK_ALLOCATION", tc.allocation)
			t.Setenv("SWBLOCK_PREZERO", tc.prezero)
			path := filepath.Join(t.TempDir(), "volume")
			s, err := CreateWALStore(path, 32, 4096)
			if err == nil {
				s.Close()
				t.Fatal("invalid policy accepted")
			}
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("invalid policy touched volume before refusal: %v", err)
			}
			t.Setenv("SWBLOCK_SYNC", "fdatasync")
			t.Setenv("SWBLOCK_ALLOCATION", "sparse")
			t.Setenv("SWBLOCK_PREZERO", "0")
			s, err = CreateWALStore(path, 32, 4096)
			if err != nil {
				t.Fatalf("corrected retry failed: %v", err)
			}
			s.Close()
		})
	}
}

func TestSyncCrashFailureDiagnosticsWaitForStderr(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFdatasyncAcknowledgedWritesSurviveSIGKILL$")
	cmd.Env = append(os.Environ(), "SWBLOCK_TEST_SYNC_CRASH=", "SWBLOCK_TEST_SYNC_CHILD_FAILURE=1", "SWBLOCK_ALLOCATION=", "SWBLOCK_PREZERO=0")
	out, err := cmd.CombinedOutput()
	if err == nil || ctx.Err() != nil {
		t.Fatalf("wrong failed-child outcome: %v %v", err, ctx.Err())
	}
	if !strings.Contains(string(out), "SYNC_CHILD_FAILURE_SETTLED") || strings.Contains(string(out), "WARNING: DATA RACE") {
		t.Fatalf("failure diagnostics read before child settlement: %s", out)
	}
}

func TestSyncControlsIgnoreAmbientAllocation(t *testing.T) {
	for _, tc := range []struct{ name, test, allocation string }{
		{"prezero", "TestPrezeroCreatesInitializedVolume", "sparse"},
		{"crash-matrix", "TestFdatasyncAcknowledgedWritesSurviveSIGKILL", "prezero"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+tc.test+"$")
			cmd.Env = append(os.Environ(), "SWBLOCK_TEST_SYNC_CRASH=", "SWBLOCK_TEST_SYNC_CHILD_FAILURE=", "SWBLOCK_ALLOCATION="+tc.allocation, "SWBLOCK_PREZERO=0")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("ambient allocation contaminated control: %v: %s", err, out)
			}
		})
	}
}
