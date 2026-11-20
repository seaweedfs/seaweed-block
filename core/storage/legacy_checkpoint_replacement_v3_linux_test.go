//go:build linux

package storage

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestNativeReplacementCheckpointSIGKILL(t *testing.T) {
	const phaseEnv, pathEnv = "SW_BLOCK_NATIVE_CHECKPOINT_KILL", "SW_BLOCK_NATIVE_CHECKPOINT_PATH"
	if phase := os.Getenv(phaseEnv); phase != "" {
		s, err := createMappedWALStoreV3ForTest(os.Getenv(pathEnv), 16, 4096, 64*1024, WModeStrict, 4)
		if err != nil {
			t.Fatal(err)
		}
		s.DisableAutoFlushForRecoveryTest()
		if _, err := s.Write(3, blk(0x7C)); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Sync(); err != nil {
			t.Fatal(err)
		}
		kill := func() error { return syscall.Kill(os.Getpid(), syscall.SIGKILL) }
		switch phase {
		case "extent_sync":
			s.flusher.afterExtentSyncForTest = func() { _ = kill() }
		case "slot_pwrite":
			s.v3.pub.io.fsync = kill
		case "slot_sync":
			real := s.v3.pub.io.fsync
			s.v3.pub.io.fsync = func() error {
				if err := real(); err != nil {
					return err
				}
				return kill()
			}
		case "tail_publication":
		default:
			t.Fatal("unknown native checkpoint crash phase", phase)
		}
		if err := s.flusher.flushOnce(); err != nil {
			t.Fatal(err)
		}
		_ = kill()
		t.Fatal("SIGKILL returned")
	}
	for _, phase := range []string{"extent_sync", "slot_pwrite", "slot_sync", "tail_publication"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "volume")
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestNativeReplacementCheckpointSIGKILL$")
			cmd.Env = append(os.Environ(), phaseEnv+"="+phase, pathEnv+"="+path)
			out, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if ctx.Err() != nil || !errors.As(err, &exited) || exited.ProcessState.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
				t.Fatalf("helper did not reach its own crash hook: %v ctx=%v output=%s", err, ctx.Err(), out)
			}
			r, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			r.DisableAutoFlushForRecoveryTest()
			t.Cleanup(func() { _ = r.Close() })
			cp := r.CheckpointLSN()
			if cp > 1 || (phase == "extent_sync" && cp != 0) || ((phase == "slot_sync" || phase == "tail_publication") && cp != 1) {
				t.Fatalf("%s checkpoint=%d", phase, cp)
			}
			if _, err := r.Recover(); err != nil {
				t.Fatal(err)
			}
			if got, err := r.Read(3); err != nil || !bytes.Equal(got, blk(0x7C)) {
				t.Fatalf("acknowledged bytes lost at %s: %v", phase, err)
			}
			if r.NextLSN() != 2 {
				t.Fatalf("recovered allocator=%d", r.NextLSN())
			}
		})
	}
}
