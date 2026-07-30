//go:build linux

package storage

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

const (
	checkpointCrashHelperEnv = "SW_BLOCK_CHECKPOINT_CRASH_HELPER"
	checkpointCrashPathEnv   = "SW_BLOCK_CHECKPOINT_CRASH_PATH"
)

func TestWALStoreCheckpointSIGKILLCrashWindows(t *testing.T) {
	if crashWindow := os.Getenv(checkpointCrashHelperEnv); crashWindow != "" {
		runCheckpointCrashHelper(crashWindow, os.Getenv(checkpointCrashPathEnv))
		return
	}

	tests := []struct {
		name          string
		checkpointMin uint64
		checkpointMax uint64
	}{
		{name: "after_extent_sync", checkpointMin: 0, checkpointMax: 0},
		{name: "after_checkpoint_pwrite", checkpointMin: 0, checkpointMax: 1},
		{name: "after_checkpoint_sync", checkpointMin: 1, checkpointMax: 1},
		{name: "after_tail_publish", checkpointMin: 1, checkpointMax: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "store.bin")
			cmd := exec.Command(os.Args[0], "-test.run=^TestWALStoreCheckpointSIGKILLCrashWindows$")
			cmd.Env = append(os.Environ(),
				checkpointCrashHelperEnv+"="+tt.name,
				checkpointCrashPathEnv+"="+path,
			)
			if output, err := cmd.CombinedOutput(); err == nil {
				t.Fatalf("crash helper returned normally: %s", output)
			}

			reopened, err := OpenWALStore(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			checkpoint := reopened.CheckpointLSN()
			if checkpoint < tt.checkpointMin || checkpoint > tt.checkpointMax {
				t.Fatalf("checkpoint=%d want in [%d,%d]",
					checkpoint, tt.checkpointMin, tt.checkpointMax)
			}
			if _, err := reopened.Recover(); err != nil {
				t.Fatal(err)
			}
			got, err := reopened.Read(3)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, makeBlock(4096, 0x7c)) {
				t.Fatal("acknowledged bytes did not survive checkpoint crash window")
			}
		})
	}
}

func runCheckpointCrashHelper(window, path string) {
	s, err := CreateWALStore(path, 16, 4096)
	if err != nil {
		panic(err)
	}
	s.flusher.Stop()
	if _, err := s.Write(3, makeBlock(4096, 0x7c)); err != nil {
		panic(err)
	}
	if _, err := s.Sync(); err != nil {
		panic(err)
	}

	kill := func() error {
		process, err := os.FindProcess(os.Getpid())
		if err != nil {
			return err
		}
		return process.Kill()
	}
	switch window {
	case "after_extent_sync":
		s.writeSuperblockMetadata = func([]byte) error {
			return kill()
		}
	case "after_checkpoint_pwrite":
		s.writeSuperblockMetadata = func(data []byte) error {
			_, err := s.fd.WriteAt(data, 0)
			return err
		}
		s.syncSuperblockMetadata = kill
	case "after_checkpoint_sync":
		s.syncSuperblockMetadata = func() error {
			if err := s.fd.Sync(); err != nil {
				return err
			}
			return kill()
		}
	case "after_tail_publish":
	default:
		panic(fmt.Sprintf("unknown checkpoint crash window %q", window))
	}

	if err := s.flusher.flushOnce(); err != nil {
		panic(err)
	}
	if err := kill(); err != nil {
		panic(err)
	}
	panic("SIGKILL returned")
}
