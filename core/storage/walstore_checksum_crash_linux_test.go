//go:build linux

package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestWALStoreC12_CrashBetweenDataAndCRCRepairsFromWAL(t *testing.T) {
	if path := os.Getenv("SWB_C12_CRASH_PATH"); path != "" {
		s, err := CreateWALStore(path, 16, 4096)
		if err != nil {
			panic(err)
		}
		s.flusher.Stop()
		if _, err = s.Write(3, makeBlock(4096, 0x71)); err != nil {
			panic(err)
		}
		if _, err = s.Sync(); err != nil {
			panic(err)
		}
		s.testAfterExtentDataWrite = func() error {
			if err := s.fd.Sync(); err != nil {
				panic(err)
			}
			if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
				panic(err)
			}
			select {}
		}
		if err = s.flusher.flushOnce(); err != nil {
			panic(err)
		}
		t.Fatal("crash hook not reached")
		return
	}
	path := filepath.Join(t.TempDir(), "store")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWALStoreC12_CrashBetweenDataAndCRCRepairsFromWAL$")
	cmd.Env = append(os.Environ(), "SWB_C12_CRASH_PATH="+path)
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	cmd.WaitDelay = time.Second
	err := cmd.Run()
	var exit *exec.ExitError
	if ctx.Err() != nil || !errors.As(err, &exit) || exit.Sys().(syscall.WaitStatus).Signal() != syscall.SIGKILL {
		t.Fatalf("expected owned SIGKILL: %v context=%v", err, ctx.Err())
	}
	s, err := OpenWALStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.flusher.Stop()
	if s.CheckpointLSN() != 0 {
		t.Fatal("crash advanced checkpoint")
	}
	if _, err = s.readFromExtent(3); !errors.Is(err, ErrDataChecksum) {
		t.Fatalf("fixture has no actual torn CRC pair: %v", err)
	}
	if _, err = s.Recover(); err != nil {
		t.Fatal(err)
	}
	want := makeBlock(4096, 0x71)
	got, err := s.Read(3)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("WAL replay did not repair read: %v", err)
	}
	if err = s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	got, err = s.readFromExtent(3)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("repaired extent/CRC not usable: %v", err)
	}
}
