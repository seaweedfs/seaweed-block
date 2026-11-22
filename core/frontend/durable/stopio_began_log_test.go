package durable_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// A detach logs when its I/O stop began (the operational flip, from which
// the backend refuses new I/O) as well as when StopIO returned, both with
// RFC 3339 nanoseconds, so a test measuring guest errors can tell a refusal
// by contract (after the flip) from an admitted I/O that failed.
func TestLiveDetachLogsWhenTheIOStopBegan(t *testing.T) {
	p := chunkProvider(t, t.TempDir(), storage.NewMemChunkRemote(), v1View(), time.Hour)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(context.Background(), 4096, block(0x51)); err != nil {
		t.Fatal(err)
	}
	logs := &lockedLog{}
	captureWriteLogs(t, logs)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := time.Now()
	if _, _, err := p.DetachVolume(ctx, "v1"); err != nil {
		t.Fatal(err)
	}
	ended := time.Now()

	stamp := func(marker string) time.Time {
		t.Helper()
		var found []time.Time
		for _, line := range strings.Split(logs.String(), "\n") {
			if _, s, ok := strings.Cut(line, marker); ok {
				at, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(s))
				if err != nil {
					t.Fatalf("%q: %v", line, err)
				}
				found = append(found, at)
			}
		}
		if len(found) != 1 {
			t.Fatalf("%d lines with %q, want 1:\n%s", len(found), marker, logs.String())
		}
		return found[0]
	}
	began := stamp("volume=v1 I/O stop began at ")
	returned := stamp("volume=v1 StopIO returned at ")
	if began.Before(started) || returned.Before(began) || ended.Before(returned) {
		t.Fatalf("began %s, returned %s: want started %s <= began <= returned <= ended %s", began, returned, started, ended)
	}
}
