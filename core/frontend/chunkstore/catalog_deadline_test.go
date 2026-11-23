package chunkstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// A snapshot or fork copy intent records its publication deadline, and the
// publishing create runs under a context bounded by that deadline (W5).
// The deadline is now metadata only:
// a sweep settles a dead attempt by a tombstone claim (catalog_tombstone).

func TestCatalog_CopyIntentRecordsItsDeadline(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	t0 := time.Unix(1_800_000_000, 0)
	c.SetClock(func() time.Time { return t0 })
	holdFirst(f, catBase+"/v1/snaps/s1/")
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r", SourceReadAt: t0}, h); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("held snapshot create: %v", err)
	}
	in := intentsOf(t, f, "v1")
	if len(in) != 1 || in[0].DeadlineNs != t0.Add(DefaultOpDeadline).UnixNano() {
		t.Fatalf("snapshot intent %+v; want deadline %d", in, t0.Add(DefaultOpDeadline).UnixNano())
	}

	f.deferCreate = nil
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s2", RequestID: "r2"}, h); err != nil {
		t.Fatal(err)
	}
	holdFirst(f, catBase+"/v2/HEAD")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s2", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("held fork HEAD create: %v", err)
	}
	if fin := intentsOf(t, f, "v2"); len(fin) != 1 || fin[0].DeadlineNs != t0.Add(DefaultOpDeadline).UnixNano() {
		t.Fatalf("fork intent %+v; want deadline %d", fin, t0.Add(DefaultOpDeadline).UnixNano())
	}
}

func TestCatalog_PublishIsBoundedByTheDeadline(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	c.OpDeadline = 300 * time.Millisecond
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s0", RequestID: "r0"}, h); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, path string
		run        func() error
	}{
		{"snapshot", catBase + "/v1/snaps/s1", func() error {
			_, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r1"}, h)
			return err
		}},
		{"fork", catBase + "/v2/HEAD", func() error {
			_, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s0", Target: "v2", RequestID: "f"})
			return err
		}},
	} {
		stalled := tc.path
		f.stallCreate = func(p string) bool { return p == stalled || strings.HasPrefix(p, stalled+"/") }
		done := make(chan error, 1)
		start := time.Now()
		go func() { done <- tc.run() }()
		select {
		case err := <-done:
			if CodeOf(err) != CodeOutcomeUnknown || time.Since(start) > 2*time.Second {
				t.Fatalf("%s against a filer that never answers: %v after %v; want outcome_unknown by the deadline", tc.name, err, time.Since(start))
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the publishing create outlived its deadline (still waiting after 5s)", tc.name)
		}
		f.stallCreate = nil
	}
}

// A delayed source read must not create a target, and an expired attempt
// after VOLUME must not create a copy intent, even one it later deletes.
func TestFork_ExpiredAttemptWritesNoNewMetadata(t *testing.T) {
	for _, origin := range []string{OriginFork, OriginRestore} {
		for _, stage := range []string{"source_read", "volume_written", "live"} {
			t.Run(origin+"/"+stage, func(t *testing.T) {
				f, _, c, h := catalogFixture(t)
				ctx := context.Background()
				now := time.Unix(1800000000, 0)
				c.SetClock(func() time.Time { return now })
				if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "s"}, h); err != nil {
					t.Fatal(err)
				}
				start := now
				volumeWrites, intentWrites := 0, 0
				f.afterLookup = func(p string) {
					if stage == "source_read" && isSnapPath(p, "v1", "s1") {
						now = start.Add(DefaultOpDeadline)
					}
				}
				f.beforeCreate = func(p string) {
					if p == catBase+"/v2/VOLUME" {
						volumeWrites++
						if stage == "volume_written" {
							now = start.Add(DefaultOpDeadline)
						}
					}
					if strings.HasPrefix(p, catBase+"/v2/intents/") {
						intentWrites++
					}
				}
				req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}
				var err error
				if origin == OriginFork {
					_, err = c.Fork(ctx, req)
				} else {
					_, err = c.Restore(ctx, req)
				}
				f.afterLookup, f.beforeCreate = nil, nil
				if stage == "live" {
					if err != nil || volumeWrites != 1 || intentWrites != 1 {
						t.Fatalf("live attempt: err=%v VOLUME=%d intents=%d", err, volumeWrites, intentWrites)
					}
					return
				}
				if CodeOf(err) != CodeDeadlineExceeded {
					t.Fatalf("expired attempt: %v", err)
				}
				wantVolume := 0
				if stage == "volume_written" {
					wantVolume = 1
				}
				if volumeWrites != wantVolume || intentWrites != 0 {
					t.Fatalf("expired attempt wrote VOLUME=%d intents=%d; want %d and 0", volumeWrites, intentWrites, wantVolume)
				}
				if _, err := f.LookupEntry(ctx, catBase+"/v2", headName); err == nil {
					t.Fatal("expired attempt published HEAD")
				}
				if intents := intentsOf(t, f, "v2"); len(intents) != 0 {
					t.Fatalf("expired attempt left intents: %+v", intents)
				}
			})
		}
	}
}

// The frozen flat snapshot forces the manifest-upgrade path to create an
// intent before uploading its segment; count creates, even if later removed.
func TestFork_ExpiredLegacyPreparationWritesNoIntent(t *testing.T) {
	for _, origin := range []string{OriginFork, OriginRestore} {
		for _, expired := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/expired=%t", origin, expired), func(t *testing.T) {
				f, c, r := preF4ForkFixture(t, false)
				now := time.Unix(1800000000, 0)
				start := now
				c.SetClock(func() time.Time { return now })
				intents := 0
				f.beforeCreate = func(p string) {
					if p == catBase+"/v3/VOLUME" && expired {
						now = start.Add(DefaultOpDeadline)
					}
					if strings.HasPrefix(p, catBase+"/v3/intents/") {
						intents++
					}
				}
				req := ForkRequest{Source: "v1", Snapshot: "s1", Target: "v3", RequestID: "legacy-deadline"}
				var err error
				if origin == OriginFork {
					_, err = c.Fork(context.Background(), req)
				} else {
					_, err = c.Restore(context.Background(), req)
				}
				f.beforeCreate = nil
				if expired {
					if CodeOf(err) != CodeDeadlineExceeded || intents != 0 {
						t.Fatalf("expired legacy attempt: err=%v intent creates=%d; want deadline and zero", err, intents)
					}
					if _, err := f.LookupEntry(context.Background(), catBase+"/v3", headName); err == nil {
						t.Fatal("expired legacy attempt published HEAD")
					}
				} else {
					if err != nil || intents != 2 {
						t.Fatalf("live legacy attempt: err=%v intent creates=%d; want success and upgrade+copy intents", err, intents)
					}
					dst, err := NewChunkRemote(f, r.b, catBase+"/v3")
					if err != nil {
						t.Fatal(err)
					}
					assertLegacyImage(t, dst)
				}
			})
		}
	}
}
