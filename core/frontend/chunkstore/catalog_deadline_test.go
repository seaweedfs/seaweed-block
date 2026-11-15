package chunkstore

import (
	"context"
	"testing"
	"time"
)

// A snapshot or fork copy intent records its publication deadline, and the
// publishing create runs under a context bounded by that deadline (W5
// ruling, 2026-11-15 17:06Z): GC may expire copy intents by deadline.

func TestCatalog_CopyIntentRecordsItsDeadline(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	t0 := time.Unix(1_800_000_000, 0)
	c.SetClock(func() time.Time { return t0 })
	holdFirst(f, catBase+"/v1/snaps/s1")
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
		f.stallCreate = func(p string) bool { return p == stalled }
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
