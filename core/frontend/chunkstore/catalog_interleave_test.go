package chunkstore

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Interleaved sweeps and name reuse (review of #119 r4): every decision is
// taken from ONE read of the entry acted on, and a fork's or snapshot's
// success from re-reading the entry it tried to create.

func abortedHeadEntry(t *testing.T) *filerpb.Entry {
	t.Helper()
	he, err := headEntry(storage.ChunkHead{Gen: 1, Chunks: map[uint64]storage.ChunkRef{}, State: storage.ChunkHeadAborted})
	if err != nil {
		t.Fatal(err)
	}
	return he
}

// A sweep claims the fork's HEAD right after the attach gate's first read:
// the gate must not pass the aborted HEAD.
func TestEnsureVolume_OneHeadReadDecides(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	holdFirst(f, catBase+"/v2/HEAD")
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeOutcomeUnknown {
		t.Fatalf("fork with a held HEAD create: %v", err)
	}
	tomb := abortedHeadEntry(t)
	done := false
	f.afterLookup = func(p string) {
		if p == catBase+"/v2/HEAD" && !done {
			done = true
			f.entries[p] = tomb // the sweep wins right after the first read
		}
	}
	if _, err := c.EnsureVolume(ctx, "v2", catGeo); CodeOf(err) != CodeForkIncomplete {
		t.Fatalf("attach with a sweep between reads: %v; want fork_incomplete", err)
	}
	f.afterLookup = nil
}

// A sweep claims the fork's HEAD after the fork wrote its intent and before
// its HEAD create: the fork lost, so it must not report success.
func TestFork_LosesToASweepBeforeItsHeadCreate(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	tomb := abortedHeadEntry(t)
	f.beforeCreate = func(p string) {
		if path.Dir(p) == catBase+"/v2/intents" {
			f.entries[catBase+"/v2/HEAD"] = tomb
		}
	}
	if res, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "f"}); CodeOf(err) != CodeTargetExists {
		t.Fatalf("fork that lost to a sweep: %+v %v; want target_exists", res, err)
	}
	if in := intentsOf(t, f, "v2"); len(in) != 0 {
		t.Fatalf("a fork that lost to a tombstone left %d intents", len(in))
	}
	f.beforeCreate = nil
}

// The name is deleted and reused by another request between a snapshot's
// create and its completion: the caller gets its own attempt, not the name's
// new owner.
func TestCatalog_SnapshotCompletionReadsItsOwnAttempt(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	f.failCreateAfterApply = func(p string) error {
		if isSnapPath(p, "v1", "s1") {
			return errTransport // applied; the response is lost
		}
		return nil
	}
	reused := false
	f.afterLookup = func(p string) {
		if reused || !isSnapPath(p, "v1", "s1") {
			return
		}
		reused = true
		// Meanwhile: the name is deleted and taken by request rB, whose
		// snapshot carries other labels.
		attB := "1-00000000000000bb"
		rec, _ := nameRecordEntry(nameRecord{Format: snapFormat, VolumeID: "v1", Name: "s1", RequestID: "rB", Attempt: attB})
		f.entries[catBase+"/v1/"+snapNamesDir+"/s1"] = rec
		other := proto.Clone(f.entries[p]).(*filerpb.Entry)
		var doc snapDoc
		_ = json.Unmarshal(other.Content, &doc)
		doc.RequestID, doc.Labels = "rB", map[string]string{"who": "b"}
		other.Content, _ = json.Marshal(doc)
		other.Name = attB
		other.Extended[extReq] = []byte("rB")
		f.entries[path.Join(path.Dir(p), attB)] = other
	}
	info, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "rA", Labels: map[string]string{"who": "a"}}, h)
	if err != nil || info.Labels["who"] != "a" {
		t.Fatalf("completion after the name was reused: %+v %v; want the caller's own snapshot (labels who=a)", info, err)
	}
	f.afterLookup, f.failCreateAfterApply = nil, nil
}

// A sweep's tombstone lands on the attempt path before the create: the
// create is refused for good and reported aborted, never as this attempt's.
func TestCatalog_SnapshotTombstoneIsNeverThisAttempt(t *testing.T) {
	f, _, c, h := catalogFixture(t)
	ctx := context.Background()
	f.beforeCreate = func(p string) {
		if path.Dir(p) != catBase+"/v1/intents" {
			return
		}
		re := f.entries[catBase+"/v1/"+snapNamesDir+"/s1"]
		attempt := string(re.Extended[extAttempt])
		tomb, _ := c.abortedSnapEntry("v1", "s1", attempt)
		f.entries[catBase+"/v1/snaps/s1/"+attempt] = tomb
	}
	_, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h)
	if CodeOf(err) != CodeSnapshotExists || !strings.Contains(err.Error(), "aborted") {
		t.Fatalf("create over its own attempt's tombstone: %v; want the aborted refusal", err)
	}
	if in := intentsOf(t, f, "v1"); len(in) != 0 {
		t.Fatalf("an attempt that lost to a tombstone left %d intents", len(in))
	}
	f.beforeCreate = nil
}
