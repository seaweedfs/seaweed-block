package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

var errTransportGet = errors.New("connection reset")

// A fork whose exclusive VOLUME create finds another VOLUME already there
// must read it to judge it; when that read fails for now the fork is
// retryable (outcome unknown), never a permanent target_exists.
func TestRecord_ForkTargetReadErrorIsRetryable(t *testing.T) {
	chunkedRecords(t)
	f, b, c, h := catalogFixture(t)
	ctx := context.Background()
	if _, err := c.CreateSnapshot(ctx, "v1", SnapshotRequest{Name: "s1", RequestID: "r"}, h); err != nil {
		t.Fatal(err)
	}
	// A chunked VOLUME written by another fork request, planted at the
	// target just before this fork's create (a lost race).
	if _, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v9", RequestID: "other"}); err != nil {
		t.Fatal(err)
	}
	theirs := recordEntry(t, f, catBase+"/v9/VOLUME")
	fid := theirs.GetChunks()[0].GetFileId()
	f.failCreate = func(p string) error {
		if p == catBase+"/v2/VOLUME" {
			f.entries[p] = theirs
			return fmt.Errorf("%w: %s", ErrExists, p)
		}
		return nil
	}
	b.failGet = func(got string) error {
		if got == fid {
			return errTransportGet
		}
		return nil
	}
	_, err := c.Fork(ctx, ForkRequest{Source: "v1", Snapshot: "s1", Target: "v2", RequestID: "fork-1"})
	var ce *CatalogError
	if CodeOf(err) == CodeTargetExists || !errors.As(err, &ce) || !ce.Retryable {
		t.Fatalf("unreadable target VOLUME: %v (code %s); want retryable, not target_exists", err, CodeOf(err))
	}
}

// Verify over an intent whose payload cannot be read for now (transport)
// is incomplete (an error), not a complete pass with a finding.
func TestRecord_VerifyIntentTransportErrorIsIncomplete(t *testing.T) {
	chunkedRecords(t)
	fx := newVerifyFixture(t, "v1")
	fx.publish("v1", map[uint64][]byte{0: verifyData(1)}, false)
	r, err := NewChunkRemote(fx.f, fx.b, catBase+"/v1")
	if err != nil {
		t.Fatal(err)
	}
	id, err := r.PutIntent(context.Background(), storage.ChunkIntent{Gen: 9, Epoch: 1, Holder: "h1", Keys: []string{"k"}})
	if err != nil {
		t.Fatal(err)
	}
	fid := recordEntry(t, fx.f, catBase+"/v1/intents/"+id).GetChunks()[0].GetFileId()
	fx.b.failGet = func(f string) error {
		if f == fid {
			return errTransportGet
		}
		return nil
	}
	res, err := fx.c.Verify(context.Background(), "v1", VerifyOptions{})
	if err == nil && res.Complete {
		t.Fatalf("verify over an unreadable intent: complete, findings %+v", res.Findings)
	}
}

// The assign path is the directory form the start check judges, so a rule
// on exactly <base>/ that the check accepts is applied to assigned needles.
func TestPathRule_AssignPathIsTheCheckedPath(t *testing.T) {
	const base = "/swblock/chunks"
	if got := AssignPathFor(base); got != base+"/" || AssignPathFor(base+"/") != got {
		t.Fatalf("assign path %q", got)
	}
	rules, err := parseFilerConf([]byte(`{"version":2,"locations":[{"locationPrefix":"/swblock/chunks/","fsync":true}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkPathRule(rules, base, ""); err != nil || !strings.HasPrefix(AssignPathFor(base), rules[0].prefix) {
		t.Fatalf("a rule on %s/ refused (%v) or not a prefix of the assign path %q", base, err, AssignPathFor(base))
	}
}
