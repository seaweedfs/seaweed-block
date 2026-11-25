package durable_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable/testutil"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"testing"
	"time"
)

func TestFrozenProviderFlagOffRefusesWithoutUploadThenFinalCommits(t *testing.T) {
	fixture := testutil.NewLayoutFixture(t, "v1", pcBlocks, pcChunkBlocks)
	p, _ := parkedLayoutProvider(t, fixture)
	openRecovered(t, p)
	b, err := p.Open(context.Background(), "v1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Write(context.Background(), 4096, block(0x77)); err != nil {
		t.Fatal(err)
	}
	before := layoutHead(t, fixture)
	puts := fixture.Blobs.Puts()
	r, _, err := p.DetachVolumeFrozen(context.Background(), "v1", storage.FrozenDetachOptions{StartedAt: time.Now().Add(-6 * time.Second)})
	after := layoutHead(t, fixture)
	if !errors.Is(err, storage.ErrFrozenTailTooLarge) || r.Precopy.Frozen || puts != fixture.Blobs.Puts() || before.Gen != after.Gen {
		t.Fatalf("frozen refusal mutated remote: %+v %v puts=%d/%d", r.Precopy, err, puts, fixture.Blobs.Puts())
	}
	if _, err = b.Write(context.Background(), 8192, block(0x88)); err != nil {
		t.Fatalf("refusal stopped serving before thaw: %v", err)
	}
	r, _, err = p.DetachVolumeFrozen(context.Background(), "v1", storage.FrozenDetachOptions{StartedAt: time.Now().Add(-time.Minute), AllowOverBudget: true})
	if err != nil || !r.Precopy.GuestFrozen || !r.Precopy.OverBudget || !r.Precopy.Frozen || r.Head.State != storage.ChunkHeadDetached || len(r.Precopy.Rounds) != 1 || r.Precopy.FreezeElapsed < time.Minute || r.Precopy.DeclaredWorst < time.Minute || !r.Precopy.LatencyGateFailed {
		t.Fatalf("explicit final failed/underreported total freeze: %+v %v", r, err)
	}
	original, _ := json.Marshal(r.Precopy)
	originalHead := r.Head.Gen
	puts = fixture.Blobs.Puts()
	replay, _, replayErr := p.DetachVolumeFrozen(context.Background(), "v1", storage.FrozenDetachOptions{StartedAt: time.Now()})
	replayJSON, _ := json.Marshal(replay.Precopy)
	if replayErr != nil || replay.Head.Gen != originalHead || !bytes.Equal(original, replayJSON) || fixture.Blobs.Puts() != puts {
		t.Fatalf("provider replay changed original freeze report or re-uploaded: original=%s replay=%s err=%v", original, replayJSON, replayErr)
	}
	if layoutHeadBlock(t, fixture, 1)[0] != 0x77 || layoutHeadBlock(t, fixture, 2)[0] != 0x88 {
		t.Fatal("final detached point lost writes")
	}
}
