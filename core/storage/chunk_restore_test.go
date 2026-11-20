package storage

import (
	"context"
	"errors"
	"testing"
)

type claimFaultRemote struct {
	ChunkRemote
	applied, failRead, called bool
	calls                     int
}

func (r *claimFaultRemote) CASHead(ctx context.Context, expected ChunkHeadVersion, next ChunkHead) (uint64, error) {
	r.calls++
	r.called = true
	if r.applied {
		if _, err := r.ChunkRemote.CASHead(ctx, expected, next); err != nil {
			return 0, err
		}
	}
	return 0, errors.New("injected lost CAS response")
}
func (r *claimFaultRemote) GetHead(ctx context.Context) (ChunkHead, error) {
	if r.called && r.failRead {
		return ChunkHead{}, errors.New("injected HEAD read outage")
	}
	return r.ChunkRemote.GetHead(ctx)
}

func TestClaimChunkHeadSettlesUnknownWithoutReplay(t *testing.T) {
	for _, applied := range []bool{false, true} {
		for _, failRead := range []bool{false, true} {
			remote := NewMemChunkRemote()
			if _, err := remote.CASHead(context.Background(), ChunkHeadVersion{}, ChunkHead{Epoch: 1, Holder: "old", RemoteLSN: 7, Chunks: map[uint64]ChunkRef{}}); err != nil {
				t.Fatal(err)
			}
			fault := &claimFaultRemote{ChunkRemote: remote, applied: applied, failRead: failRead}
			head, err := ClaimChunkHead(context.Background(), fault, ChunkLayerConfig{Epoch: 2, Holder: "new"})
			if (err == nil) != (applied && !failRead) {
				t.Fatalf("applied=%v readFail=%v err=%v", applied, failRead, err)
			}
			if fault.calls != 1 {
				t.Fatalf("CAS replayed %d times", fault.calls)
			}
			if err == nil && (head.Epoch != 2 || head.Holder != "new" || head.RemoteLSN != 7) {
				t.Fatal(head)
			}
		}
	}
}

func TestClaimChunkHeadRefusesStaleAuthorityAndAdmissionDrift(t *testing.T) {
	remote := NewMemChunkRemote()
	_, err := remote.CASHead(context.Background(), ChunkHeadVersion{}, ChunkHead{Epoch: 3, Holder: "current", Chunks: map[uint64]ChunkRef{}})
	if err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []ChunkLayerConfig{{Epoch: 2, Holder: "old"}, {Epoch: 3, Holder: "other"}} {
		if _, err := ClaimChunkHead(context.Background(), remote, cfg); !errors.Is(err, ErrChunkLayerFenced) {
			t.Fatalf("stale claim accepted: %v", err)
		}
	}
	before, _ := remote.GetHead(context.Background())
	checks := 0
	_, err = ClaimChunkHead(context.Background(), remote, ChunkLayerConfig{Epoch: 4, Holder: "new", CaptureAllowed: func() error {
		checks++
		if checks == 2 {
			return errors.New("lineage moved")
		}
		return nil
	}})
	after, _ := remote.GetHead(context.Background())
	if err == nil || after.Gen != before.Gen || after.Epoch != 3 {
		t.Fatalf("drift published: head=%+v err=%v", after, err)
	}
	// An identical already-settled claim is idempotent and creates no new gen.
	if _, err := ClaimChunkHead(context.Background(), remote, ChunkLayerConfig{Epoch: 3, Holder: "current"}); err != nil {
		t.Fatal(err)
	}
	final, _ := remote.GetHead(context.Background())
	if final.Gen != before.Gen {
		t.Fatal("idempotent claim advanced HEAD")
	}
}

func TestClaimChunkHeadFencesNoopCut(t *testing.T) {
	s, _ := newChunkTestStore(t)
	remote := newFakeChunkRemote()
	layer := mustAttach(t, s, remote, ctConfig(1))
	if _, err := s.Write(0, makeBlock(4096, 0xAA)); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := ClaimChunkHead(ctTimeout(t), remote, ChunkLayerConfig{Epoch: 2, Holder: "host-b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Capture(ctTimeout(t)); !errors.Is(err, ErrChunkLayerFenced) {
		t.Fatalf("stale no-op cut returned %v, want fenced", err)
	}
	if !layer.Fenced() {
		t.Fatal("no-op cut did not propagate fence")
	}
}

// A no-op cut still consults remote authority. An outage there is retryable
// remote unavailability, not an untyped failure or an ownership fence.
func TestClaimChunkHeadNoopCutClassifiesRemoteReadFailure(t *testing.T) {
	s, _ := newChunkTestStore(t)
	remote := newFakeChunkRemote()
	layer := mustAttach(t, s, remote, ctConfig(1))
	if _, err := s.Write(0, makeBlock(4096, 0xA7)); err != nil {
		t.Fatal(err)
	}
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	before, _ := remote.GetHead(ctTimeout(t))
	remote.mu.Lock()
	remote.failGet = errFakeRemoteDown
	calls := remote.casCalls
	remote.mu.Unlock()
	_, err := layer.Capture(ctTimeout(t))
	if !errors.Is(err, ErrChunkRemoteUnavailable) || !errors.Is(err, errFakeRemoteDown) {
		t.Errorf("no-op authority read: %v; want remote unavailable and underlying cause", err)
	}
	if layer.Fenced() {
		t.Error("remote read outage latched an ownership fence")
	}
	remote.mu.Lock()
	if remote.casCalls != calls {
		t.Error("no-op read failure attempted a CAS")
	}
	remote.failGet = nil
	remote.mu.Unlock()
	if _, err := layer.Capture(ctTimeout(t)); err != nil {
		t.Fatal("retry after remote recovery", err)
	}
	after, _ := remote.GetHead(ctTimeout(t))
	if after.Gen != before.Gen || after.RemoteLSN != before.RemoteLSN {
		t.Fatal("no-op retry changed HEAD", before, after)
	}
}
