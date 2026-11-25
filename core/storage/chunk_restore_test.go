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

func TestChunkNoopCutClassifiesRemoteReadFailure(t *testing.T) {
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
