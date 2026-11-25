package storage

import (
	"context"
	"reflect"
	"testing"
	"time"
)

type fixedSpanRemote struct {
	*fakeChunkRemote
	delay time.Duration
}

func (r *fixedSpanRemote) CASHead(ctx context.Context, v ChunkHeadVersion, h ChunkHead) (uint64, error) {
	time.Sleep(r.delay)
	return r.fakeChunkRemote.CASHead(ctx, v, h)
}

func TestChunkCutFixedSpansAttributeManifestAndCleanup(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 0)
	delay := 10 * time.Millisecond
	l.remote = &fixedSpanRemote{fakeChunkRemote: f, delay: delay}
	w.write(t, s, 1, 0x32)
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"CASWriteNs"} {
		v := reflect.ValueOf(r.Stats).FieldByName(name)
		if !v.IsValid() || time.Duration(v.Int()) < delay {
			t.Fatalf("missing fixed span %s: %+v", name, r.Stats)
		}
	}
	if len(l.precopySamples) == 0 {
		t.Fatal("no fixed sample")
	}
	parts := reflect.ValueOf(l.precopySamples[len(l.precopySamples)-1].parts)
	for _, name := range []string{"HeadCAS"} {
		v := parts.FieldByName(name)
		if !v.IsValid() || time.Duration(v.Int()) < delay {
			t.Fatalf("missing selected fixed part %s", name)
		}
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}

func TestPrecopyLiveBarrierSpansExplainOverage(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 1024, 0)
	seedFrozenSamples(l, time.Millisecond)
	delay := 20 * time.Millisecond
	l.testBeforeBarrierLockHook = func() { time.Sleep(delay) }
	r, err := l.PrecopyDetach(ctTimeout(t), func() bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	if r.Precopy.BarrierOverage <= 0 || r.Precopy.BarrierParts.StoreLock < delay || len(r.Precopy.Rounds) != 1 || r.Precopy.Rounds[0].BarrierParts.StoreLock < delay {
		t.Fatalf("live lock overage hidden as retained other: %+v", r.Precopy)
	}
}

func TestPrecopyFixedPartsDoNotDoubleCountSyncPhases(t *testing.T) {
	st := chunkCutStats{SyncNs: int64(100 * time.Millisecond), SyncPhases: SyncPhaseStats{StoreWaitNs: int64(30 * time.Millisecond), GroupCommitNs: int64(70 * time.Millisecond)}}
	p := precopyFixedParts(st, 150*time.Millisecond)
	if p.DataSync != 100*time.Millisecond || p.Other != 50*time.Millisecond || p.SyncPhases != st.SyncPhases {
		t.Fatalf("nested Sync cost counted twice or dropped: %+v", p)
	}
}

func TestCheckpointTailScanIncludedInTotalAndFixedCosts(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 0)
	l.remote = &fixedSpanRemote{fakeChunkRemote: f, delay: time.Millisecond}
	w.write(t, s, 1, 0x6a)
	delay := 20 * time.Millisecond
	scans := 0
	l.testBeforeTailScanHook = func() { scans++; time.Sleep(delay) }
	result, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if scans != 1 || result.Stats.CandidateNs < int64(delay) || result.Stats.TotalNs < int64(delay) {
		t.Fatalf("scan missing from returned costs: scans=%d stats=%+v", scans, result.Stats)
	}
	sample := l.precopySamples[len(l.precopySamples)-1]
	if sample.fixed < delay || sample.parts.Candidate < delay {
		t.Fatalf("scan omitted from fixed observation: %+v", sample)
	}
}
