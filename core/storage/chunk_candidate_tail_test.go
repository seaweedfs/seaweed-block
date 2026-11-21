package storage

import (
	"testing"
	"time"
)

func TestCheckpointTailMeasuresWritesAfterItsCut(t *testing.T) {
	s, f, l, w := precopyLayer(t, 1024, 0)
	for i := 0; i < 3; i++ {
		l.precopySamples = append(l.precopySamples, precopySample{at: time.Now(), fixed: time.Millisecond, rate: 2 * float64(ctChunkBytes)})
	}
	w.write(t, s, 1, 0x11)
	f.beforeCAS = func(*fakeChunkRemote) { f.beforeCAS = nil; w.write(t, s, 2, 0x22) }
	r, err := l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	tail := r.CandidateTail
	if tail == nil || tail.Bytes != ctChunkBytes || !tail.Known || tail.BytesPerSecond <= 0 {
		t.Fatalf("missing post-cut dirty tail: %+v", tail)
	}
	expected := time.Duration(float64(tail.Bytes) / tail.BytesPerSecond * float64(time.Second))
	if expected > precopyDuration {
		expected = precopyDuration
	}
	if tail.Estimate != expected {
		t.Fatalf("tail and rate sampled inconsistently: %+v want=%s", tail, expected)
	}
	r, err = l.Cut(ctTimeout(t))
	if err != nil {
		t.Fatal(err)
	}
	if r.CandidateTail == nil || r.CandidateTail.Bytes != 0 || !r.CandidateTail.Known || r.CandidateTail.Estimate != 0 {
		t.Fatalf("settled checkpoint not clean: %+v", r.CandidateTail)
	}
	assertEveryHeadIsPrefixImage(t, f, w)
}
func TestCheckpointTailUnknownRateIsNotZeroCost(t *testing.T) {
	s, _, l, w := precopyLayer(t, 1024, 0)
	w.write(t, s, 1, 0x44)
	l.precopySamples = []precopySample{{at: time.Now(), fixed: time.Millisecond, rate: 100}}
	tail := l.candidateTail()
	if tail.Bytes == 0 || tail.Known || tail.RateSamples != 1 {
		t.Fatalf("partial rate window misreported ready: %+v", tail)
	}
}
