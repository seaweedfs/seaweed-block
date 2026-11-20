package storage

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestCutPayloadIntentCountIndependentOfCandidates(t *testing.T) {
	for _, n := range []int{4, 12} {
		_, f, l, _ := precopyLayer(t, 0, n)
		l.cfg.UploadConcurrency = 1
		before := len(f.putIntents)
		r, err := l.Cut(ctTimeout(t))
		if err != nil {
			t.Fatal(err)
		}
		calls := len(f.putIntents) - before
		if calls > 1+r.Stats.StageRounds {
			t.Fatalf("candidates=%d intent writes=%d rounds=%d", n, calls, r.Stats.StageRounds)
		}
	}
}

type blip271Remote struct {
	*fakeChunkRemote
	calls    int
	failures int
}

func (r *blip271Remote) Put(ctx context.Context, key string, data []byte) error {
	r.calls++
	if r.calls <= r.failures {
		return errFakeRemoteDown
	}
	return r.fakeChunkRemote.Put(ctx, key, data)
}
func TestCutUploadRetriesPastThreeAttemptsAtSamePoint(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 0, 1)
	r := &blip271Remote{fakeChunkRemote: newFakeChunkRemote(), failures: 4}
	l.remote = r
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := makeBlock(ctBlockSize, 0x73)
	if err := l.putCaptured(ctx, "immutable", data); err != nil {
		t.Fatal(err)
	}
	got, err := r.Get(ctx, "immutable")
	if err != nil || !bytes.Equal(got, data) || r.calls != 5 {
		t.Fatalf("calls=%d data=%v err=%v", r.calls, bytes.Equal(got, data), err)
	}
}

func TestChunkCloseDropsReusableButKeepsUnknownCASRoots(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(map[bool]string{false: "known-abort", true: "unknown-CAS"}[pending], func(t *testing.T) {
			_, f, l, _ := precopyLayer(t, 0, 1)
			id, err := f.PutIntent(context.Background(), chunkIntent{Keys: []string{"unpublished"}})
			if err != nil {
				t.Fatal(err)
			}
			l.stMu.Lock()
			l.reusable = map[uint64]stagedChunk{0: {capturedChunk: capturedChunk{intent: id}}}
			if pending {
				l.pending = &chunkHead{Gen: 9}
				l.pendingIntents = []string{id}
			}
			l.stMu.Unlock()
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			live := len(liveIntents(t, f))
			if (pending && live != 1) || (!pending && live != 0) {
				t.Fatalf("pending=%v live=%d", pending, live)
			}
		})
	}
}

func TestV3ReadLargeBatchScratchIsBlockBounded(t *testing.T) {
	s, err := createMappedWALStoreV3ForTest(filepath.Join(t.TempDir(), "volume"), 512, 4096, 4<<20, WModeStrict, 4)
	if err != nil {
		t.Fatal(err)
	}
	s.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { _ = s.Close() })
	blocks := make([][]byte, 256)
	for i := range blocks {
		blocks[i] = makeBlock(4096, byte(i))
	}
	if _, err = s.WriteBatch(0, blocks); err != nil {
		t.Fatal(err)
	}
	off, dataOff, lsn, _, ok := s.dm.get(3)
	if !ok {
		t.Fatal("missing dirty batch")
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	got, ioErr, verr := s.readRecordV3(3, off, dataOff, lsn)
	runtime.ReadMemStats(&after)
	if ioErr != nil || verr != nil || !bytes.Equal(got, blocks[3]) {
		t.Fatalf("read: %v %v", ioErr, verr)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 96<<10 {
		t.Fatalf("16KiB chunk /96KiB budget: single batch read allocated %d", allocated)
	}
	// Corruption outside the requested block must still fail the whole-record CRC.
	at := int64(s.sb.WALOffset + off + walEntryPrefixSizeV3 + 255*4096)
	if _, err = s.fd.WriteAt([]byte{0xff}, at); err != nil {
		t.Fatal(err)
	}
	_, ioErr, verr = s.readRecordV3(3, off, dataOff, lsn)
	if ioErr != nil || !errors.Is(verr, errCRCMismatch) {
		t.Fatalf("unrequested payload corruption: io=%v validation=%v", ioErr, verr)
	}
}
