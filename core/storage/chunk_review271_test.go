package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCutPayloadIntentCountIndependentOfCandidates(t *testing.T) {
	for _, n := range []int{4, 8} {
		_, f, l, _ := precopyLayer(t, 0, n)
		l.cfg.UploadConcurrency = 1
		before := len(f.putIntents)
		mark := len(f.stats().ops)
		r, err := l.Cut(ctTimeout(t))
		if err != nil {
			t.Fatal(err)
		}
		rooted := map[string]bool{}
		for _, in := range f.putIntents[before:] {
			for _, key := range in.Keys {
				rooted[key] = true
			}
		}
		for _, key := range uploadedKeys(opsSince(f, mark)) {
			if !rooted[key] {
				t.Fatalf("unrooted upload %s", key)
			}
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
	// Exercise the same scratch accounting through the attached COW reader.
	cfg := ctConfig(1)
	cfg.Bitmap, cfg.FreshFromHead, cfg.PayloadMemoryBudget = nil, true, 96<<10
	l := mustAttach(t, s, newFakeChunkRemote(), cfg).(*chunkLayerImpl)
	w := &cutCOWWindow{chunkBytes: 16 << 10, ended: make(chan struct{})}
	runtime.ReadMemStats(&before)
	image, missing, readErr := l.readCOWChunk(w, 0)
	runtime.ReadMemStats(&after)
	if readErr != nil || len(missing) != 0 || !bytes.Equal(image, bytes.Join(blocks[:4], nil)) {
		t.Fatalf("COW batch image: %v missing=%v", readErr, missing)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 96<<10 {
		t.Fatalf("COW batch read allocated %d above configured budget", allocated)
	}
	// Corruption outside the requested block must still fail the whole-record CRC.
	at := int64(s.sb.WALOffset + off + walEntryPrefixSizeV3 + 255*4096)
	if _, err = s.fd.WriteAt([]byte{0x17}, at); err != nil {
		t.Fatal(err)
	}
	_, ioErr, verr = s.readRecordV3(3, off, dataOff, lsn)
	if ioErr != nil || !errors.Is(verr, errCRCMismatch) {
		t.Fatalf("unrequested payload corruption: io=%v validation=%v", ioErr, verr)
	}
}

func TestCutAbortReasonsAreCountedAndCopied(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 0, 1)
	for reason, cause := range map[string]error{"saver_capacity": errCutSaverCapacity, "cow_capacity": errCutCOWCapacity, "budget_revoked": errCutBudgetRevoked, "upload": errCutUpload} {
		l.recordCut(chunkCutResult{}, chunkCutStats{}, errors.Join(errCutCOWAborted, cause))
		st := l.Stats()
		if st.CutsAborted[reason] != 1 {
			t.Fatalf("%s: %+v", reason, st.CutsAborted)
		}
		st.CutsAborted[reason] = 100
		if l.Stats().CutsAborted[reason] != 1 {
			t.Fatal("Stats exposed mutable counter map")
		}
	}
	raw, err := json.Marshal(l.Stats())
	if err != nil || !bytes.Contains(raw, []byte(`"CutsAborted"`)) {
		t.Fatalf("status: %s %v", raw, err)
	}
}

type overlap271Remote struct {
	*fakeChunkRemote
	first   chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (r *overlap271Remote) Put(ctx context.Context, key string, data []byte) error {
	if r.calls.Add(1) == 1 {
		close(r.first)
		select {
		case <-r.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.fakeChunkRemote.Put(ctx, key, data)
}
func TestCutPayloadReadsOverlapEarlierUploadsWithinBudget(t *testing.T) {
	_, _, l, _ := precopyLayer(t, 0, 1)
	l.cfg.UploadConcurrency = 2
	r := &overlap271Remote{fakeChunkRemote: newFakeChunkRemote(), first: make(chan struct{}), release: make(chan struct{})}
	l.remote = r
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readThird, done := make(chan struct{}), make(chan struct{})
	errs := make(chan error, 1)
	var once sync.Once
	free := func() { once.Do(func() { close(r.release) }) }
	t.Cleanup(func() { cancel(); free(); cowWaitSignal(t, done, "overlap cleanup") })
	go func() {
		defer close(done)
		_, _, err := l.streamCutPayloads(ctx, []uint64{0, 1, 2}, chunkHead{}, false, func(_ context.Context, idx uint64) (capturedChunk, error) {
			if idx == 2 {
				close(readThird)
			}
			data := makeBlock(ctBlockSize, byte(idx+1))
			return capturedChunk{idx: idx, data: data, sum: sha256.Sum256(data)}, nil
		})
		errs <- err
	}()
	cowWaitSignal(t, r.first, "held first PUT")
	select {
	case <-readThird:
	case <-time.After(time.Second):
		t.Fatal("next local read waited for earlier remote PUT")
	}
	free()
	cowWaitSignal(t, done, "overlapping upload join")
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func TestCutPayloadIntentCountLargeCandidateSets(t *testing.T) {
	for _, n := range []int{256, 4096} {
		_, _, l, _ := precopyLayer(t, 0, 1)
		f := newFakeChunkRemote()
		l.remote = f
		batch := make([]uint64, n)
		for i := range batch {
			batch[i] = uint64(i)
		}
		_, ids, err := l.streamCutPayloads(ctTimeout(t), batch, chunkHead{}, false, func(_ context.Context, idx uint64) (capturedChunk, error) {
			data := makeBlock(ctBlockSize, byte(idx%255+1))
			return capturedChunk{idx: idx, data: data, sum: sha256.Sum256(data)}, nil
		})
		if err != nil || len(ids) != 1 || len(f.putIntents) != 1 || len(f.putIntents[0].Keys) != n {
			t.Fatalf("D=%d intents=%d ids=%d err=%v", n, len(f.putIntents), len(ids), err)
		}
	}
}
