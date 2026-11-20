package storage

import (
	"bytes"
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// A fully WAL-resident 4 MiB chunk requires 1024 independently validated
// single-block records. Account for both the uploader and the guest saver.
func TestCutCOWV3_WALResidentChunkReadCost(t *testing.T) {
	const blocks = 1024
	s, err := CreateWALStoreWithOptions(filepath.Join(t.TempDir(), "volume"), 2*blocks, ctBlockSize, WALStoreOptions{ChunkBlocks: blocks})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	s.DisableAutoFlushForRecoveryTest()
	if _, err = s.Recover(); err != nil {
		t.Fatal(err)
	}
	cfg := ctConfig(1)
	cfg.Bitmap, cfg.ChunkBlocks, cfg.BarrierBound, cfg.UploadConcurrency = nil, blocks, 0, 1
	f := newFakeChunkRemote()
	l := mustAttach(t, s, f, cfg).(*chunkLayerImpl)
	if _, err = l.Cut(ctTimeout(t)); err != nil {
		t.Fatal(err)
	}
	data := makeBlock(ctBlockSize, 0x31)
	for _, first := range []uint32{blocks, 0} {
		for i := uint32(0); i < blocks; i++ {
			if _, err = s.Write(first+i, data); err != nil {
				t.Fatal(err)
			}
		}
	}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	free := func() { once.Do(func() { close(release) }) }
	l.testCOWRead = func(idx uint64, _ bool) {
		if idx == 0 {
			close(entered)
			<-release
		}
	}
	results := make(chan chunkCutResult, 1)
	errs := make(chan error, 1)
	t.Cleanup(func() { free(); cowWaitSignal(t, done, "WAL cost cleanup") })
	go func() { defer close(done); r, e := l.Cut(ctTimeout(t)); results <- r; errs <- e }()
	cowWaitSignal(t, entered, "WAL-resident uploader")
	start := time.Now()
	_, err = s.Write(blocks, makeBlock(ctBlockSize, 0x9a))
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed >= ctLocalBound {
		t.Fatalf("one local chunk read took %v", elapsed)
	}
	free()
	cowWaitSignal(t, done, "WAL-resident cut")
	if err = <-errs; err != nil {
		t.Fatal(err)
	}
	r := <-results
	if r.Stats.COWWALReadAttempts != 2*blocks || r.Stats.COWSavedChunks != 1 || r.Stats.COWLocalReadNs <= 0 {
		t.Fatalf("local accounting: %+v", r.Stats)
	}
	want := bytes.Repeat([]byte{0x31}, blocks*ctBlockSize)
	for _, ref := range r.Head.Chunks {
		got, e := f.Get(context.Background(), ref.Key)
		if e != nil || !bytes.Equal(got, want) {
			t.Fatalf("WAL-resident at-H image: %v", e)
		}
	}
	t.Logf("wal_record_attempts=%d local_read_ns=%d guest_saver_wall_ns=%d", r.Stats.COWWALReadAttempts, r.Stats.COWLocalReadNs, elapsed.Nanoseconds())
}
