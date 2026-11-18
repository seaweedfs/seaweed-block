package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// The forged-replay finding (docs/spec/README.md, open item 2) end to end on
// a mapped v3 WALStore (the production layout). A guest batch in lap 1 carries, in its payload, a
// well-formed v3 record bound to this volume's own nonce (LSN 999, LBA 7) at
// the cursor its bytes occupy in lap 1. The WAL then wraps: one newer record
// overwrites only the start of the batch, so the scan of lap 2 resumes
// exactly at the embedded record's first byte. The cursor binding (lap
// included) must stop the scan there: after a crash and reopen, LBA 7 is
// never the forged data and the recovered frontier is the newest real write,
// never 999.
func TestWALStoreV3StaleLapBytesAtTheWrapAreNotReplayed(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	s.SetRecoveryRetentionLSNs(0) // a checkpoint releases everything it covers
	d := s.v3.pub.current()
	rec := uint64(walEntryHeaderSizeV3 + v3TestBlock) // one single-block record
	if d.WALSize != v3TestWAL || d.RetainedHead != 0 || d.RetainedTail != 0 {
		t.Fatalf("fixture: WAL %d, head %d, tail %d", d.WALSize, d.RetainedHead, d.RetainedTail)
	}

	// Lap 1, cursor 0: the batch. Its payload follows the record prefix, so a
	// single record written at the wrap ends at cursor rec, inside it.
	forged, err := encodeWALEntryV3(walEntryV3{LSN: 999, Type: walEntryWrite, LBA: 7, Length: v3TestBlock,
		Data: blk(0xF0), LogicalStart: rec, Nonce: d.Nonce}, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 3*v3TestBlock)
	copy(payload, blk(0x11))
	copy(payload[rec-walEntryPrefixSizeV3:], forged)
	if _, err := s.WriteBatch(8, [][]byte{payload[:v3TestBlock], payload[v3TestBlock : 2*v3TestBlock], payload[2*v3TestBlock:]}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if off := recordOffset(t, path, 1); off != int64(d.WALOffset) {
		t.Fatalf("batch at file offset %d, want the WAL start %d", off, d.WALOffset)
	}

	// Fill lap 1 until the next record cannot fit before the lap end, then let
	// the flusher checkpoint everything so the tail releases the batch.
	batch := uint64(walEntryHeaderSizeV3 + 3*v3TestBlock)
	singles := (d.WALSize - batch) / rec // the last one still fits in lap 1
	writeSynced(t, s, 20, 20+uint32(singles)-1)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	head := batch + singles*rec
	if cur := s.v3.pub.current(); cur.RetainedTail != head || cur.CheckpointLSN != 3+singles {
		t.Fatalf("after the flusher: tail %d, checkpoint %d; want %d, %d", cur.RetainedTail, cur.CheckpointLSN, head, 3+singles)
	}

	// The wrap: one newer record lands at cursor WALSize (file offset 0 of the
	// WAL), over the batch's header and first block.
	newest := uint32(20 + singles)
	if _, err := s.Write(newest, blk(0x5A)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	lastLSN := 3 + singles + 1
	if off := recordOffset(t, path, lastLSN); off != int64(d.WALOffset) {
		t.Fatalf("wrapped record at file offset %d, want the WAL start %d", off, d.WALOffset)
	}

	// The scan reaches the forged bytes and stops on the binding there.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	r, err := scanWALV3(f, s.v3.pub.current())
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if r.End != d.WALSize+rec || !errors.Is(r.StopCause, errWALBindingV3) {
		t.Fatalf("scan end %d, stop %v; want %d and the binding refusal at the forged record", r.End, r.StopCause, d.WALSize+rec)
	}
	for _, x := range r.Records {
		if x.Entry.LSN == 999 {
			t.Fatalf("forged record admitted by the scan: %+v", x.Entry)
		}
	}

	crashRecoveryHardeningStore(t, s)
	o, err := openWALStoreV3ForTest(path)
	if err != nil {
		t.Fatal(err)
	}
	o.DisableAutoFlushForRecoveryTest()
	t.Cleanup(func() { crashRecoveryHardeningStore(t, o) })
	frontier, err := o.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if frontier != lastLSN {
		t.Fatalf("recovered frontier %d, want the newest real write %d (never 999)", frontier, lastLSN)
	}
	if got := readV3(t, o, 7); bytes.Equal(got, blk(0xF0)) || !bytes.Equal(got, make([]byte, v3TestBlock)) {
		t.Fatalf("LBA 7 reads %x..., want never written (the forged record replayed?)", got[:8])
	}
	if !bytes.Equal(readV3(t, o, newest), blk(0x5A)) || !bytes.Equal(readV3(t, o, 8), blk(0x11)) {
		t.Fatal("a real write was lost")
	}
}
