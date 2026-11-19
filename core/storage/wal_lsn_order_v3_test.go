package storage

import (
	"bytes"
	"errors"
	"testing"
)

func TestWALLSNOrderV3_RefusesDuplicateAndLower(t *testing.T) {
	for _, lsn := range []uint64{9, 10} {
		t.Run(map[uint64]string{9: "lower", 10: "equal"}[lsn], func(t *testing.T) {
			f, p, w := newWALIOTestV3(t, 0, 0)
			if _, e := w.append(walEntryV3{LSN: 10, Type: walEntryBarrier}, 0, nil); e != nil {
				t.Fatal(e)
			}
			before := make([]byte, p.current().WALSize)
			if _, e := f.ReadAt(before, int64(p.current().WALOffset)); e != nil {
				t.Fatal(e)
			}
			head := w.logicalHeadV3()
			if _, e := w.append(walEntryV3{LSN: lsn, Type: walEntryBarrier}, 0, nil); !errors.Is(e, ErrWALLSNOrder) {
				t.Fatalf("non-increasing LSN %d verdict after10: %v", lsn, e)
			}
			after := make([]byte, len(before))
			if _, e := f.ReadAt(after, int64(p.current().WALOffset)); e != nil {
				t.Fatal(e)
			}
			if w.logicalHeadV3() != head || !bytes.Equal(before, after) {
				t.Fatal("refused LSN changed cursor or WAL bytes")
			}
		})
	}
}
func TestWALLSNOrderV3_BatchConsumesFullRange(t *testing.T) {
	_, p, w := newWALIOTestV3(t, 0, 0)
	bs := p.current().BlockSize
	if _, e := w.append(walEntryV3{LSN: 10, BatchCount: 2, Type: walEntryWriteBatch, Length: 2 * bs, Data: make([]byte, 2*bs)}, 0, nil); e != nil {
		t.Fatal(e)
	}
	if _, e := w.append(walEntryV3{LSN: 11, Type: walEntryBarrier}, 0, nil); !errors.Is(e, ErrWALLSNOrder) {
		t.Fatalf("overlap with batch last LSN: %v", e)
	}
	if _, e := w.append(walEntryV3{LSN: 12, Type: walEntryBarrier}, 0, nil); e != nil {
		t.Fatal(e)
	}
}
func TestWALLSNOrderV3_RestoreUsesKeptNotDroppedFrontier(t *testing.T) {
	f, p, _ := newWALIOTestV3(t, 0, 0)
	d := p.current()
	d.CheckpointLSN = 100
	d.MapCoveredCheckpoint = 100
	d.W = 100
	p.cur = d
	w, e := newWALWriterV3(f, p)
	if e != nil {
		t.Fatal(e)
	}
	for _, lsn := range []uint64{101, 104} {
		if _, e = w.append(walEntryV3{LSN: lsn, Type: walEntryBarrier}, 100, nil); e != nil {
			t.Fatal(e)
		}
	}
	r, e := scanWALV3(f, d)
	if e != nil {
		t.Fatal(e)
	}
	if r.Frontier != 101 || r.HighestLSN != 104 {
		t.Fatalf("setup scan=%+v", r)
	}
	// The owner discards unacked104 and durably fences the exact remaining lap.
	s := &WALStore{fd: f}
	if e = s.fenceV3(70, 0, d.WALSize, true, d.WALOffset); e != nil {
		t.Fatal(e)
	}
	r.End = 70
	recovered, e := newWALWriterV3(f, p)
	if e != nil {
		t.Fatal(e)
	}
	if e = recovered.restoreScanV3(r); e != nil {
		t.Fatal(e)
	}
	if _, e = recovered.append(walEntryV3{LSN: 101, Type: walEntryBarrier}, 100, nil); !errors.Is(e, ErrWALLSNOrder) {
		t.Fatalf("restored kept frontier duplicate: %v", e)
	}
	if _, e = recovered.append(walEntryV3{LSN: 102, Type: walEntryBarrier}, 100, nil); e != nil {
		t.Fatalf("dropped104 pinned next102: %v", e)
	}
}

func TestWALLSNOrderV3_DescriptorFloorIsAlreadyConsumed(t *testing.T) {
	f, p, _ := newWALIOTestV3(t, 0, 0)
	d := p.current()
	d.CheckpointLSN = 100
	d.MapCoveredCheckpoint = 100
	d.W = 100
	p.cur = d
	w, e := newWALWriterV3(f, p)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = w.append(walEntryV3{LSN: 100, Type: walEntryBarrier}, 100, nil); !errors.Is(e, ErrWALLSNOrder) {
		t.Fatalf("descriptor floor100 not consumed: %v", e)
	}
	if _, e = w.append(walEntryV3{LSN: 101, Type: walEntryBarrier}, 100, nil); e != nil {
		t.Fatal(e)
	}
}
