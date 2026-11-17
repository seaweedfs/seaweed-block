package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// A real file with row-1 slots and row-3 publisher; no v2 constructor or
// legacy recovery scanner is used by this dark writer/scanner harness.
func newWALIOTestV3(t *testing.T, head, tail uint64) (*os.File, *descriptorPublisherV3, *walWriterV3) {
	t.Helper()
	d, err := decodeSlotV3(readSlotVector(t, "slot-ref"), nil)
	if err != nil {
		t.Fatal(err)
	}
	d.WALSize = 16 * 1024
	d.RetainedHead = head
	d.RetainedTail = tail
	d.W = 0
	d.CheckpointLSN = 0
	d.MapCoveredCheckpoint = 0
	d.BaseState = BaseNone
	d.BaseLSN = 0
	d.BaseCursor = 0
	d.BaseInstallID = [16]byte{}
	f, err := os.Create(filepath.Join(t.TempDir(), "store.v3"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if err := f.Truncate(int64(d.WALOffset + d.WALSize + d.VolumeSize)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt(d.encode(), slotAOffsetV3); err != nil {
		t.Fatal(err)
	}
	var leaf sync.Mutex
	pub := newDescriptorPublisherV3(d, slotA, nil, descriptorIOV3{
		writeSlot: func(off int64, b []byte) error {
			n, e := f.WriteAt(b, off)
			if e == nil && n != len(b) {
				return io.ErrShortWrite
			}
			return e
		},
		fsync: f.Sync, sticky: func() error { return nil },
		syncFile: func(write, sync func() error) error {
			leaf.Lock()
			defer leaf.Unlock()
			if err := write(); err != nil {
				return err
			}
			return sync()
		},
	})
	w, err := newWALWriterV3(f, pub)
	if err != nil {
		t.Fatal(err)
	}
	return f, pub, w
}

func TestWALIOV3_RealWriterReopenScan(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 0, 0)
	d := p.current()
	for i := uint64(1); i <= 3; i++ {
		e := walEntryV3{LSN: i, Type: walEntryWrite, LBA: i, Length: d.BlockSize, Data: bytes.Repeat([]byte{byte(i)}, int(d.BlockSize))}
		// Caller binding fields are overwritten, not trusted.
		e.Nonce = [16]byte{0xFF}
		e.LogicalStart = 999
		if _, err := w.append(e, i-1); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	// Close/reopen is real; slots still lag the appends, as in mode C.
	name := f.Name()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	rfd, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer rfd.Close()
	r, err := scanWALV3(rfd, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Records) != 3 || r.Frontier != 3 || r.HighestLSN != 3 || r.MaxDurableLSN != 2 || r.End != w.logicalHeadV3() {
		t.Fatalf("validated scan=%+v", r)
	}
	for i, record := range r.Records {
		e := record.Entry
		if e.Nonce != d.Nonce || e.LogicalStart != uint64(i)*(uint64(d.BlockSize)+70) || !bytes.Equal(e.Data, bytes.Repeat([]byte{byte(i + 1)}, int(d.BlockSize))) {
			t.Fatal("writer/scanner binding or payload mismatch")
		}
	}
}

func TestWALIOV3_WrapPaddingAndSlack(t *testing.T) {
	for _, remaining := range []uint64{32, 70, 130} {
		t.Run(map[uint64]string{32: "geometric slack", 70: "empty padding", 130: "payload padding"}[remaining], func(t *testing.T) {
			const size = 16 * 1024
			start := uint64(size) - remaining
			f, p, w := newWALIOTestV3(t, start, start)
			d := p.current()
			if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryWrite, LBA: 1, Length: d.BlockSize, Data: make([]byte, d.BlockSize)}, 0); err != nil {
				t.Fatal(err)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			r, err := scanWALV3(f, d)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Records) != 1 || r.Records[0].Entry.LogicalStart != size || r.Frontier != 1 || r.End != size+70+uint64(d.BlockSize) {
				t.Fatalf("wrapped scan=%+v", r)
			}
		})
	}
}

func TestWALIOV3_StopsAtBindingBeforeEmbeddedRecord(t *testing.T) {
	for _, kind := range []string{"foreign nonce", "wrong cursor same lap", "old lap", "bad CRC", "bad footer"} {
		t.Run(kind, func(t *testing.T) {
			f, p, _ := newWALIOTestV3(t, 0, 0)
			d := p.current()
			// A valid barrier is hidden after the first header. The scanner must not
			// search onward for it after failing the first record's admission.
			good, err := encodeWALEntryV3(walEntryV3{LSN: 1, Type: walEntryBarrier, Nonce: d.Nonce}, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
			if err != nil {
				t.Fatal(err)
			}
			b := bytes.Clone(good)
			switch kind {
			case "foreign nonce":
				b[46] ^= 1
			case "wrong cursor same lap":
				binary.LittleEndian.PutUint64(b[38:46], 70)
			case "old lap":
				binary.LittleEndian.PutUint64(b[38:46], d.WALSize)
			case "bad CRC":
				b[62] ^= 1
			case "bad footer":
				b[66] ^= 1
			}
			if kind != "bad CRC" && kind != "bad footer" {
				binary.LittleEndian.PutUint32(b[62:66], crc32.Checksum(b[:62], walEntryCRC32CV3))
			}
			hidden := bytes.Clone(good)
			binary.LittleEndian.PutUint64(hidden[38:46], 70)
			binary.LittleEndian.PutUint32(hidden[62:66], crc32.Checksum(hidden[:62], walEntryCRC32CV3))
			if _, err := f.WriteAt(append(b, hidden...), int64(d.WALOffset)); err != nil {
				t.Fatal(err)
			}
			r, err := scanWALV3(f, d)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Records) != 0 || r.End != 0 || r.MaxDurableLSN != 0 || r.StopCause == nil {
				t.Fatalf("untrusted/embedded record admitted: %+v", r)
			}
		})
	}
}

func TestWALIOV3_InstalledScanBeginsAtMaxTail(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 140, 140)
	d := p.current()
	d.BaseState = BaseInstalled
	d.BaseInstallID = [16]byte{1}
	d.BaseCursor = 70
	d.BaseLSN = 5
	d.CheckpointLSN = 5
	d.W = 5
	d.MapCoveredCheckpoint = 5
	if _, err := w.append(walEntryV3{LSN: 6, Type: walEntryBarrier}, 5); err != nil {
		t.Fatal(err)
	}
	r, err := scanWALV3(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if r.Start != 140 || r.End != 210 || r.Frontier != 6 || len(r.Records) != 1 {
		t.Fatalf("max-tail scan=%+v", r)
	}
}

func TestWALIOV3_ValidatedFrontierIntervals(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 0, 0)
	d := p.current()
	// A gap above BASE must remain a gap; no allocation proportional to LSN.
	d.BaseState = BaseInstalling
	d.BaseInstallID = [16]byte{1}
	d.BaseLSN = 100
	for _, lsn := range []uint64{102, 101, 104} {
		if _, err := w.append(walEntryV3{LSN: lsn, Type: walEntryBarrier}, 100); err != nil {
			t.Fatal(err)
		}
	}
	r, err := scanWALV3(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if r.Frontier != 102 || r.HighestLSN != 104 || r.MaxDurableLSN != 100 || len(r.Records) != 3 {
		t.Fatalf("interval scan=%+v", r)
	}
}

func TestWALIOV3_FloorAndPwriteFailureRefuse(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 16*1024, 0)
	_ = f
	before := w.logicalHeadV3()
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); !errors.Is(err, errWALFull) {
		t.Fatalf("full ring error=%v", err)
	}
	if w.logicalHeadV3() != before {
		t.Fatal("full append changed writer head")
	}
	if _, err := p.publish(nil, nil, func(d *descriptorV3) error { d.RetainedTail = 16 * 1024; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); !errors.Is(err, errWALFull) {
		t.Fatalf("fallback tail must still pin: %v", err)
	}
	if ok, err := p.rolloverIfPinned(); err != nil || !ok {
		t.Fatalf("rollover=%v %v", ok, err)
	}
	calls := 0
	w.writeAt = func(b []byte, off int64) (int, error) { calls++; return 1, io.ErrShortWrite }
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); !errors.Is(err, errWALPwrite) {
		t.Fatalf("append failure=%v", err)
	}
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); !errors.Is(err, errWALPwrite) || calls != 1 {
		t.Fatal("failed append retried physical I/O")
	}
}

func TestWALIOV3_EmbeddedRecordInPayloadIsNotScanned(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 0, 0)
	d := p.current()
	inner, err := encodeWALEntryV3(walEntryV3{LSN: 99, Type: walEntryBarrier, DurableLSN: 88, LogicalStart: 100, Nonce: d.Nonce}, d.BlockSize, d.VolumeSize/uint64(d.BlockSize))
	if err != nil {
		t.Fatal(err)
	}
	data := make([]byte, d.BlockSize)
	copy(data[100:], inner)
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryWrite, LBA: 4, Length: d.BlockSize, Data: data}, 0); err != nil {
		t.Fatal(err)
	}
	r, err := scanWALV3(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Records) != 1 || r.Frontier != 1 || r.HighestLSN != 1 || r.MaxDurableLSN != 0 || !bytes.Equal(r.Records[0].Entry.Data, data) {
		t.Fatalf("embedded payload interpreted as WAL: %+v", r)
	}
}

func TestWALIOV3_ConcurrentAppendsHaveUniqueCursors(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 0, 0)
	d := p.current()
	done := make(chan error, 40)
	for n := uint64(1); n <= 40; n++ {
		go func(lsn uint64) { _, err := w.append(walEntryV3{LSN: lsn, Type: walEntryBarrier}, 0); done <- err }(n)
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for n := 0; n < 40; n++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline.C:
			t.Fatal("bounded append completion timed out")
		}
	}
	r, err := scanWALV3(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Records) != 40 || r.Frontier != 40 || r.End != 40*70 {
		t.Fatalf("concurrent scan=%+v", r)
	}
}

func TestWALIOV3_CounterRefusalBeforePwrite(t *testing.T) {
	_, _, w := newWALIOTestV3(t, counterMaxV3-30, counterMaxV3-30)
	calls := 0
	w.writeAt = func(b []byte, off int64) (int, error) { calls++; return len(b), nil }
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); !errors.Is(err, ErrCounterExhausted) || calls != 0 {
		t.Fatalf("counter error=%v writes=%d", err, calls)
	}
}

func TestWALIOV3_ScanRequiresActualSourceBounds(t *testing.T) {
	f, p, _ := newWALIOTestV3(t, 0, 0)
	d := p.current()
	if err := f.Truncate(int64(d.WALOffset + walEntryPrefixSizeV3)); err != nil {
		t.Fatal(err)
	}
	if _, err := scanWALV3(f, d); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated source bounds error=%v", err)
	}
}

func TestWALIOV3_LastRepresentableRecordStillRecovers(t *testing.T) {
	start := uint64(counterMaxV3) - 70
	f, p, w := newWALIOTestV3(t, start, start)
	d := p.current()
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); err != nil {
		t.Fatal(err)
	}
	r, err := scanWALV3(f, d)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Records) != 1 || r.Frontier != 1 || r.End != counterMaxV3 {
		t.Fatalf("last representable scan=%+v", r)
	}
	if _, err := w.append(walEntryV3{LSN: 2, Type: walEntryBarrier}, 0); !errors.Is(err, ErrCounterExhausted) {
		t.Fatalf("next append=%v", err)
	}
}

func TestWALIOV3_RestoreCursorDoesNotPublishOrResetNonce(t *testing.T) {
	f, p, w := newWALIOTestV3(t, 0, 0)
	if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); err != nil {
		t.Fatal(err)
	}
	r, err := scanWALV3(f, p.current())
	if err != nil {
		t.Fatal(err)
	}
	before := p.current()
	reopened, err := newWALWriterV3(f, p)
	if err != nil {
		t.Fatal(err)
	}
	if err := reopened.restoreScanV3(r); err != nil {
		t.Fatal(err)
	}
	if reopened.logicalHeadV3() != r.End || p.current() != before {
		t.Fatal("cursor restore either lost E or published descriptor")
	}
	wrong := r
	wrong.Nonce[0] ^= 1
	if err := reopened.restoreScanV3(wrong); !errors.Is(err, errWALBindingV3) {
		t.Fatalf("wrong incarnation restore=%v", err)
	}
	wrong = r
	wrong.End = before.RetainedTail + before.WALSize + 1
	if err := reopened.restoreScanV3(wrong); !errors.Is(err, errWALBindingV3) {
		t.Fatalf("out-of-window restore=%v", err)
	}
	if reopened.logicalHeadV3() != r.End {
		t.Fatal("refused cursor restore changed E")
	}
	// No append is attempted here: fence/publication admission is row5/11.
}

// The amended row-3 publisher and the existing row-4 writer share one
// refusal boundary even when the caller has not yet wired a global latch.
func TestWALIOV3_UncertainPublisherStopsExistingAndNewWriters(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprintf("partial=%v", partial), func(t *testing.T) {
			f, p, w := newWALIOTestV3(t, 0, 0)
			boom := errors.New("slot response lost")
			p.io.writeSlot = func(off int64, b []byte) error {
				if partial {
					b = b[:len(b)/2]
				}
				if _, err := f.WriteAt(b, off); err != nil {
					return err
				}
				return boom
			}
			_, err := p.publish(nil, nil, func(d *descriptorV3) error { d.W = 1; return nil })
			if !errors.Is(err, ErrDescriptorPublishUncertain) {
				t.Fatalf("publish=%v", err)
			}
			writes := 0
			w.writeAt = func(b []byte, off int64) (int, error) { writes++; return f.WriteAt(b, off) }
			if _, err := w.append(walEntryV3{LSN: 1, Type: walEntryBarrier}, 0); err == nil {
				t.Fatal("existing writer accepted uncertain publication")
			}
			if writes != 0 {
				t.Fatalf("physical writes after uncertainty=%d", writes)
			}
			if _, err := newWALWriterV3(f, p); err == nil {
				t.Fatal("new writer accepted uncertain publication")
			}
		})
	}
}
