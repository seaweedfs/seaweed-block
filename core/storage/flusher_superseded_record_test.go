package storage

import (
	"bytes"
	"os"
	"testing"
)

// The flusher skips an LBA whose newest entry is not yet synced, so a
// cycle's checkpoint can pass an OLDER synced record of that LBA that never
// reached the extent (it was superseded in the dirty map by the unsynced
// one). That is safe only because the cycle's sync is the whole-file fsync
// of the one descriptor holding WAL and extent: it makes the newer record
// durable before the checkpoint is published, so recovery replays it
// (INV-WAL-FLUSH-SYNC-COVERS-WAL-001). b=X, c=Y, Sync, b=Z unsynced, one
// flusher cycle, crash after its checkpoint: b must read X or Z, never the
// bytes from before X.
//
// The crash image keeps the WAL region as of the last sync that covered it
// and everything else as of the crash. The product's syncs (the WAL sync and
// the flusher's) both cover the WAL. The control models a flusher sync that
// covers only the extent (a range sync, or WAL and extent in separate files)
// and shows the checkpoint bound would then lose X: the premise is
// load-bearing.
func TestFlusherCheckpointOverASupersededSyncedRecord(t *testing.T) {
	for _, c := range []struct {
		name            string
		flushCoversWAL  bool
		wantBeforeXRead bool
	}{
		{"whole-file flusher sync (product)", true, false},
		{"control: extent-only flusher sync", false, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, path, crash := crashableStore(t)
			d := s.v3.pub.current()
			walOff, walLen := int64(d.WALOffset), int64(d.WALSize)
			readWAL := func() []byte {
				b := make([]byte, walLen)
				if _, err := s.fd.ReadAt(b, walOff); err != nil {
					t.Fatal(err)
				}
				return b
			}
			durableWAL := readWAL()
			realWALSync, realFlushSync := s.syncWALFile, s.syncExtentFile
			s.syncWALFile = func() error {
				err := realWALSync()
				if err == nil {
					durableWAL = readWAL()
				}
				return err
			}
			s.syncExtentFile = func() error {
				err := realFlushSync()
				if err == nil && c.flushCoversWAL {
					durableWAL = readWAL()
				}
				return err
			}

			const b, cLBA = 3, 9
			X, Y, Z := bytes.Repeat([]byte{0x58}, ctBlockSize), bytes.Repeat([]byte{0x59}, ctBlockSize), bytes.Repeat([]byte{0x5a}, ctBlockSize)
			before, err := s.Read(b)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(b, X); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(cLBA, Y); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Sync(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(b, Z); err != nil { // unsynced: supersedes X in the dirty map
				t.Fatal(err)
			}
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if cp := s.CheckpointLSN(); cp < 2 {
				t.Fatalf("checkpoint %d after the cycle; want it past X's record (it covers Y)", cp)
			}
			crash()

			img, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			copy(img[walOff:walOff+walLen], durableWAL)
			if err := os.WriteFile(path, img, 0o644); err != nil {
				t.Fatal(err)
			}
			s2, _ := reopen(t, path)
			got, err := s2.Read(b)
			if err != nil {
				t.Fatal(err)
			}
			isBefore := bytes.Equal(got, before) && !bytes.Equal(got, X)
			switch {
			case c.wantBeforeXRead && !isBefore:
				t.Fatalf("control: block %d reads %#x after the crash; the model should lose X here (premise not shown load-bearing)", b, got[0])
			case !c.wantBeforeXRead && !bytes.Equal(got, X) && !bytes.Equal(got, Z):
				t.Fatalf("block %d reads %#x after the crash; want X (%#x) or Z (%#x), never the bytes from before X", b, got[0], X[0], Z[0])
			}
			t.Logf("block %d after the crash: %#x", b, got[0])
		})
	}
}
