package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

func TestWALStoreV3_ReadOnlyMergesWALAndExtentWithoutWrites(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(5, blk(0x55)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(5, blk(0x77)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mapBefore, err := os.ReadFile(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	reader, err := openReadOnlyV3ForTest(path, WALStoreOptions{})
	if err != nil {
		t.Fatalf("actual readonly v3 open refused: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	for lba, want := range map[uint32][]byte{0: blk(0), 1: blk(1), 5: blk(0x77)} {
		got, e := reader.Read(lba)
		if e != nil || !bytes.Equal(got, want) {
			t.Fatalf("readonly LBA%d err=%v equal=%v", lba, e, bytes.Equal(got, want))
		}
	}
	if _, err = reader.fd.WriteAt([]byte{0}, 0); err == nil {
		t.Fatal("readonly descriptor allowed writes")
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mapAfter, err := os.ReadFile(path + ".chunkmap")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) || !bytes.Equal(mapBefore, mapAfter) {
		t.Fatal("readonly open/read/close mutated store or map")
	}
}

func crashReadOnlyStoreV3(t *testing.T, s *WALStore) {
	t.Helper()
	crashRecoveryHardeningStore(t, s)
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}
func TestWALStoreV3_ReadOnlyRefusesIncompleteOrCorruptDependencies(t *testing.T) {
	for _, fault := range []string{"installing", "acknowledged_wal", "missing_map", "fallback_page", "presence", "fresh"} {
		t.Run(fault, func(t *testing.T) {
			s, path := newMappedCheckpointStoreV3(t)
			oldPage := s.v3.pub.mapCurrent.refs[0].Offset
			writeSynced(t, s, 1, 1)
			if err := s.flusher.flushOnce(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Write(5, blk(5)); err != nil {
				t.Fatal(err)
			}
			// Keep the pre-checkpoint root in the valid fallback slot. Another
			// strict Sync would rotate that slot to the current map root.
			if fault != "fallback_page" {
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			walOff, _, _, _, _ := s.dm.get(5)
			if fault == "installing" {
				beginV3(t, s, 2, idA)
			}
			crashReadOnlyStoreV3(t, s)
			switch fault {
			case "acknowledged_wal":
				f, e := os.OpenFile(path, os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				_, e = f.WriteAt([]byte{0xff}, int64(walOffsetV3+walOff+walEntryPrefixSizeV3))
				ce := f.Close()
				if e != nil || ce != nil {
					t.Fatal(e, ce)
				}
			case "missing_map":
				if e := os.Rename(path+".chunkmap", path+".saved-map"); e != nil {
					t.Fatal(e)
				}
			case "fallback_page":
				f, e := os.OpenFile(path+".chunkmap", os.O_RDWR, 0)
				if e != nil {
					t.Fatal(e)
				}
				_, e = f.WriteAt([]byte{0xff}, int64(oldPage)+64)
				ce := f.Close()
				if e != nil || ce != nil {
					t.Fatal(e, ce)
				}
			case "presence":
				if e := os.WriteFile(path+".chunkpresence", []byte("absent nope\n"), 0600); e != nil {
					t.Fatal(e)
				}
			case "fresh":
				if e := os.WriteFile(path+".chunkfresh", []byte("fresh\n"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			before, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			reader, e := openReadOnlyV3ForTest(path, WALStoreOptions{})
			if reader != nil {
				_ = reader.Close()
				t.Error("refused readonly returned a reader")
			}
			if e == nil {
				t.Fatal("readonly accepted unsafe " + fault)
			}
			if fault == "installing" && !errors.Is(e, ErrBaseIncomplete) {
				t.Errorf("installing error=%v", e)
			}
			if fault == "acknowledged_wal" && !errors.Is(e, ErrWALIntegrityFault) {
				t.Errorf("corrupt ACK error=%v", e)
			}
			if fault == "fallback_page" && !errors.Is(e, ErrMapInvalid) {
				t.Errorf("fallback map error=%v", e)
			}
			after, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(before, after) {
				t.Fatal("failed readonly mutated store", e)
			}
		})
	}
}
func TestWALStoreV3_ReadOnlyNeverReturnsUnfilledExtentZeros(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 5, 5)
	crashReadOnlyStoreV3(t, s)
	if e := os.WriteFile(path+".chunkpresence", []byte("absent 1\nlocal 4\n"), 0600); e != nil {
		t.Fatal(e)
	}
	r, e := openReadOnlyV3ForTest(path, WALStoreOptions{})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = r.Close() })
	if got, e := r.Read(5); e != nil || !bytes.Equal(got, blk(5)) {
		t.Fatal("trusted WAL winner not readable", e)
	}
	if data, e := r.Read(4); !errors.Is(e, ErrReadOnlyNotResident) || data != nil {
		t.Fatalf("unfilled extent returned bytes=%d error=%v", len(data), e)
	}
}
func TestWALStoreV3_ReadOnlyRevalidatesWALAndBindsReadonlyMetadata(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	writeSynced(t, s, 1, 1)
	if e := s.flusher.flushOnce(); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Write(5, blk(5)); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Sync(); e != nil {
		t.Fatal(e)
	}
	walOff, _, _, _, _ := s.dm.get(5)
	crashReadOnlyStoreV3(t, s)
	h := &recordingMetadata{}
	opts := WALStoreOptions{MetadataFactory: func(f BlockMetadataFile, l BlockMetadataLayout) (BlockMetadataBinding, error) {
		if !l.ReadOnly || l.Creating || l.ImplVersion != 3 {
			t.Errorf("wrong readonly metadata layout: %+v", l)
		}
		h.file = f
		return BlockMetadataBinding{ImplVersion: 3, Hook: h}, nil
	}}
	r, e := openReadOnlyV3ForTest(path, opts)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = r.Close() })
	if _, e = r.Read(1); e != nil || h.reads != 1 || h.writes != 0 || h.barriers != 0 {
		t.Fatalf("readonly metadata reads/writes/barriers=%d/%d/%d err=%v", h.reads, h.writes, h.barriers, e)
	}
	if _, e = h.file.WriteAt([]byte{0}, 0); e == nil {
		t.Fatal("metadata factory obtained writable fd")
	}
	f, e := os.OpenFile(path, os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.WriteAt([]byte{0xff}, int64(walOffsetV3+walOff+walEntryPrefixSizeV3))
	ce := f.Close()
	if e != nil || ce != nil {
		t.Fatal(e, ce)
	}
	if data, e := r.Read(5); e == nil || data != nil {
		t.Fatalf("changed WAL returned bytes=%d err=%v", len(data), e)
	} else {
		var failure *RecoveryFailure
		if !errors.As(e, &failure) || failure.Kind != StorageRecoveryFailureWALIntegrity {
			t.Fatalf("wrong integrity classification: %v", e)
		}
	}
}
