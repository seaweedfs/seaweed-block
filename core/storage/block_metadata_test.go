package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type recordingMetadata struct {
	file                          BlockMetadataFile
	end                           int64
	writes, reads, barriers       int
	writeErr, readErr, barrierErr error
	before                        func(uint64)
}

func (h *recordingMetadata) OnExtentWrite(uint32, []byte) error { h.writes++; return h.writeErr }
func (h *recordingMetadata) VerifyExtent(uint32, []byte) error  { h.reads++; return h.readErr }
func (h *recordingMetadata) BeforeCheckpointAdvance(lsn uint64) error {
	h.barriers++
	if h.before != nil {
		h.before(lsn)
	}
	if h.barrierErr != nil {
		return h.barrierErr
	}
	if _, err := h.file.WriteAt([]byte{1}, h.end); err != nil {
		return err
	}
	return h.file.Sync()
}
func recordingOptions(h *recordingMetadata, version uint32) WALStoreOptions {
	return WALStoreOptions{MetadataFactory: func(f BlockMetadataFile, l BlockMetadataLayout) (BlockMetadataBinding, error) {
		h.file = f
		h.end = l.ExtentEnd
		return BlockMetadataBinding{ImplVersion: version, AppendedBytes: 1, Hook: h}, nil
	}}
}
func metadataStore(t *testing.T, h *recordingMetadata) (*WALStore, string) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "store")
	s, err := CreateWALStoreWithOptions(p, 16, 4096, recordingOptions(h, WALStoreImplVersion))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.flusher.Stop()
	return s, p
}
func TestBlockMetadata_DurableBeforeTail(t *testing.T) {
	h := &recordingMetadata{}
	s, _ := metadataStore(t, h)
	if _, err := s.Write(3, makeBlock(4096, 0x31)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	cp, tail := s.CheckpointLSN(), walLogicalTail(s)
	h.before = func(uint64) {
		if s.checkpointLSN != cp || s.wal.logicalTail != tail {
			t.Error("tail/checkpoint advanced before hook durability")
		}
	}
	if err := s.flusher.flushOnce(); err != nil {
		t.Fatal(err)
	}
	if h.writes != 1 || h.barriers != 1 || s.CheckpointLSN() <= cp || walLogicalTail(s) <= tail {
		t.Fatalf("hook not ordered: writes=%d barriers=%d checkpoint=%d", h.writes, h.barriers, s.CheckpointLSN())
	}
	b := []byte{0}
	if _, err := s.fd.ReadAt(b, h.end); err != nil || b[0] != 1 {
		t.Fatalf("durable marker absent: %v %v", b, err)
	}
}
func TestBlockMetadata_BarrierErrorBlocksCheckpointAndDirectFrontier(t *testing.T) {
	for _, direct := range []bool{false, true} {
		t.Run(map[bool]string{false: "WAL", true: "BASE"}[direct], func(t *testing.T) {
			sentinel := errors.New("metadata persistence refused")
			h := &recordingMetadata{barrierErr: sentinel}
			s, _ := metadataStore(t, h)
			cp, tail := s.CheckpointLSN(), walLogicalTail(s)
			if direct {
				if err := s.WriteExtentDirect(3, makeBlock(4096, 0x32)); err != nil {
					t.Fatal(err)
				}
				s.AdvanceFrontier(7)
			} else {
				if _, err := s.Write(3, makeBlock(4096, 0x32)); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			if direct {
				_, err = s.Sync()
			} else {
				err = s.flusher.flushOnce()
			}
			if !errors.Is(err, sentinel) || s.CheckpointLSN() != cp || walLogicalTail(s) != tail {
				t.Fatalf("hook error recycled: err=%v checkpoint=%d tail=%d", err, s.CheckpointLSN(), walLogicalTail(s))
			}
		})
	}
}
func TestBlockMetadata_WriteAndVerifyRouting(t *testing.T) {
	h := &recordingMetadata{}
	s, p := metadataStore(t, h)
	sentinel := errors.New("metadata refusal")
	h.writeErr = sentinel
	if err := s.WriteExtentDirect(3, makeBlock(4096, 0x33)); !errors.Is(err, sentinel) {
		t.Fatalf("write hook bypassed: %v", err)
	}
	h.writeErr = nil
	if err := s.WriteExtentDirect(3, makeBlock(4096, 0x33)); err != nil {
		t.Fatal(err)
	}
	h.readErr = sentinel
	if got, err := s.Read(3); !errors.Is(err, sentinel) || len(got) != 0 {
		t.Fatalf("verify bypassed: %d %v", len(got), err)
	}
	h.readErr = nil
	if got, err := s.Read(3); err != nil || !bytes.Equal(got, makeBlock(4096, 0x33)) {
		t.Fatalf("positive read: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rHook := &recordingMetadata{readErr: sentinel}
	r, err := OpenReadOnlyWithOptions(p, recordingOptions(rHook, WALStoreImplVersion))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if got, err := r.Read(3); !errors.Is(err, sentinel) || len(got) != 0 || rHook.reads != 1 {
		t.Fatalf("readonly verify bypassed: %d %v calls=%d", len(got), err, rHook.reads)
	}
}
func TestBlockMetadata_UnsupportedFormatRequiresFactory(t *testing.T) {
	p := filepath.Join(t.TempDir(), "store")
	h := &recordingMetadata{}
	s, err := CreateWALStoreWithOptions(p, 16, 4096, recordingOptions(h, 77))
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if x, err := OpenWALStore(p); err == nil {
		x.Close()
		t.Fatal("unknown format silently opened without hook")
	}
	if x, err := OpenReadOnly(p); err == nil {
		x.Close()
		t.Fatal("unknown readonly format silently opened without hook")
	}
	after, _ := os.ReadFile(p)
	if !bytes.Equal(before, after) {
		t.Fatal("format refusal mutated file")
	}
	x, err := OpenWALStoreWithOptions(p, recordingOptions(&recordingMetadata{}, 77))
	if err != nil {
		t.Fatal(err)
	}
	x.Close()
	if x, err := OpenWALStoreWithOptions(p, recordingOptions(&recordingMetadata{}, 78)); err == nil {
		x.Close()
		t.Fatal("factory accepted a different stored version")
	}
}

func TestBlockMetadata_SeedFailureRetry(t *testing.T) {
	sentinel := errors.New("deferred metadata not durable")
	hook := &recordingMetadata{barrierErr: sentinel}
	s, path := metadataStore(t, hook)
	if err := s.WriteExtentDirect(3, makeBlock(4096, 0x45)); err != nil {
		t.Fatal(err)
	}
	persist := s.syncDirectFrontierMetadata
	s.syncDirectFrontierMetadata = func(data []byte) error {
		if hook.barriers != 2 {
			t.Errorf("seed persistence before retry barrier: %d", hook.barriers)
		}
		marker := []byte{0}
		if _, err := s.fd.ReadAt(marker, hook.end); err != nil || marker[0] != 1 {
			t.Errorf("seed persisted before deferred marker: %v %v", marker, err)
		}
		return persist(data)
	}
	if err := s.SeedChunkFrontier(7); !errors.Is(err, sentinel) {
		t.Fatalf("seed bypassed deferred metadata error: %v", err)
	}
	if s.NextLSN() != 1 || s.CheckpointLSN() != 0 {
		t.Fatal("failed seed published next/checkpoint")
	}
	if r, _, h := s.Boundaries(); r != 0 || h != 0 {
		t.Fatalf("failed seed published boundaries: %d %d", r, h)
	}
	header := make([]byte, 4096)
	if _, err := s.fd.ReadAt(header, 0); err != nil {
		t.Fatal(err)
	}
	if binary.LittleEndian.Uint64(header[72:80]) != 0 {
		t.Fatal("failed seed persisted checkpoint")
	}
	hook.barrierErr = nil
	if err := s.SeedChunkFrontier(7); err != nil {
		t.Fatal(err)
	}
	if hook.barriers != 2 || s.NextLSN() != 8 || s.CheckpointLSN() != 7 {
		t.Fatalf("retry failed: calls=%d next=%d cp=%d", hook.barriers, s.NextLSN(), s.CheckpointLSN())
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenWALStoreWithOptions(path, recordingOptions(&recordingMetadata{}, WALStoreImplVersion))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.flusher.Stop()
	if _, err = reopened.Recover(); err != nil {
		t.Fatal(err)
	}
	if reopened.NextLSN() != 8 {
		t.Fatal("seed retry not durable after reopen")
	}
	if data, err := reopened.Read(3); err != nil || !bytes.Equal(data, makeBlock(4096, 0x45)) {
		t.Fatalf("seed data changed: %v", err)
	}
}
