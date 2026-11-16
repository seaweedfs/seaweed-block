package smartwal

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func flipSlotCRC(t *testing.T, path string, lsn uint64) {
	t.Helper()
	_, records, err := InspectRecords(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		if r.LSN != lsn {
			continue
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		b := make([]byte, 1)
		if _, err := f.ReadAt(b, r.Offset+31); err != nil {
			t.Fatal(err)
		}
		b[0] ^= 0xFF
		if _, err := f.WriteAt(b, r.Offset+31); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Fatalf("no record LSN %d", lsn)
}

func warnsHole(s *Store) bool {
	for _, w := range s.RecoveryWarnings() {
		if strings.HasPrefix(w, storage.WarningWALHoleSuspected+":") {
			return true
		}
	}
	return false
}

// codex01's r1 P1, under claude05's 06:47Z ruling (detection only): a torn
// slot accepted by one recovery stays a recoverable warning after a
// frontier jump and a new synced write; nothing infers its generation from
// its position in the ring.
func TestSmartWALTornSlotSurvivesAFrontierJump(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.sw")
	s, err := CreateStore(path, 16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	for i, lba := range []uint32{0, 1} { // LSN 1, 2
		data := make([]byte, 4096)
		data[0] = byte(0xE0 + i)
		if _, err := s.Write(lba, data); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	flipSlotCRC(t, path, 2) // LSN 2 torn

	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recover(); err != nil {
		t.Fatalf("first recovery: %v; want the torn tail accepted", err)
	}
	firstWarned, firstHoles := warnsHole(s), s.HolesSuspected()
	s.AdvanceFrontier(4)
	data := make([]byte, 4096)
	data[0] = 0xEF
	if _, err := s.Write(2, data); err != nil { // LSN 5
		t.Fatal(err)
	}
	if _, err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Recover(); err != nil {
		t.Fatalf("second recovery after the frontier jump: %v; want recovered (the old torn slot is not corruption)", err)
	}
	if !firstWarned || firstHoles != 1 || !warnsHole(s) {
		t.Fatalf("warnings: first %t (%d), second %v; want the counted %s both times", firstWarned, firstHoles, s.RecoveryWarnings(), storage.WarningWALHoleSuspected)
	}
}
