package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage/smartwal"
)

func TestSmartWALCorruptLatestRecordWritesEvidence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "store.sw")
	store, err := smartwal.CreateStoreWithSlots(path, 8, 4096, 8)
	if err != nil {
		t.Fatalf("CreateStoreWithSlots: %v", err)
	}
	data := make([]byte, 4096)
	data[0] = 0xab
	if _, err := store.Write(2, data); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := store.Sync(); err != nil {
		t.Fatalf("Sync: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	outDir := filepath.Join(dir, "out")
	var stdout, stderr bytes.Buffer
	code := run([]string{"smartwal-corrupt-latest-record", "--path", path, "--out", outDir}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("run code=%d stderr=%s", code, stderr.String())
	}
	evidencePath := filepath.Join(outDir, "smartwal-corruption-evidence.txt")
	raw, err := os.ReadFile(evidencePath)
	if err != nil {
		t.Fatalf("ReadFile evidence: %v", err)
	}
	evidence := string(raw)
	for _, want := range []string{
		"smartwal_corruption_status=ok",
		"target_offset_inside_wal=true",
		"target_offset_inside_extent=false",
		"mutation=flip_last_record_crc_byte",
	} {
		if !strings.Contains(evidence, want) {
			t.Fatalf("evidence missing %q:\n%s", want, evidence)
		}
	}
	if stdout.String() != evidence {
		t.Fatalf("stdout should mirror evidence file")
	}

	_, records, err := smartwal.InspectRecords(path)
	if err != nil {
		t.Fatalf("InspectRecords after corruption: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("corrupted latest record should no longer decode, got %d valid records", len(records))
	}
}
