package storage

import (
	"bytes"
	"errors"
	"os"
	"testing"
)

// Until row9 installs the v3 seed transaction, the legacy seed API must
// refuse rather than overwrite a v3 descriptor using the v2 serializer.
func TestWALStoreV3_SeedNeverUsesLegacyDescriptorWriter(t *testing.T) {
	s, path := newMappedCheckpointStoreV3(t)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	d := s.v3.pub.current()
	err = s.SeedChunkFrontier(7)
	if !errors.Is(err, errV3NotYet) {
		t.Errorf("unwired v3 seed must refuse: %v", err)
	}
	after, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(before, after) {
		t.Error("legacy seed rewrote v3 file bytes")
	}
	if s.v3.pub.current() != d || s.NextLSN() != 1 {
		t.Error("refused seed published local progress")
	}
}
