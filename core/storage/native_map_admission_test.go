package storage

import (
	"errors"
	"testing"
)

func TestChunkAttachRefusesAnUnmappedNativeStore(t *testing.T) {
	s, _ := newStoreV3(t, WModeStrict)
	t.Cleanup(func() { _ = s.Close() })
	f := newFakeChunkRemote()
	l, err := attachChunkLayerImpl(s, f, ctConfig(1))
	if l != nil {
		defer l.Close()
	}
	if !errors.Is(err, ErrMapInvalid) {
		t.Fatalf("unmapped native store fell back to the retired bitmap engine: %v", err)
	}
	st := f.stats()
	if st.puts != 0 || st.gets != 0 || st.getHeads != 0 || st.cas != 0 {
		t.Fatal("map admission contacted remote before refusing")
	}
}
