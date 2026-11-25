package authority

import "sync"

// fakeBasisReader lets unit tests inject an AuthorityBasis result
// without spinning a real Publisher.
type fakeBasisReader struct {
	mu   sync.Mutex
	data map[string]AuthorityBasis
}

func newFakeBasisReader() *fakeBasisReader {
	return &fakeBasisReader{data: make(map[string]AuthorityBasis)}
}

func (f *fakeBasisReader) LastAuthorityBasis(volumeID, replicaID string) (AuthorityBasis, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.data[volumeID+"/"+replicaID]
	return b, ok
}

// VolumeAuthorityLine satisfies the new AuthorityBasisReader
// method. Iterates stored entries whose key starts with the
// VolumeID prefix and returns the highest-Epoch entry.
func (f *fakeBasisReader) VolumeAuthorityLine(volumeID string) (AuthorityBasis, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	prefix := volumeID + "/"
	var best AuthorityBasis
	any := false
	for k, b := range f.data {
		if len(k) < len(prefix) || k[:len(prefix)] != prefix {
			continue
		}
		if !any || b.Epoch > best.Epoch {
			best = b
			any = true
		}
	}
	return best, any
}

func (f *fakeBasisReader) Set(volumeID, replicaID string, b AuthorityBasis) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[volumeID+"/"+replicaID] = b
}

func (f *fakeBasisReader) Clear(volumeID, replicaID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.data, volumeID+"/"+replicaID)
}

// acceptableSlot builds an SlotObservation whose acceptable()
// predicate returns true — all four fields correct and not
// withdrawn. Tests then knock specific fields down to make it
// unacceptable.
