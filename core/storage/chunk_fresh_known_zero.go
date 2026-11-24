package storage

// RULE-FRESH-KNOWN-ZERO (INV-CUT-BITMAP-001). A fresh store (FreshFromHead
// still true at its attach: the .chunkfresh marker is present and no BASE
// is installed) has never had a write or a fill reach it: frontend I/O is
// admitted only after the attach commit retires the marker, a chunk volume
// takes no replication apply, no cut or hydration can resolve the layer
// before that commit, and no other implementation opens a root that carries
// chunk metadata. Its extent therefore holds the creation bytes (zeros) in
// every chunk, and a chunk the claimed HEAD does not name is zero there too:
// the two are equal, so the chunk need not be a cut candidate.
//
// freshKnownZeroCandidates samples, under the attach's capture barrier, the
// mutation token of every chunk the claimed HEAD does not name whose owned
// map entry is still its creation value {LSN 0, Unknown}; the entry test is
// an extra exclusion, not the premise. attachChunkLayer then clears only the
// UNKNOWN bit of those whose token is unchanged, through the cut's own
// persistCapturedMapV3, before the provider's attach commit. A failed map
// publication leaves them UNKNOWN on disk (conservative).
func (l *chunkLayerImpl) freshKnownZeroCandidates(head chunkHead) map[uint64]uint64 {
	s := l.s
	if s.v3 == nil || s.v3.ownedMap == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := map[uint64]uint64{}
	for idx := uint64(0); idx < l.numChunks && idx < uint64(len(s.v3.mapEntries)); idx++ {
		if _, named := head.Chunks[idx]; named {
			continue
		}
		if e := s.v3.mapEntries[idx]; e.LSN != 0 || !e.Unknown {
			continue
		}
		out[idx] = l.chunkVersion(idx)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
