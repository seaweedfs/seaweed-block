package storage

import "log"

// RULE-FRESH-KNOWN-ZERO (INV-CUT-FRESH-KNOWN-ZERO-001). A store this process
// start created (CreatedThisStart, in memory) that is still fresh at its
// attach (FreshFromHead: the .chunkfresh marker is present and no BASE is
// installed) has never had a write or a fill reach it: frontend I/O is
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
// an extra exclusion, not the premise. ApplyFreshKnownZero then clears only
// the UNKNOWN bit of those whose token is unchanged, through the cut's own
// persistCapturedMapV3. The order is: the attach commit retires the marker,
// THEN the marking is made durable, THEN the volume serves. A crash between
// the first two leaves no marker and every chunk UNKNOWN (one scan); a
// failed map publication leaves them UNKNOWN on disk (conservative).
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

// ApplyFreshKnownZero makes the fresh attach's certification durable: called
// once, by the provider, after the attach commit retired the fresh marker and
// before the volume serves, outside the capture barrier (as for a cut).
// It counts the chunks it certified in Stats.KnownZeroChunks and logs the
// outcome: a failed map publication or a raced write leaves chunks UNKNOWN,
// and the first cut then scans them (the log says why).
func (l *chunkLayerImpl) ApplyFreshKnownZero() {
	tokens := l.freshKnownZero
	if tokens == nil {
		return
	}
	l.freshKnownZero = nil
	l.persistCapturedMapV3(tokens)
	s := l.s
	s.mu.RLock()
	certified := 0
	for idx := range tokens {
		if idx < uint64(len(s.v3.mapEntries)) && !s.v3.mapEntries[idx].Unknown {
			certified++
		}
	}
	s.mu.RUnlock()
	l.stMu.Lock()
	l.stats.KnownZeroChunks = uint64(certified)
	l.stMu.Unlock()
	if certified == len(tokens) {
		log.Printf("storage: fresh attach: %d chunks HEAD does not name start known-zero", certified)
	} else {
		log.Printf("storage: WARNING: fresh attach: %d of %d chunks HEAD does not name start known-zero; the rest stay unknown (map publication failed or a write raced) and the first cut scans them", certified, len(tokens))
	}
}
