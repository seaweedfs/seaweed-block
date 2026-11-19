package storage

// The caller holds captureMu exclusive, with no layer state lock held.
// An owned-map max is conservative: recovery can drop an unacked suffix
// while retaining its maximum. Bound it by the same-barrier cut frontier;
// the captured image has no later writes. Preserve lower inherited births.
func (l *chunkLayerImpl) captureBirths(candidates map[uint64]bool, frontier uint64) map[uint64]uint64 {
	out := make(map[uint64]uint64, len(candidates))
	l.s.mu.RLock()
	defer l.s.mu.RUnlock()
	for idx := range candidates {
		birth := frontier
		if l.usesOwnedMapV3() {
			birth = min(l.s.v3.mapEntries[idx].LSN, frontier)
		}
		out[idx] = birth
	}
	return out
}
