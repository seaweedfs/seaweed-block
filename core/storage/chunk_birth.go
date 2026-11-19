package storage

// The caller holds captureMu exclusive, with no layer state lock held.
// An owned-map max is the newest local write in that chunk; base fills do
// not advance it. Other engines use the cut frontier as an upper bound.
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
