package storage

// chunkRecency is a copy-order hint for the COW window: each candidate
// chunk's newest local write, bounded by the cut frontier. It is local,
// never persisted, never compared with HEAD or the store; correctness never
// depends on it. The caller holds captureMu exclusive, with no layer state
// lock held.
func (l *chunkLayerImpl) chunkRecency(candidates map[uint64]bool, frontier uint64) map[uint64]uint64 {
	out := make(map[uint64]uint64, len(candidates))
	l.s.mu.RLock()
	defer l.s.mu.RUnlock()
	for idx := range candidates {
		out[idx] = min(l.s.v3.mapEntries[idx].LSN, frontier)
	}
	return out
}
