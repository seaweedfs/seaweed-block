package storage

// mapProof is an immutable discriminator for this open store. ownedMap may
// be cleared by Close, so callers such as Stats must not use that cleanup
// pointer to decide which metadata format this layer implements.
func (l *chunkLayerImpl) usesOwnedMapV3() bool { return l.s.v3 != nil && l.s.v3.mapProof != nil }

// persistCapturedMapV3 is called only after HEAD's publication is definite
// (or an authority-checked no-op), outside bmMu/stMu and the capture barrier.
// The capture's mutation versions were sampled under the quiesced barrier.
// Store.mu prevents a WAL append from slipping between the version check and
// the UNKNOWN update. A later WAL append updates max LSN and stays retained;
// an effective UNKNOWN change or lifecycle transition invalidates proofs.
// A failed map publication leaves the old disk UNKNOWN conservative; a map
// persistence failure also latches the store, while the remote HEAD remains
// the successfully captured image. No remote durability claim is undone.
func (l *chunkLayerImpl) persistCapturedMapV3(versions map[uint64]uint64) {
	s := l.s
	v := s.v3
	if v == nil || v.ownedMap == nil {
		return
	}
	s.mu.Lock()
	if s.stickyIOFault() != nil {
		s.mu.Unlock()
		return
	}
	var clear []uint64
	for idx, version := range versions {
		if idx < uint64(len(v.mapEntries)) && v.mapEntries[idx].Unknown && l.chunkVersion(idx) == version {
			clear = append(clear, idx)
		}
	}
	if len(clear) > 0 {
		if err := v.mapProof.mutatedV3(); err != nil {
			s.mu.Unlock()
			return
		}
		for _, idx := range clear {
			v.mapEntries[idx].Unknown = false
		}
	}
	v.pub.descMu.Lock()
	cp := v.pub.cur.CheckpointLSN
	need := false
	for i, e := range v.mapEntries {
		if e != v.pub.mapCurrent.entries[i] {
			need = true
			break
		}
	}
	v.pub.descMu.Unlock()
	s.mu.Unlock()
	if !need {
		return
	}
	prepared, err := s.prepareOwnedMapCheckpointV3(cp)
	if err != nil {
		return
	}
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	// The ordinary merged-coverage and proof checks handle a concurrent
	// checkpoint/Recover/append. Failure retires no root or page. A later cut
	// retries a cache/disk difference even if UNKNOWN was already cleared in
	// memory by this verified publication.
	_, _ = v.pub.publishMapV3(&s.mu, nil, prepared, func(n *descriptorV3) error {
		n.W = max(n.W, v.wack.Load())
		return nil
	})
}
