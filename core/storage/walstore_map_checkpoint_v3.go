package storage

// Actual owned-map checkpoint path, still dark until the single activation.
// capture/lifecycle admission remains with the existing caller; preparation
// performs no I/O under store/descriptor/leader locks. Publication uses the
// existing syncMu -> store.mu -> descMu -> fileSyncMu graph.

// Caller holds WALStore.mu. Higher-LSN writes update the max; a reused or
// lower/equal effective LSN is conservative UNKNOWN, independent of the max.
// Capture is the only future owner allowed to clear UNKNOWN after a version
// check. Keeping it set here cannot manufacture a remotely clean chunk.
func (s *WALStore) recordMapWriteV3(lba, lsn uint64) {
	v := s.v3
	if v.ownedMap == nil {
		return
	}
	e := &v.mapEntries[lba/v.ownedMap.chunkBlocks]
	if lsn <= e.LSN {
		e.Unknown = true
	}
	e.LSN = max(e.LSN, lsn)
}

func (s *WALStore) prepareOwnedMapCheckpointV3(cp uint64) (*preparedMapPublicationV3, error) {
	v := s.v3
	s.mu.RLock()
	if err := s.stickyIOFault(); err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	v.pub.descMu.Lock()
	prior, domain := v.pub.mapCurrent, v.pub.mapDomain
	if prior == nil || prior.owner != v.ownedMap || domain != v.mapProof {
		v.pub.descMu.Unlock()
		s.mu.RUnlock()
		return nil, ErrMapCoverageStale
	}
	proof := domain.snapshotV3()
	entries := append([]chunkLSNEntryV3(nil), v.mapEntries...)
	v.pub.descMu.Unlock()
	s.mu.RUnlock()
	c, err := v.ownedMap.prepareNextV3(cp, entries, prior)
	if err != nil {
		return nil, err
	}
	if !proof.validV3(domain) {
		return nil, ErrMapCoverageStale
	}
	return &preparedMapPublicationV3{candidate: c, proof: proof}, nil
}

func (s *WALStore) persistOwnedMapCheckpointV3(highestLSN, recoveryGen uint64) error {
	v := s.v3
	if err := s.stickyIOFault(); err != nil {
		return err
	}
	cp := min(highestLSN, v.wack.Load())
	s.mu.RLock()
	floorSource := s.recycleFloorSrc
	s.mu.RUnlock()
	if floorSource != nil {
		if floor, active := floorSource.MinPinAcrossActiveSessions(); active {
			cp = min(cp, floor)
		}
	}
	if cp <= v.pub.current().CheckpointLSN {
		return nil
	}
	// The extent was fsynced by the flusher.
	d, err := s.publishCheckpointV3(cp, false, recoveryGen)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.checkpointLSN = d.CheckpointLSN
	s.mu.Unlock()
	return nil
}

// publishCheckpointV3 is the one v3 checkpoint publication (plan row 9: the
// flusher's checkpoint and SeedChunkFrontier both use it). The optional
// metadata and chunk callbacks settle first, then the owned-map candidate
// is persisted outside every lock, then the sole slot publisher runs under
// the leader role (syncMu), refusing a candidate from another recovery
// lifetime or one above the acknowledged W. seed raises the acknowledged W
// to cp in the same leader section (the inherited remote frontier). The
// caller has made the extent durable. Nothing is published on any error.
func (s *WALStore) publishCheckpointV3(cp uint64, seed bool, recoveryGen uint64) (descriptorV3, error) {
	v := s.v3
	if s.metadata != nil {
		if err := s.metadata.BeforeCheckpointAdvance(cp); err != nil {
			return descriptorV3{}, err
		}
	}
	if h := s.chunk.Load(); h != nil {
		if err := h.sink.BeforeCheckpointAdvance(cp); err != nil {
			return descriptorV3{}, err
		}
	}
	prepared, err := s.prepareOwnedMapCheckpointV3(cp)
	if err != nil {
		return descriptorV3{}, err
	}
	if h := v.testMapPrepared; h != nil {
		h()
	}
	v.syncMu.Lock()
	defer v.syncMu.Unlock()
	// The candidate proof alone is insufficient: it was prepared after extent
	// I/O and could belong to a newer Recover lifetime than those bytes.
	if !v.flushLifetimeValidV3(recoveryGen) {
		return descriptorV3{}, ErrMapCoverageStale
	}
	wack := v.wack.Load()
	if seed {
		wack = max(wack, cp)
	}
	if wack < cp {
		return descriptorV3{}, ErrMapCoverageStale
	}
	s.mu.RLock()
	retention := s.recoveryRetentionLSNs
	s.mu.RUnlock()
	releaseLSN := uint64(0)
	if cp > retention {
		releaseLSN = cp - retention
	}
	tail := v.w.reclaimThroughV3(releaseLSN)
	current := v.pub.current()
	if current.BaseState == BaseInstalling {
		// An incomplete BASE still needs its entire install-lane suffix.
		tail = min(tail, current.BaseCursor)
	}
	d, err := v.pub.publishMapV3(&s.mu, nil, prepared, func(n *descriptorV3) error {
		n.W = max(n.W, wack)
		n.CheckpointLSN = cp
		n.RetainedTail = max(n.RetainedTail, tail)
		n.RetainedHead = max(n.RetainedHead, n.RetainedTail)
		return nil
	})
	if err != nil {
		return descriptorV3{}, err
	}
	// Reuse still waits for BOTH valid slots. This uses the same sole
	// publisher and existing immutable map; failure keeps the old slot pin
	// and latches the store before the flusher retires dirty entries.
	if _, err := v.pub.rolloverIfPinned(); err != nil {
		return descriptorV3{}, err
	}
	if seed {
		v.appended.Store(max(v.appended.Load(), cp))
		v.wack.Store(wack)
	}
	return d, nil
}
