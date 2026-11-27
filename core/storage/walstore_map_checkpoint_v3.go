package storage

// Actual owned-map checkpoint path, still dark until the single activation.
// capture/lifecycle admission remains with the existing caller; preparation
// performs no I/O under store/descriptor/leader locks. Publication uses the
// existing syncMu -> store.mu -> descMu -> fileSyncMu graph.

// Caller holds WALStore.mu. Higher-LSN writes update the max; a reused or
// lower/equal effective LSN is conservative UNKNOWN, independent of the max.
// A reused ordered LSN below a speculative max can set UNKNOWN here without
// invalidating a prefix proof; the max and retained suffix stay conservative.
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
		return nil, mapCoverageRefusal("map_binding_changed")
	}
	proof := domain.snapshotV3()
	// Entries may already contain LSNs above cp or W from earlier appends: max-LSN
	// entries are monotone and idempotent under retained-suffix replay.
	entries := append([]chunkLSNEntryV3(nil), v.mapEntries...)
	v.pub.descMu.Unlock()
	s.mu.RUnlock()
	c, err := v.ownedMap.prepareNextV3(cp, entries, prior)
	if err != nil {
		return nil, err
	}
	if !proof.validV3(domain) {
		v.ownedMap.abandonCandidateV3(c) // prepared but never publishable
		return nil, mapCoverageRefusal("proof_changed_during_prepare")
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
		v.syncMu.Lock()
		defer v.syncMu.Unlock()
		s.mu.Lock()
		defer s.mu.Unlock()
		if !v.flushLifetimeValidV3(recoveryGen) {
			return mapCoverageRefusal("recovery_lifetime_changed")
		}
		return s.rolloverCheckpointV3Locked()
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
		prepared.abandonV3()
		return descriptorV3{}, mapCoverageRefusal("recovery_lifetime_changed")
	}
	wack := v.wack.Load()
	if seed {
		wack = max(wack, cp)
	}
	if wack < cp {
		prepared.abandonV3()
		return descriptorV3{}, mapCoverageRefusal("checkpoint_above_wack")
	}
	s.mu.RLock()
	retention := s.catchUpRetentionLocked(v.pub.current())
	s.mu.RUnlock()
	releaseLSN := catchUpFloorV3(cp, retention)
	tail := v.w.reclaimThroughV3(releaseLSN)
	current := v.pub.current()
	if current.BaseState == BaseInstalling {
		// An incomplete BASE still needs its entire install-lane suffix.
		tail = min(tail, current.BaseCursor)
	}
	// Keep writer admission excluded until both durable publications AND the
	// old dirty references are settled. The ordinary publisher releases a
	// supplied store lock before I/O, so acquire it here and pass nil instead.
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := v.pub.publishMapV3(nil, nil, prepared, func(n *descriptorV3) error {
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
	// Only a successful checkpoint plus rollover may retire retry ownership.
	// Every current entry <= cp is durable in extent (a newer concurrent write
	// has a higher LSN and survives). No writer can reuse a released slot until
	// this store lock is released, after these references have disappeared.
	s.retireCheckpointDirtyV3Locked(d.CheckpointLSN)
	if seed {
		v.appended.Store(max(v.appended.Load(), cp))
		v.wack.Store(wack)
	}
	return d, nil
}

// Caller holds store.mu; the successful durable checkpoint proves the current
// versions at/below cp are in extent. Each shard preserves newer WAL entries.
func (s *WALStore) retireCheckpointDirtyV3Locked(cp uint64) {
	for i := range s.dm.shards {
		shard := &s.dm.shards[i]
		shard.mu.Lock()
		for lba, e := range shard.m {
			if e.lsn <= cp {
				delete(shard.m, lba)
			}
		}
		shard.mu.Unlock()
	}
}

// Caller holds store.mu (and, except write admission, the leader role).
// No writer may use the newly released floor until its obsolete dirty
// references are retired. On failure keep those exact retry entries.
func (s *WALStore) rolloverCheckpointV3Locked() error {
	d, floor, valid := s.v3.pub.walSnapshotV3()
	if valid && floor >= d.RetainedTail {
		return nil
	}
	if _, err := s.v3.pub.rolloverIfPinned(); err != nil {
		return err
	}
	s.retireCheckpointDirtyV3Locked(d.CheckpointLSN)
	return nil
}
