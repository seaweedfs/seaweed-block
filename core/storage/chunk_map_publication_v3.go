package storage

// Dark row-7 map publication/proof controls. These are NOT flusher/provider
// integration: the row-5b real WALStore must call the lifecycle invalidations
// and prepare from its permitted quiesced mutation snapshot before activation.
import (
	"encoding/binary"
	"errors"
	"math"
	"os"
	"sync"
	"sync/atomic"
)

var ErrMapCoverageStale = errors.New("storage: v3 chunk map coverage or proof stale")

// Each open has a distinct domain pointer. Epochs never wrap. Recover must
// invalidate at START and at END (also on failure); reset and every install
// transition invalidate too. No store/map lock is taken by an invalidation.
// The owner must serialize these lifecycle transitions against descriptor
// publication using its existing lifecycle admission; this type adds no lock.
type mapProofEpochV3 struct{ epoch, mutation atomic.Uint64 }

func newMapProofEpochV3() *mapProofEpochV3 { d := &mapProofEpochV3{}; d.epoch.Store(1); return d }
func advanceMapProofCounterV3(v *atomic.Uint64) error {
	for {
		n := v.Load()
		if n >= counterMaxV3 {
			return ErrCounterExhausted
		}
		if v.CompareAndSwap(n, n+1) {
			return nil
		}
	}
}
func (d *mapProofEpochV3) invalidateV3() error { return advanceMapProofCounterV3(&d.epoch) }
func (d *mapProofEpochV3) mutatedV3() error    { return advanceMapProofCounterV3(&d.mutation) }

type mapPublicationProofV3 struct {
	domain          *mapProofEpochV3
	epoch, mutation uint64
}

func (d *mapProofEpochV3) snapshotV3() mapPublicationProofV3 {
	return mapPublicationProofV3{domain: d, epoch: d.epoch.Load(), mutation: d.mutation.Load()}
}
func (p mapPublicationProofV3) validV3(d *mapProofEpochV3) bool {
	return d != nil && p.domain == d && p.epoch < counterMaxV3 && p.mutation < counterMaxV3 && p.epoch == d.epoch.Load() && p.mutation == d.mutation.Load()
}

type preparedMapPublicationV3 struct {
	candidate *chunkMapCandidateV3
	proof     mapPublicationProofV3
}

// The caller supplies entries from its permitted mutation/version boundary.
// Preparation takes the sidecar mutex and I/O only, before store/desc locks.
func prepareMapPublicationV3(m *chunkLSNMapV3, d *mapProofEpochV3, g, C uint64, entries []chunkLSNEntryV3, prior *chunkMapCandidateV3) (*preparedMapPublicationV3, error) {
	if m == nil || d == nil {
		return nil, mapCoverageRefusal("map_binding_missing")
	}
	proof := d.snapshotV3()
	c, e := m.Prepare(g, C, entries, prior)
	if e != nil {
		return nil, e
	}
	if !proof.validV3(d) {
		m.abandonCandidateV3(c) // prepared but never publishable
		return nil, mapCoverageRefusal("proof_changed_during_prepare")
	}
	return &preparedMapPublicationV3{candidate: c, proof: proof}, nil
}
func sameMapTupleV3(d descriptorV3, c *chunkMapCandidateV3) bool {
	if c == nil || c.owner == nil {
		return false
	}
	id := c.owner.identity
	return d.UUID == id.UUID && sameGeometryV3(d, id) && d.MapLineage == id.MapLineage && d.MapFormatID == mapFormatIDV1 && d.MapGeneration == c.generation && d.MapCoveredCheckpoint == c.covered && d.MapRootLocator == c.rootOffset && d.MapRootHash == c.hash
}

// bindMapV3 is used only while opening/quiesced. Snapshot both slots under
// descMu, validate all dependencies without store/desc locks, then recheck the
// snapshot before installing the open proof domain. A fallback remains a root
// even when none of its pages belong to the selected generation.
func (p *descriptorPublisherV3) bindMapV3(c *chunkMapCandidateV3, d *mapProofEpochV3) error {
	if c == nil || c.owner == nil || d == nil {
		return mapCoverageRefusal("map_binding_missing")
	}
	p.descMu.Lock()
	current, other, otherValid := p.cur, p.other, p.otherValid
	p.descMu.Unlock()
	if !sameMapTupleV3(current, c) {
		return mapCoverageRefusal("map_tuple_changed")
	}
	if _, _, e := readChunkLSNMapV3(c.owner.fd, current, c.owner.chunkBlocks); e != nil {
		return e
	}
	if otherValid {
		if other.UUID != current.UUID || other.MapLineage != current.MapLineage || !sameGeometryV3(other, current) {
			return ErrMapInvalid
		}
		if _, _, e := readChunkLSNMapV3(c.owner.fd, other, c.owner.chunkBlocks); e != nil {
			return e
		}
	}
	p.descMu.Lock()
	defer p.descMu.Unlock()
	if p.cur != current || p.otherValid != otherValid || (otherValid && p.other != other) {
		return mapCoverageRefusal("descriptor_changed_during_bind")
	}
	p.mapCurrent = c
	p.mapDomain = d
	return nil
}
func (p *descriptorPublisherV3) validateMapPublicationV3(next descriptorV3, prepared *preparedMapPublicationV3) error {
	if p.mapCurrent == nil {
		if prepared != nil {
			return mapCoverageRefusal("map_not_bound")
		}
		return nil
	} // dark core not bound yet
	c := p.mapCurrent
	if prepared != nil {
		c = prepared.candidate
		if c == nil || c.owner != p.mapCurrent.owner || !prepared.proof.validV3(p.mapDomain) {
			return mapCoverageRefusal("proof_changed")
		}
		if c.generation <= p.mapCurrent.generation || c.covered < p.mapCurrent.covered {
			return mapCoverageRefusal("candidate_superseded")
		}
	}
	if !sameMapTupleV3(next, c) || c.covered < next.CheckpointLSN || c.covered > next.W {
		return mapCoverageRefusal("map_coverage_mismatch")
	}
	return nil
}
func (p *descriptorPublisherV3) publishMapV3(storeMu sync.Locker, snap func(), prepared *preparedMapPublicationV3, mutate func(*descriptorV3) error) (descriptorV3, error) {
	if prepared == nil || prepared.candidate == nil {
		return descriptorV3{}, mapCoverageRefusal("candidate_missing")
	}
	d, err := p.publishWithMapV3(storeMu, snap, func(d *descriptorV3) error {
		if mutate != nil {
			if e := mutate(d); e != nil {
				return e
			}
		}
		*d = prepared.candidate.descriptor(*d)
		return nil
	}, prepared)
	if err != nil {
		// Definite abandon: unless the slot I/O started (the candidate is
		// the uncertain one, pinned until reopen), no slot can name it.
		c := prepared.candidate
		p.descMu.Lock()
		unc := p.uncertain
		if unc == nil || unc.MapRootLocator != c.rootOffset || unc.MapGeneration != c.generation {
			prepared.abandonV3()
		}
		p.descMu.Unlock()
	}
	return d, err
}

// invalidateMapProofV3 is called under WALStore.mu at a lifecycle boundary.
// Taking the existing descMu orders the epoch change after any publication
// already inside descriptor I/O. It performs no callback, allocation or I/O.
// A not-yet-bound dark store has no candidate domain to invalidate.
func (p *descriptorPublisherV3) invalidateMapProofV3() error {
	p.descMu.Lock()
	defer p.descMu.Unlock()
	if p.mapDomain == nil {
		return nil
	}
	return p.mapDomain.invalidateV3()
}

// openDescriptorMapV3 validates geometry from the root and both valid slot
// dependencies before any store background worker is started. Geometry is
// subsequently checked by the chunk/provider caller against its configuration.
func openDescriptorMapV3(path string, selected descriptorV3, fallback *descriptorV3) (*chunkLSNMapV3, *chunkMapCandidateV3, error) {
	if selected.MapRootLocator > math.MaxInt64-64 {
		return nil, nil, ErrMapInvalid
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	var raw [8]byte
	_, err = f.ReadAt(raw[:], int64(selected.MapRootLocator)+56)
	err = errors.Join(err, f.Close())
	if err != nil {
		return nil, nil, err
	}
	cb := binary.LittleEndian.Uint64(raw[:])
	m, c, err := openChunkLSNMapV3(path, selected, cb)
	if err != nil {
		return nil, nil, err
	}
	if fallback != nil {
		if fallback.UUID != selected.UUID || fallback.MapLineage != selected.MapLineage || !sameGeometryV3(*fallback, selected) {
			err = ErrMapInvalid
		} else {
			var refs []mapPageRefV3
			if _, refs, err = readChunkLSNMapV3(m.fd, *fallback, cb); err == nil {
				if sh, e := mapShapeForV3(*fallback, cb); e == nil {
					m.registerRootV3(fallback.MapGeneration, fallback.MapRootLocator, sh.aligned, refs)
				}
			}
		}
		if err != nil {
			_ = m.Close()
			return nil, nil, err
		}
	}
	return m, c, nil
}
