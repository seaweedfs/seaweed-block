package storage

// Dark row-7 map publication/proof controls. These are NOT flusher/provider
// integration: the row-5b real WALStore must call the lifecycle invalidations
// and prepare from its permitted quiesced mutation snapshot before activation.
import (
	"errors"
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
		return nil, ErrMapCoverageStale
	}
	proof := d.snapshotV3()
	c, e := m.Prepare(g, C, entries, prior)
	if e != nil {
		return nil, e
	}
	if !proof.validV3(d) {
		return nil, ErrMapCoverageStale
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

// bindMapV3 is used only while opening/quiesced. Dependencies are read before
// descMu. The final descriptor comparison under descMu prevents a stale bind.
func (p *descriptorPublisherV3) bindMapV3(c *chunkMapCandidateV3, d *mapProofEpochV3) error {
	if c == nil || c.owner == nil || d == nil {
		return ErrMapCoverageStale
	}
	tuple := c.descriptor(c.owner.identity)
	if _, _, e := readChunkLSNMapV3(c.owner.fd, tuple, c.owner.chunkBlocks); e != nil {
		return e
	}
	p.descMu.Lock()
	defer p.descMu.Unlock()
	if !sameMapTupleV3(p.cur, c) {
		return ErrMapCoverageStale
	}
	p.mapCurrent = c
	p.mapDomain = d
	return nil
}
func (p *descriptorPublisherV3) validateMapPublicationV3(next descriptorV3, prepared *preparedMapPublicationV3) error {
	if p.mapCurrent == nil {
		if prepared != nil {
			return ErrMapCoverageStale
		}
		return nil
	} // dark core not bound yet
	c := p.mapCurrent
	if prepared != nil {
		c = prepared.candidate
		if c == nil || c.owner != p.mapCurrent.owner || !prepared.proof.validV3(p.mapDomain) {
			return ErrMapCoverageStale
		}
		if c.generation <= p.mapCurrent.generation || c.covered < p.mapCurrent.covered {
			return ErrMapCoverageStale
		}
	}
	if !sameMapTupleV3(next, c) || c.covered < next.CheckpointLSN || c.covered > next.W {
		return ErrMapCoverageStale
	}
	return nil
}
func (p *descriptorPublisherV3) publishMapV3(storeMu sync.Locker, snap func(), prepared *preparedMapPublicationV3, mutate func(*descriptorV3) error) (descriptorV3, error) {
	if prepared == nil || prepared.candidate == nil {
		return descriptorV3{}, ErrMapCoverageStale
	}
	return p.publishWithMapV3(storeMu, snap, func(d *descriptorV3) error {
		if mutate != nil {
			if e := mutate(d); e != nil {
				return e
			}
		}
		*d = prepared.candidate.descriptor(*d)
		return nil
	}, prepared)
}
