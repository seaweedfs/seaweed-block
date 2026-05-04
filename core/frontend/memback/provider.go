// Package memback is the T1 contract-smoke backend
// (sketch: sw-block/design/v3-phase-15-t1-sketch.md §8).
//
// memback stores bytes in process-local memory per backend
// instance. It is NOT replicated, NOT durable, and MUST NOT be
// used to claim G8 failover data continuity. It exists to prove
// the frontend contract — healthy open, lineage-gated I/O,
// stale-primary rejection — and to be easy to remove once a
// real block backend lands.
package memback

import (
	"context"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// Provider opens memback backends against a ProjectionView.
type Provider struct {
	view frontend.ProjectionView

	// volumes holds one shared byte map per (volumeID, lineage).
	// Keyed by the full Identity so a reopened backend on a new
	// lineage starts from a clean slice — consistent with the
	// explicit non-claim in sketch §8 that pre-move bytes do not
	// survive on the new primary.
	mu      sync.Mutex
	volumes map[Identity]*volumeStore
}

// Identity is the internal map key. Same layout as
// frontend.Identity; kept distinct so we never accidentally
// expose internal storage keying through the public API.
type Identity = frontend.Identity

// NewProvider constructs a Provider bound to view. view is the
// ONLY source of authority lineage the provider is allowed to
// consult — sketch §9 boundary rule.
func NewProvider(view frontend.ProjectionView) *Provider {
	return &Provider{
		view:    view,
		volumes: map[Identity]*volumeStore{},
	}
}

// Open polls the projection until it reports healthy for the
// requested volumeID and snapshots the lineage into the
// backend's Identity. Returns ErrNotReady if ctx expires before
// the projection becomes healthy.
func (p *Provider) Open(ctx context.Context, volumeID string) (frontend.Backend, error) {
	const poll = 10 * time.Millisecond
	for {
		proj := p.view.Projection()
		if proj.VolumeID == volumeID && proj.Healthy {
			id := frontend.Identity{
				VolumeID:        proj.VolumeID,
				ReplicaID:       proj.ReplicaID,
				Epoch:           proj.Epoch,
				EndpointVersion: proj.EndpointVersion,
			}
			store := p.storeFor(id)
			return newBackend(p.view, id, store), nil
		}
		select {
		case <-ctx.Done():
			return nil, frontend.ErrNotReady
		case <-time.After(poll):
		}
	}
}

// storeFor returns the shared byte store for a given Identity,
// creating it on first use. Backends opened against the same
// lineage share a store so a round-trip write→read within a
// single (volume, replica, epoch, ev) sees its own bytes.
func (p *Provider) storeFor(id Identity) *volumeStore {
	p.mu.Lock()
	defer p.mu.Unlock()
	s, ok := p.volumes[id]
	if !ok {
		s = newVolumeStore()
		p.volumes[id] = s
	}
	return s
}
