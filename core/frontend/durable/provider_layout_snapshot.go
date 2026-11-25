package durable

import (
	"context"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// layoutSnapshotter is the named-snapshot surface of the attached layout
// remote (chunkstore.LayoutRemote).
type layoutSnapshotter interface {
	SetSnapshot(name string, meta layout.SnapshotMeta)
	TakeSnapshotErr() error
	CommitPendingSnapshot(ctx context.Context) error
	ReadHead(ctx context.Context) (layout.Head, error)
}

// snapshotLocks serializes named snapshots of one volume, so one request's
// name is never committed by another's cut.
var snapshotLocks sync.Map // volume -> *sync.Mutex

// SnapshotVolume takes a named snapshot of the volume through its attached
// layout runtime: the name and its retention facts ride on the next cut's
// commit (the engine's barrier gives the image at H); when the cut had
// nothing to publish, the current image is named by a commit of its own.
// A replay of a name already in HEAD returns the facts recorded there. A
// name or facts the kernel refuses is the typed refusal; the cut itself
// still commits unnamed. It returns HEAD with the name recorded
// (chunkstore.SnapshotInfoFromHead makes the request's answer).
func (p *DurableProvider) SnapshotVolume(ctx context.Context, volumeID, name string, meta layout.SnapshotMeta) (layout.Head, error) {
	if err := layout.CheckSnapshot(name, meta); err != nil {
		return layout.Head{}, err
	}
	r, err := p.LayoutRemote(ctx, volumeID)
	if err != nil {
		return layout.Head{}, err
	}
	sr, ok := r.(layoutSnapshotter)
	if !ok {
		return layout.Head{}, fmt.Errorf("durable: the remote of %s is not a layout runtime", volumeID)
	}
	mu, _ := snapshotLocks.LoadOrStore(volumeID, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	h, err := sr.ReadHead(ctx)
	if err != nil {
		return layout.Head{}, err
	}
	if _, ok := h.Snapshots[name]; ok {
		return h, nil
	}
	sr.SetSnapshot(name, meta)
	if _, err := p.CutVolume(ctx, volumeID, 0); err != nil {
		return layout.Head{}, err
	}
	if err := sr.TakeSnapshotErr(); err != nil {
		return layout.Head{}, err
	}
	if err := sr.CommitPendingSnapshot(ctx); err != nil {
		return layout.Head{}, err
	}
	if h, err = sr.ReadHead(ctx); err != nil {
		return layout.Head{}, err
	}
	if _, ok := h.Snapshots[name]; !ok {
		return layout.Head{}, fmt.Errorf("durable: snapshot %q of %s is not in HEAD after its commit", name, volumeID)
	}
	return h, nil
}
