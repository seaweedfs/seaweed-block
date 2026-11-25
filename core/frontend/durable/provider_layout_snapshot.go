package durable

import (
	"context"
	"fmt"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
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
// still commits unnamed.
func (p *DurableProvider) SnapshotVolume(ctx context.Context, volumeID, name string, meta layout.SnapshotMeta) (chunkstore.SnapshotInfo, error) {
	if err := layout.CheckSnapshot(name, meta); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	r, err := p.LayoutRemote(ctx, volumeID)
	if err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	sr, ok := r.(layoutSnapshotter)
	if !ok {
		return chunkstore.SnapshotInfo{}, fmt.Errorf("durable: the remote of %s is not a layout runtime", volumeID)
	}
	mu, _ := snapshotLocks.LoadOrStore(volumeID, &sync.Mutex{})
	mu.(*sync.Mutex).Lock()
	defer mu.(*sync.Mutex).Unlock()

	h, err := sr.ReadHead(ctx)
	if err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if _, ok := h.Snapshots[name]; ok {
		return snapshotInfo(volumeID, name, h), nil
	}
	sr.SetSnapshot(name, meta)
	if _, err := p.CutVolume(ctx, volumeID, 0); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if err := sr.TakeSnapshotErr(); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if err := sr.CommitPendingSnapshot(ctx); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if h, err = sr.ReadHead(ctx); err != nil {
		return chunkstore.SnapshotInfo{}, err
	}
	if _, ok := h.Snapshots[name]; !ok {
		return chunkstore.SnapshotInfo{}, fmt.Errorf("durable: snapshot %q of %s is not in HEAD after its commit", name, volumeID)
	}
	return snapshotInfo(volumeID, name, h), nil
}

// snapshotInfo is the request's answer from HEAD's record of the name.
func snapshotInfo(volumeID, name string, h layout.Head) chunkstore.SnapshotInfo {
	s := h.Snapshots[name]
	return chunkstore.SnapshotInfo{
		VolumeID: volumeID, Name: name, Root: s.Root.String(), Dependents: s.Dependents,
		HeadGen: h.Gen, HeadEpoch: h.Epoch, CutLSN: h.Position, State: chunkstore.SnapActive,
		Class: s.Meta.Class, CreatedNs: s.Meta.CreatedNs, ExpiresAtNs: s.Meta.ExpiresNs, Labels: s.Meta.Labels,
	}
}
