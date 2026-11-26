package master

import (
	"context"
	"errors"
	"fmt"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// LayoutHeadSource is the catalog as the master reads HEAD through it: the
// same filer RPC read the holder uses, with the entry's modification time
// (chunkstore.BCatalog.ReadHeadObserved).
type LayoutHeadSource interface {
	ReadHeadObserved(ctx context.Context, v chunkstore.VolumeRef) (layout.Head, int64, bool, error)
}

// NewLayoutWorkspaceHEADReader is the master's one HEAD reader: it resolves
// the volume's immutable identity from the master's own lifecycle record,
// then reads HEAD through the catalog. Content, generation and the head time
// come from that one entry; there is no second source.
func NewLayoutWorkspaceHEADReader(src LayoutHeadSource, resolve func(context.Context, string) (chunkstore.VolumeRef, error)) (func(context.Context, string) (authority.WorkspaceHead, error), error) {
	if src == nil || resolve == nil {
		return nil, errors.New("workspace HEAD needs the layout catalog and the master volume resolver")
	}
	return func(ctx context.Context, name string) (authority.WorkspaceHead, error) {
		if err := ctx.Err(); err != nil {
			return authority.WorkspaceHead{}, err
		}
		v, err := resolve(ctx, name)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		if v.Name != name {
			return authority.WorkspaceHead{}, layout.ErrIdentity
		}
		if err := (layout.Volume{ID: v.ID, Name: v.Name, Format: layout.VolumeFormat}).CheckIdentity(); err != nil {
			return authority.WorkspaceHead{}, err
		}
		h, mtime, found, err := src.ReadHeadObserved(ctx, v)
		if err != nil {
			return authority.WorkspaceHead{}, err
		}
		if !found {
			return authority.WorkspaceHead{}, layout.ErrHeadAbsent
		}
		proof := authority.WorkspaceHead{Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.Position, Detached: h.State == layout.StateDetached, FilerMtimeNS: mtime}
		if h.State == layout.StateDeleting || h.State == layout.StateDeleted {
			proof.Deleting = true
			return proof, authority.ErrWorkspaceHeadDeleting
		}
		if h.Epoch == 0 {
			return proof, authority.ErrWorkspaceHeadUnclaimed
		}
		return proof, nil
	}, nil
}

// WorkspaceLayoutVolume reads only published lifecycle intent. A missing
// identity cannot be inferred from a filer directory or a public HEAD.
func (h *Host) WorkspaceLayoutVolume(ctx context.Context, name string) (chunkstore.VolumeRef, error) {
	if err := ctx.Err(); err != nil {
		return chunkstore.VolumeRef{}, err
	}
	if h == nil || h.lifecycle == nil || h.lifecycle.Volumes == nil {
		return chunkstore.VolumeRef{}, fmt.Errorf("workspace lifecycle intent is not ready")
	}
	rec, ok := h.lifecycle.Volumes.GetVolumeSnapshot(name)
	if !ok {
		return chunkstore.VolumeRef{}, fmt.Errorf("workspace volume %q is not registered", name)
	}
	id, err := layout.ParseVolumeID(rec.Spec.LayoutID)
	if err != nil {
		return chunkstore.VolumeRef{}, fmt.Errorf("workspace volume %q has no valid layout identity; recreate the legacy volume: %w", name, err)
	}
	v := chunkstore.VolumeRef{Name: rec.Spec.VolumeID, ID: id}
	if v.Name != name {
		return chunkstore.VolumeRef{}, layout.ErrIdentity
	}
	return v, nil
}
