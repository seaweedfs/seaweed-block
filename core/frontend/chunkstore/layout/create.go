package layout

import (
	"context"
	"errors"
	"fmt"
)

// NewVolumeStore is a new volume's never-reused directory as its creator
// sees it: exclusive creates of the VOLUME record and of the first HEAD
// (ErrExists if the entry is already there), and their reads.
type NewVolumeStore interface {
	ReadHead(ctx context.Context) (h Head, found bool, err error)
	CreateHead(ctx context.Context, h Head) error
	ReadVolumeRecord(ctx context.Context) (v Volume, found bool, err error)
	CreateVolumeRecord(ctx context.Context, v Volume) error
}

// Unclaimed is the first HEAD of a volume: its id, epoch 0, no holder, no
// claim id, idle, live, position 0, and current (nil for a new volume, the
// copied root for a restore target). The first Start claims it from a
// fresh root.
func Unclaimed(id VolumeID, current *RootName) Head {
	return Head{VolumeID: id, Activity: Activity{Kind: ActIdle}, Current: current}
}

// CreateVolume creates a volume in its directory: the VOLUME record, then
// the unclaimed HEAD, each exclusively. A re-run after a crash between them
// finds VOLUME of the same id and writes the HEAD; an existing HEAD of the
// same id means it is done. Another id in either entry is ErrIdentity.
func CreateVolume(ctx context.Context, st NewVolumeStore, v Volume) (Head, error) {
	return createVolume(ctx, st, v, nil)
}

// CreateTarget creates a restore target from the source's named snapshot,
// in this order: the target is recorded as a dependent of that snapshot
// (RunAddDependent, which refuses a snapshot naming another volume's
// chunks), the snapshot's segments and root are written into the target's
// directory (copy), then the target's VOLUME, then its unclaimed HEAD with
// current = that root. Its HEAD is never written before the dependent: an
// operator's release of a target without HEAD writes the deleted HEAD
// first, and the target's HEAD create then fails.
func CreateTarget(ctx context.Context, src StartStore, srcReader *Reader, snapshot string, st NewVolumeStore, v Volume, copy func(ctx context.Context, root RootName) error) (Head, error) {
	if copy == nil {
		return Head{}, errors.New("layout: CreateTarget needs a copy of the snapshot's root")
	}
	source, found, err := src.ReadVolumeRecord(ctx)
	if err != nil {
		return Head{}, err
	}
	if !found {
		return Head{}, ErrMissing
	}
	if err := source.Geometry.Check(); err != nil {
		return Head{}, err
	}
	if v.Geometry != source.Geometry {
		return Head{}, ErrGeometryMismatch
	}
	sh, err := RunAddDependent(ctx, src, srcReader, snapshot, Dependent{Target: v.Name, TargetID: v.ID})
	if err != nil {
		return Head{}, err
	}
	root := sh.Snapshots[snapshot].Root
	if err := copy(ctx, root); err != nil {
		return Head{}, fmt.Errorf("layout: copy %s: %w", root, err)
	}
	return createVolume(ctx, st, v, &root)
}

func createVolume(ctx context.Context, st NewVolumeStore, v Volume, current *RootName) (Head, error) {
	if st == nil {
		return Head{}, errors.New("layout: create needs the volume's store")
	}
	if _, err := v.Encode(); err != nil {
		return Head{}, err
	}
	if err := st.CreateVolumeRecord(ctx, v); err != nil {
		if !errors.Is(err, ErrExists) {
			return Head{}, err
		}
		got, found, rerr := st.ReadVolumeRecord(ctx)
		if rerr != nil {
			return Head{}, rerr
		}
		if !found || got.ID != v.ID {
			return Head{}, fmt.Errorf("%w: VOLUME holds %s, not %s", ErrIdentity, got.ID, v.ID)
		}
		if got.Geometry != v.Geometry {
			return Head{}, ErrGeometryMismatch
		}

	}
	h := Unclaimed(v.ID, current)
	if err := h.check(); err != nil {
		return Head{}, err
	}
	if err := st.CreateHead(ctx, h); err != nil {
		if !errors.Is(err, ErrExists) {
			return Head{}, err
		}
		got, found, rerr := st.ReadHead(ctx)
		if rerr != nil {
			return Head{}, rerr
		}
		if !found || got.VolumeID != v.ID {
			return Head{}, fmt.Errorf("%w: HEAD of %s, not %s", ErrIdentity, got.VolumeID, v.ID)
		}
		if got.State != StateLive {
			// the operator released this target and tombstoned it first
			return Head{}, fmt.Errorf("%w: HEAD of %s is %s", ErrVolumeDeleting, v.ID, got.State)
		}
		if current != nil && (got.Current == nil || *got.Current != *current) {
			// another create (an empty volume) got there first, or the
			// target has moved on since: refuse rather than report a restore
			return Head{}, fmt.Errorf("%w: %s was created without the restored image", ErrExists, v.ID)
		}
		return got, nil
	}
	return h, nil
}
