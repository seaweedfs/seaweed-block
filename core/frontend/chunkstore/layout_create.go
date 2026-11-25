package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

var _ layout.NewVolumeStore = (*LayoutStore)(nil)
var _ layout.TargetStore = (*LayoutStore)(nil)

// CreateHead writes the volume's first HEAD (unclaimed) exclusively;
// layout.ErrExists if a HEAD is already there.
func (s *LayoutStore) CreateHead(ctx context.Context, h layout.Head) error {
	if h.State != layout.StateLive || h.Holder != "" {
		return errors.New("chunkstore: only an unclaimed live HEAD is created")
	}
	return s.createHead(ctx, h)
}

// CreateDeletedHead writes a deleted HEAD (a tombstone) exclusively;
// layout.ErrExists if a HEAD is already there.
func (s *LayoutStore) CreateDeletedHead(ctx context.Context, h layout.Head) error {
	if h.State != layout.StateDeleted {
		return errors.New("chunkstore: only a deleted HEAD is created as a tombstone")
	}
	return s.createHead(ctx, h)
}

func (s *LayoutStore) createHead(ctx context.Context, h layout.Head) error {
	if h.VolumeID != s.id {
		return layout.ErrIdentity
	}
	raw, err := h.Encode()
	if err != nil {
		return err
	}
	e := layoutEntry(headName, raw)
	e.Extended = map[string][]byte{layoutKind: []byte("head"), layoutGen: []byte(strconv.FormatUint(1, 10)), layoutVolume: []byte(s.id.String())}
	err = s.f.CreateEntry(ctx, s.root, e, true)
	if errors.Is(err, ErrExists) {
		return layout.ErrExists
	}
	return err
}

// ReadVolumeRecord reads the volume's VOLUME record.
func (s *LayoutStore) ReadVolumeRecord(ctx context.Context) (layout.Volume, bool, error) {
	e, err := s.f.LookupEntry(ctx, s.root, layout.VolumeName)
	if errors.Is(err, ErrNotFound) {
		return layout.Volume{}, false, nil
	}
	if err != nil {
		return layout.Volume{}, false, err
	}
	v, err := layout.DecodeVolume(e.Content)
	if err != nil {
		return layout.Volume{}, false, fmt.Errorf("%w: VOLUME: %v", layout.ErrCorrupt, err)
	}
	return v, true, nil
}

// CreateVolumeRecord writes the VOLUME record exclusively; layout.ErrExists
// if one is already there.
func (s *LayoutStore) CreateVolumeRecord(ctx context.Context, v layout.Volume) error {
	if v.ID != s.id {
		return layout.ErrIdentity
	}
	raw, err := v.Encode()
	if err != nil {
		return err
	}
	e := layoutEntry(layout.VolumeName, raw)
	e.Extended = map[string][]byte{layoutKind: []byte("volume"), layoutVolume: []byte(s.id.String())}
	err = s.f.CreateEntry(ctx, s.root, e, true)
	if errors.Is(err, ErrExists) {
		return layout.ErrExists
	}
	return err
}
