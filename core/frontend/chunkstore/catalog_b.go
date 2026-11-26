package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// VolumeRef names a volume as the master's volume record does: its name
// and its layout volume id. Its directory is layout.VolumeDir(name, id),
// never reused.
type VolumeRef struct {
	Name string
	ID   layout.VolumeID
}

// Dir is the volume's directory name under the store's base.
func (v VolumeRef) Dir() string { return layout.VolumeDir(v.Name, v.ID) }

// BCatalog is the volume catalog on the content-named layout: every
// operation is a layout kernel step over the volume's filer store.
type BCatalog struct {
	f    Filer
	b    Blobs
	base string
}

// NewBCatalog returns the catalog of the volumes under base.
func NewBCatalog(f Filer, b Blobs, base string) (*BCatalog, error) {
	if f == nil || b == nil || !path.IsAbs(base) || path.Clean(base) != base {
		return nil, errors.New("chunkstore: catalog needs a filer, blobs and a clean base")
	}
	return &BCatalog{f: f, b: b, base: base}, nil
}

// Store returns the volume's filer store.
func (c *BCatalog) Store(v VolumeRef) (*LayoutStore, error) {
	return NewLayoutStore(c.f, c.b, path.Join(c.base, v.Dir()), v.ID)
}

func (c *BCatalog) open(v VolumeRef) (*LayoutStore, *layout.Reader, error) {
	s, err := c.Store(v)
	if err != nil {
		return nil, nil, err
	}
	r, err := layout.NewReader(c.base, v.Dir(), s.Fetch)
	return s, r, err
}

// CreateVolume creates the volume with its unclaimed HEAD.
func (c *BCatalog) CreateVolume(ctx context.Context, v VolumeRef, geometry layout.Geometry) (layout.Head, error) {
	s, err := c.Store(v)
	if err != nil {
		return layout.Head{}, err
	}
	return layout.CreateVolume(ctx, s, layout.Volume{ID: v.ID, Name: v.Name, Format: layout.VolumeFormat, Geometry: geometry})
}

// ReadVolume returns the source's immutable VOLUME record; a restore takes
// its geometry from this record rather than operator flags.
func (c *BCatalog) ReadVolume(ctx context.Context, v VolumeRef) (layout.Volume, error) {
	s, err := c.Store(v)
	if err != nil {
		return layout.Volume{}, err
	}
	got, found, err := s.ReadVolumeRecord(ctx)
	if err != nil {
		return layout.Volume{}, err
	}
	if !found {
		return layout.Volume{}, layout.ErrMissing
	}
	if got.ID != v.ID || got.Name != v.Name {
		return layout.Volume{}, layout.ErrIdentity
	}
	return got, nil
}

// ReadHead reads the volume's HEAD.
func (c *BCatalog) ReadHead(ctx context.Context, v VolumeRef) (layout.Head, bool, error) {
	s, err := c.Store(v)
	if err != nil {
		return layout.Head{}, false, err
	}
	return s.ReadHead(ctx)
}

// ReadHeadObserved reads the volume's HEAD with its entry's modification
// time (LayoutStore.ReadHeadObserved): the master's one HEAD reader.
func (c *BCatalog) ReadHeadObserved(ctx context.Context, v VolumeRef) (layout.Head, int64, bool, error) {
	s, err := c.Store(v)
	if err != nil {
		return layout.Head{}, 0, false, err
	}
	return s.ReadHeadObserved(ctx)
}

// DeleteSnapshot removes the named snapshot; refused while it has
// dependents.
func (c *BCatalog) DeleteSnapshot(ctx context.Context, v VolumeRef, name string) (layout.Head, error) {
	s, err := c.Store(v)
	if err != nil {
		return layout.Head{}, err
	}
	return layout.RunDeleteSnapshot(ctx, s, name)
}

// Restore creates target from the source's named snapshot: the target is
// recorded as the snapshot's dependent, the snapshot's root and segments
// are copied into the target's directory, then the target is created with
// that root as its current image.
func (c *BCatalog) Restore(ctx context.Context, src VolumeRef, snapshot string, target VolumeRef) (layout.Head, error) {
	ss, sr, err := c.open(src)
	if err != nil {
		return layout.Head{}, err
	}
	ts, err := c.Store(target)
	if err != nil {
		return layout.Head{}, err
	}
	copyRoot := func(ctx context.Context, rn layout.RootName) error {
		raw, err := c.fetchVerified(ctx, ss, src, rn.Path(), rn.Hash)
		if err != nil {
			return err
		}
		root, err := layout.DecodeRoot(raw)
		if err != nil {
			return err
		}
		segs := map[layout.SegName][]byte{}
		for _, n := range root.Segments {
			if segs[n], err = c.fetchVerified(ctx, ss, src, n.Path(), n.Hash); err != nil {
				return err
			}
		}
		got, err := layout.PrepareRoot(ctx, ts, root, segs, "restore-"+target.ID.String())
		if err == nil && got != rn {
			err = fmt.Errorf("%w: root %s re-encodes as %s", layout.ErrCorrupt, rn, got)
		}
		return err
	}
	source, found, err := ss.ReadVolumeRecord(ctx)
	if err != nil {
		return layout.Head{}, err
	}
	if !found {
		return layout.Head{}, layout.ErrMissing
	}
	return layout.CreateTarget(ctx, ss, sr, snapshot, ts, layout.Volume{ID: target.ID, Name: target.Name, Format: layout.VolumeFormat, Geometry: source.Geometry}, copyRoot)
}

func (c *BCatalog) fetchVerified(ctx context.Context, s *LayoutStore, v VolumeRef, rel string, want layout.Hash) ([]byte, error) {
	p := path.Join(c.base, v.Dir(), rel)
	raw, found, err := s.Fetch(ctx, p)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("%w: %s", layout.ErrMissing, p)
	}
	if layout.Sum(raw) != want {
		return nil, fmt.Errorf("%w: %s does not hash to its name", layout.ErrCorrupt, p)
	}
	return raw, nil
}

// ReleaseTarget removes target from the source's dependents once the
// target is deleted or no longer names the source's chunks (the operator's
// remove-dependent; a target without HEAD is tombstoned first).
func (c *BCatalog) ReleaseTarget(ctx context.Context, src, target VolumeRef) (layout.Head, error) {
	ss, err := c.Store(src)
	if err != nil {
		return layout.Head{}, err
	}
	ts, tr, err := c.open(target)
	if err != nil {
		return layout.Head{}, err
	}
	return layout.OperatorReleaseDependent(ctx, ss, ts, tr, target.ID)
}

// DeleteVolume deletes the volume in the kernel's order. The sources it is
// released from are the volumes its roots name chunks of (read from its
// references, never from a name given); its objects are every pool,
// segment and root entry in its directory, listed after HEAD is deleting.
// A re-run on a deleted volume sweeps any object that landed late, and a
// re-run after an interruption at any step completes it. It returns the
// roots HEAD names that were already deleted and so were not read (see
// foreignOwners).
func (c *BCatalog) DeleteVolume(ctx context.Context, v VolumeRef, auth layout.DeleteAuthority) ([]layout.RootName, error) {
	s, r, err := c.open(v)
	if err != nil {
		return nil, err
	}
	h, found, err := s.ReadHead(ctx)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, layout.ErrHeadAbsent
	}
	owners, skipped, err := foreignOwners(ctx, r, h)
	if err != nil {
		return nil, err
	}
	var sources []layout.HeadStore
	for _, o := range owners {
		ss, err := c.Store(o)
		if err != nil {
			return nil, err
		}
		sources = append(sources, ss)
	}
	list := func(ctx context.Context) ([]string, error) { return c.listObjects(ctx, v) }
	return skipped, layout.DeleteVolume(ctx, s, auth, v.ID, sources, s, list)
}

// foreignOwners returns the volumes, other than h's own, whose chunks a
// root in h names, from the references themselves.
//
// A delete removes a volume's root objects only after it released the
// volume from every source (layout.DeleteVolume's order), so on a HEAD that
// is deleting or deleted a root whose objects are already gone
// (layout.ErrMissing) has nothing left to release: it is skipped and
// returned. Anything else (a corrupt object, an I/O error), and a missing
// root of a live HEAD, is an error.
func foreignOwners(ctx context.Context, r *layout.Reader, h layout.Head) ([]VolumeRef, []layout.RootName, error) {
	roots := map[layout.RootName]bool{}
	if h.Current != nil {
		roots[*h.Current] = true
	}
	for _, s := range h.Snapshots {
		roots[s.Root] = true
	}
	deleting := h.State == layout.StateDeleting || h.State == layout.StateDeleted
	seen := map[VolumeRef]bool{}
	var skipped []layout.RootName
	for rn := range roots {
		refs, err := rootRefs(ctx, r, rn)
		if err != nil {
			if deleting && errors.Is(err, layout.ErrMissing) {
				skipped = append(skipped, rn)
				continue
			}
			return nil, nil, err
		}
		for _, ref := range refs {
			if ref.OwnerID != h.VolumeID {
				seen[VolumeRef{Name: ref.Owner, ID: ref.OwnerID}] = true
			}
		}
	}
	out := make([]VolumeRef, 0, len(seen))
	for o := range seen {
		out = append(out, o)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Dir() < out[b].Dir() })
	sort.Slice(skipped, func(a, b int) bool { return skipped[a].String() < skipped[b].String() })
	return out, skipped, nil
}

// rootRefs returns every chunk reference the root names.
func rootRefs(ctx context.Context, r *layout.Reader, rn layout.RootName) ([]layout.Ref, error) {
	var out []layout.Ref
	err := r.EachRef(ctx, rn, layout.AllShards, func(_ uint64, ref layout.Ref) bool { out = append(out, ref); return true })
	return out, err
}

// listObjects lists the volume's pool, segment and root entries.
func (c *BCatalog) listObjects(ctx context.Context, v VolumeRef) ([]string, error) {
	dir := path.Join(c.base, v.Dir())
	var out []string
	buckets, err := c.listAll(ctx, path.Join(dir, layout.PoolDir))
	if err != nil {
		return nil, err
	}
	for _, b := range buckets {
		names, err := c.listAll(ctx, path.Join(dir, layout.PoolDir, b))
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			out = append(out, path.Join(layout.PoolDir, b, n))
		}
	}
	for _, sub := range []string{layout.SegDir, layout.RootDir} {
		names, err := c.listAll(ctx, path.Join(dir, sub))
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			out = append(out, path.Join(sub, n))
		}
	}
	return out, nil
}

func (c *BCatalog) listAll(ctx context.Context, dir string) ([]string, error) {
	var out []string
	after := ""
	for {
		es, err := c.f.ListEntries(ctx, dir, "", after, 1000)
		if err != nil {
			return nil, err
		}
		for _, e := range es {
			out = append(out, e.Name)
		}
		if len(es) < 1000 {
			return out, nil
		}
		after = es[len(es)-1].Name
	}
}

// Usage is what a volume's HEAD holds: its snapshots, their dependents, and
// the chunks its current image names, its own and other volumes'.
type Usage struct {
	Snapshots     int `json:"snapshots"`
	Dependents    int `json:"dependents"`
	OwnChunks     int `json:"own_chunks"`
	ForeignChunks int `json:"foreign_chunks"`
}

// Usage reads the volume's usage from its HEAD and current root.
func (c *BCatalog) Usage(ctx context.Context, v VolumeRef) (Usage, error) {
	s, r, err := c.open(v)
	if err != nil {
		return Usage{}, err
	}
	h, found, err := s.ReadHead(ctx)
	if err != nil {
		return Usage{}, err
	}
	if !found {
		return Usage{}, layout.ErrHeadAbsent
	}
	u := Usage{Snapshots: len(h.Snapshots)}
	for _, sn := range h.Snapshots {
		u.Dependents += len(sn.Dependents)
	}
	if h.Current != nil {
		refs, err := rootRefs(ctx, r, *h.Current)
		if err != nil {
			return Usage{}, err
		}
		for _, ref := range refs {
			if ref.OwnerID == h.VolumeID {
				u.OwnChunks++
			} else {
				u.ForeignChunks++
			}
		}
	}
	return u, nil
}
