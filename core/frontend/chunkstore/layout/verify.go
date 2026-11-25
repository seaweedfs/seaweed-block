package layout

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
)

// Verify is a read-only check of one volume's store on this layout: HEAD,
// every root HEAD names (the current image and each snapshot), their
// segments and the chunks they name. It never writes or deletes. With
// Orphans it also lists the volume's own pool, seg and root names that no
// reachable root names (removal 7' deletes those inside the lease; Verify
// only reports them).

// HeadName is HEAD's name under <base>/<volume>/.
const HeadName = "HEAD"

// Verify depths.
const (
	DepthNames = "names" // every named object exists (chunks by name only)
	DepthFull  = "full"  // and every chunk's bytes hash to its name
)

// Finding classes.
const (
	FindMissing   = "MISSING"
	FindCorrupt   = "CORRUPT" // bytes do not hash to the name, or do not decode
	FindMalformed = "MALFORMED"
	FindOrphan    = "ORPHAN"
	FindIdentity  = "IDENTITY" // a chunk reference names an owner id its VOLUME does not hold
)

// List returns the entry names of the directory p. A missing directory is
// (nil, nil).
type List func(ctx context.Context, p string) ([]string, error)

// VerifyOptions configures one pass.
type VerifyOptions struct {
	Depth   string // DepthNames (default) or DepthFull
	Orphans bool   // also report unreachable own names (needs List)
	List    List
}

// Finding is one problem found at Path (relative to <base>).
type Finding struct {
	Class, Path, Detail string
}

// VerifyResult is one pass. Complete is false when the pass could not read
// what it needed; Incomplete says why. Findings are sorted by path.
type VerifyResult struct {
	Complete           bool
	Incomplete         string
	Roots, Segments    int
	Chunks, ChunksZero int
	OrphansChecked     bool
	Findings           []Finding
}

type verifier struct {
	base, vol string
	fetch     Fetch
	opts      VerifyOptions
	res       VerifyResult
	ownPool   map[string]bool // reachable own names, by path relative to the volume
	ownMeta   map[string]bool
	owners    map[string]ownerID // VOLUME id per chunk owner, read once
}

type ownerID struct {
	id VolumeID
	ok bool
}

// Verify checks vol under base; see the top of this file.
func Verify(ctx context.Context, base, vol string, fetch Fetch, opts VerifyOptions) (VerifyResult, error) {
	if err := checkOwner(vol); err != nil {
		return VerifyResult{}, err
	}
	if fetch == nil {
		return VerifyResult{}, errors.New("layout: verify needs a fetch")
	}
	if opts.Depth == "" {
		opts.Depth = DepthNames
	}
	if opts.Depth != DepthNames && opts.Depth != DepthFull {
		return VerifyResult{}, fmt.Errorf("layout: verify depth %q", opts.Depth)
	}
	if opts.Orphans && opts.List == nil {
		return VerifyResult{}, errors.New("layout: verify orphans needs a list")
	}
	v := &verifier{base: base, vol: vol, fetch: fetch, opts: opts, ownPool: map[string]bool{}, ownMeta: map[string]bool{}, owners: map[string]ownerID{}}
	v.res.Complete = true
	head, ok, err := v.head(ctx, vol)
	if err != nil {
		return VerifyResult{}, err
	}
	if ok {
		for _, rn := range headRoots(head) {
			if err := v.root(ctx, vol, rn, true); err != nil {
				return VerifyResult{}, err
			}
		}
		if opts.Orphans {
			if err := v.orphans(ctx, head); err != nil {
				return VerifyResult{}, err
			}
		}
	}
	sort.Slice(v.res.Findings, func(i, j int) bool {
		a, b := v.res.Findings[i], v.res.Findings[j]
		return a.Path < b.Path || a.Path == b.Path && a.Class < b.Class
	})
	return v.res, nil
}

func (v *verifier) find(class, p, detail string) {
	v.res.Findings = append(v.res.Findings, Finding{Class: class, Path: p, Detail: detail})
}

func (v *verifier) incomplete(why string) {
	if v.res.Complete {
		v.res.Complete, v.res.Incomplete = false, why
	}
}

// head reads vol's VOLUME and then its HEAD, decoded against the VOLUME's
// id (a HEAD of another volume id does not decode). A missing or malformed
// record is a finding and leaves nothing to walk; a read error is returned.
func (v *verifier) head(ctx context.Context, vol string) (Head, bool, error) {
	id, ok, err := v.volumeID(ctx, vol, true)
	if err != nil || !ok {
		return Head{}, false, err
	}
	p := path.Join(vol, HeadName)
	b, ok, err := v.fetch(ctx, path.Join(v.base, p))
	if err != nil {
		return Head{}, false, fmt.Errorf("layout: read %s: %w", p, err)
	}
	if !ok {
		v.find(FindMissing, p, "no HEAD")
		v.incomplete("no HEAD")
		return Head{}, false, nil
	}
	h, err := DecodeHead(b, 0, id)
	if err != nil {
		v.find(FindMalformed, p, err.Error())
		v.incomplete("HEAD does not decode against the VOLUME id")
		return Head{}, false, nil
	}
	return h, true, nil
}

// volumeID reads vol's VOLUME record. report: findings for this volume
// (false for a dependent, where absence only means "no longer a dependent").
func (v *verifier) volumeID(ctx context.Context, vol string, report bool) (VolumeID, bool, error) {
	p := path.Join(vol, VolumeName)
	b, ok, err := v.fetch(ctx, path.Join(v.base, p))
	if err != nil {
		return VolumeID{}, false, fmt.Errorf("layout: read %s: %w", p, err)
	}
	if !ok {
		if report {
			v.find(FindMissing, p, "no VOLUME")
			v.incomplete("no VOLUME")
		}
		return VolumeID{}, false, nil
	}
	rec, err := DecodeVolume(b)
	if err != nil {
		if report {
			v.find(FindMalformed, p, err.Error())
			v.incomplete("VOLUME does not decode")
		}
		return VolumeID{}, false, nil
	}
	return rec.ID, true, nil
}

// headRoots is the current root and every snapshot's root, each once.
func headRoots(h Head) []RootName {
	seen := map[RootName]bool{}
	var out []RootName
	add := func(rn RootName) {
		if !seen[rn] {
			seen[rn] = true
			out = append(out, rn)
		}
	}
	if h.Current != nil {
		add(*h.Current)
	}
	names := make([]string, 0, len(h.Snapshots))
	for n := range h.Snapshots {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		add(h.Snapshots[n].Root)
	}
	return out
}

// classify turns a read error into a finding; a plain read failure is
// returned instead (the pass cannot judge what it could not read).
func (v *verifier) classify(p string, err error) error {
	switch {
	case errors.Is(err, ErrMissing):
		v.find(FindMissing, p, err.Error())
	case errors.Is(err, ErrCorrupt):
		v.find(FindCorrupt, p, err.Error())
	default:
		return err
	}
	return nil
}

// root walks one root of owner: its segments and the chunks they name.
// count is false for a dependent's roots (walked only to learn which of
// this volume's names they reach).
func (v *verifier) root(ctx context.Context, owner string, rn RootName, count bool) error {
	r, err := NewReader(v.base, owner, v.fetch)
	if err != nil {
		return err
	}
	if owner == v.vol {
		v.ownMeta[rn.Path()] = true
	}
	img, err := r.Open(ctx, rn)
	if err != nil {
		if !count {
			v.incomplete("a dependent's root " + path.Join(owner, rn.Path()) + " is unreadable")
			return nil
		}
		return v.classify(path.Join(owner, rn.Path()), err)
	}
	if count {
		v.res.Roots++
	}
	shards := make([]uint64, 0, len(img.root.Segments))
	for s := range img.root.Segments {
		shards = append(shards, s)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	for _, s := range shards {
		sn := img.root.Segments[s]
		if owner == v.vol {
			v.ownMeta[sn.Path()] = true
		}
		seg, _, err := img.Segment(ctx, s)
		if err != nil {
			if !count {
				v.incomplete("a dependent's segment " + path.Join(owner, sn.Path()) + " is unreadable")
				continue
			}
			if err := v.classify(path.Join(owner, sn.Path()), err); err != nil {
				return err
			}
			continue
		}
		if count {
			v.res.Segments++
		}
		if err := v.chunks(ctx, r, seg, count); err != nil {
			return err
		}
	}
	return nil
}

func (v *verifier) chunks(ctx context.Context, r *Reader, seg Segment, count bool) error {
	idx := make([]uint64, 0, len(seg.Chunks))
	for i := range seg.Chunks {
		idx = append(idx, i)
	}
	sort.Slice(idx, func(i, j int) bool { return idx[i] < idx[j] })
	for _, i := range idx {
		ref := seg.Chunks[i]
		if ref.Owner == v.vol {
			v.ownPool[ref.Chunk.Path()] = true
		}
		if !count {
			continue
		}
		v.res.Chunks++
		p := path.Join(ref.Owner, ref.Chunk.Path())
		o, seen := v.owners[ref.Owner]
		if !seen {
			id, ok, err := v.volumeID(ctx, ref.Owner, false)
			if err != nil {
				return err
			}
			o = ownerID{id, ok}
			v.owners[ref.Owner] = o
		}
		if !o.ok || o.id != ref.OwnerID {
			v.find(FindIdentity, p, fmt.Sprintf("names owner %s id %s; its VOLUME has %s", ref.Owner, ref.OwnerID, map[bool]string{true: o.id.String(), false: "none"}[o.ok]))
			continue
		}
		if v.opts.Depth == DepthFull {
			if _, err := r.get(ctx, ref.Owner, ref.Chunk.Path(), ref.Chunk.Hash); err != nil {
				if err := v.classify(p, err); err != nil {
					return err
				}
			}
			continue
		}
		_, ok, err := v.fetch(ctx, path.Join(v.base, p))
		if err != nil {
			return fmt.Errorf("layout: read %s: %w", p, err)
		}
		if !ok {
			v.find(FindMissing, p, "chunk named by a segment is not in the pool")
		}
	}
	return nil
}

// orphans lists the volume's own pool, seg and root names and reports those
// no reachable root names. A dependent's roots reach this volume's chunks
// too; if any dependent cannot be walked, no orphan is reported (a false
// orphan is worse than none).
func (v *verifier) orphans(ctx context.Context, h Head) error {
	deps := map[string]VolumeID{}
	for _, s := range h.Snapshots {
		for _, d := range s.Dependents {
			deps[d.Target] = d.TargetID
		}
	}
	names := make([]string, 0, len(deps))
	for d := range deps {
		names = append(names, d)
	}
	sort.Strings(names)
	for _, d := range names {
		dh, live, err := v.depHead(ctx, d, deps[d])
		if err != nil {
			return err
		}
		if !live {
			continue
		}
		for _, rn := range headRoots(dh) {
			if err := v.root(ctx, d, rn, false); err != nil {
				return err
			}
		}
	}
	if !v.res.Complete {
		return nil
	}
	var found []Finding
	for _, dir := range []string{RootDir, SegDir} {
		names, err := v.opts.List(ctx, path.Join(v.base, v.vol, dir))
		if err != nil {
			return fmt.Errorf("layout: list %s: %w", path.Join(v.vol, dir), err)
		}
		for _, n := range names {
			rel := path.Join(dir, n)
			if bad := parseMeta(dir, n); bad != nil {
				found = append(found, Finding{FindMalformed, path.Join(v.vol, rel), bad.Error()})
			} else if !v.ownMeta[rel] {
				found = append(found, Finding{FindOrphan, path.Join(v.vol, rel), "no reachable root names it"})
			}
		}
	}
	shards, err := v.opts.List(ctx, path.Join(v.base, v.vol, PoolDir))
	if err != nil {
		return fmt.Errorf("layout: list %s: %w", path.Join(v.vol, PoolDir), err)
	}
	for _, sd := range shards {
		names, err := v.opts.List(ctx, path.Join(v.base, v.vol, PoolDir, sd))
		if err != nil {
			return fmt.Errorf("layout: list %s: %w", path.Join(v.vol, PoolDir, sd), err)
		}
		for _, n := range names {
			c, perr := ParseChunkName(n)
			rel := path.Join(PoolDir, sd, n)
			switch {
			case perr != nil:
				found = append(found, Finding{FindMalformed, path.Join(v.vol, rel), perr.Error()})
			case c.Dir() != path.Join(PoolDir, sd):
				found = append(found, Finding{FindMalformed, path.Join(v.vol, rel), "chunk in the wrong pool directory"})
			case !v.ownPool[rel]:
				found = append(found, Finding{FindOrphan, path.Join(v.vol, rel), "no reachable root names it"})
			}
		}
	}
	v.res.OrphansChecked = true
	v.res.Findings = append(v.res.Findings, found...)
	return nil
}

// depHead reads a dependent's HEAD. A target whose VOLUME is gone or has
// another id is no longer a dependent (a volume created again under the
// name matches no dependent): live false, no doubt. A live target whose
// HEAD cannot be read leaves the orphan report undecidable.
func (v *verifier) depHead(ctx context.Context, d string, want VolumeID) (Head, bool, error) {
	if checkOwner(d) != nil {
		return Head{}, false, nil
	}
	id, ok, err := v.volumeID(ctx, d, false)
	if err != nil || !ok || id != want {
		return Head{}, false, err
	}
	b, ok, err := v.fetch(ctx, path.Join(v.base, d, HeadName))
	if err != nil {
		return Head{}, false, fmt.Errorf("layout: read %s: %w", path.Join(d, HeadName), err)
	}
	h, derr := DecodeHead(b, 0, id)
	if !ok || derr != nil {
		v.incomplete("dependent " + d + " has no readable HEAD: orphans not reported")
		return Head{}, false, nil
	}
	return h, true, nil
}

func parseMeta(dir, n string) error {
	if dir == RootDir {
		_, err := ParseRootName(n)
		return err
	}
	_, err := ParseSegName(n)
	return err
}
