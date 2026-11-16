package chunkstore

import (
	"context"
	"errors"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Space accounting (claude05's rulings, 2026-11-15 22:53Z). Everything is
// computed from manifests and filer listings; segment objects are verified,
// but data chunks are not read.
//
// A chunk's bytes are ChunkBlocks x BlockSize, the last chunk clipped to
// NumBlocks (ChunkRef carries no size).
//
//   - LiveBytes: the chunks HEAD references.
//   - per snapshot, ExclusiveBytes: the chunks that snapshot references
//     and no other holder does, where the holders are this volume's HEAD
//     and live snapshots AND every other volume's HEAD and live snapshots
//     (a fork's "@owner/key" references are resolved to the owner's
//     chunk). These are the bytes deleting that snapshot makes eligible
//     for GC; GC still keeps them through its grace period and while any
//     in-flight intent names them, so they are not reclaimed at once.
//   - StoredBytes: the volume's chunk-key bindings in the filer (what is
//     stored now, referenced or not). A binding a manifest references is
//     counted at its exact (clipped) size; any other binding at the full
//     chunk size, so StoredBytes is an upper bound whenever
//     StoredUpperBound is set.
//
// "Bytes uploaded since creation" needs a counter nothing records today (a
// follow-up). The unreferenced-not-yet-collected bytes come from GC's
// preview once that lands (#140).

// SnapshotUsage is one snapshot's share of a volume's space.
type SnapshotUsage struct {
	Name           string `json:"name"`
	CreatedNs      int64  `json:"createdNs"`
	Class          string `json:"class"`
	Pinned         bool   `json:"pinned"`
	Chunks         int    `json:"chunks"`
	ExclusiveBytes uint64 `json:"exclusiveBytes"`
}

// VolumeUsage is a volume's space accounting.
type VolumeUsage struct {
	VolumeID         string          `json:"volumeID"`
	ChunkBytes       uint64          `json:"chunkBytes"` // one full chunk
	LiveChunks       int             `json:"liveChunks"`
	LiveBytes        uint64          `json:"liveBytes"`
	Snapshots        []SnapshotUsage `json:"snapshots"`
	StoredBindings   int             `json:"storedBindings"`
	StoredBytes      uint64          `json:"storedBytes"`
	StoredUpperBound bool            `json:"storedUpperBound"`
	Policy           RetentionPolicy `json:"policy"`
	PolicySet        bool            `json:"policySet"`
}

// chunkSize is the byte size of chunk index idx under g.
func chunkSize(g Geometry, idx uint64) uint64 {
	full := uint64(g.ChunkBlocks) * uint64(g.BlockSize)
	if g.ChunkBlocks == 0 {
		return 0
	}
	first := idx * uint64(g.ChunkBlocks)
	if first >= g.NumBlocks {
		return 0
	}
	if rest := g.NumBlocks - first; rest < uint64(g.ChunkBlocks) {
		return rest * uint64(g.BlockSize)
	}
	return full
}

// Usage computes vol's space accounting.
func (c *Catalog) Usage(ctx context.Context, vol string) (VolumeUsage, error) {
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return VolumeUsage{}, err
	}
	if !ok {
		return VolumeUsage{}, catErr(CodeNotChunkVolume, false, nil, "volume %s has no VOLUME entry", vol)
	}
	g := v.Geometry
	u := VolumeUsage{VolumeID: vol, ChunkBytes: uint64(g.ChunkBlocks) * uint64(g.BlockSize), Snapshots: []SnapshotUsage{}}
	if u.Policy, u.PolicySet, err = c.ReadRetentionPolicy(ctx, vol); err != nil {
		return VolumeUsage{}, err
	}

	head, _, err := c.ReadHead(ctx, vol)
	if err != nil {
		return VolumeUsage{}, err
	}
	snaps, err := c.allSnapshots(ctx, vol)
	if err != nil {
		return VolumeUsage{}, err
	}

	head, err = c.usageHead(ctx, vol, head)
	if err != nil {
		return VolumeUsage{}, err
	}
	for i := range snaps {
		snaps[i].head, err = c.usageHead(ctx, vol, snaps[i].head)
		if err != nil {
			return VolumeUsage{}, err
		}
	}
	// holders counts, per chunk identity (owner volume + key), how many
	// HEADs and live snapshots of any volume reference it; size is each
	// identity's byte size as this volume's geometry indexes it.
	holders := map[string]int{}
	size := map[string]uint64{}
	addHolder := func(owner string, h storage.ChunkHead, sized bool) {
		seen := map[string]bool{}
		for idx, ref := range h.Chunks {
			id := chunkIdentity(owner, ref.Key)
			if sized {
				size[id] = chunkSize(g, idx)
			}
			if !seen[id] {
				seen[id] = true
				holders[id]++
			}
		}
	}
	addHolder(vol, head, true)
	for idx := range head.Chunks {
		u.LiveChunks++
		u.LiveBytes += chunkSize(g, idx)
	}
	for _, s := range snaps {
		addHolder(vol, s.Head(), true)
	}
	others, err := c.ListVolumes(ctx)
	if err != nil {
		return VolumeUsage{}, err
	}
	for _, other := range others {
		if other == vol {
			continue
		}
		oh, has, err := c.ReadHead(ctx, other)
		if err != nil {
			return VolumeUsage{}, err
		}
		if has {
			oh, err = c.usageHead(ctx, other, oh)
			if err != nil {
				return VolumeUsage{}, err
			}
			addHolder(other, oh, false)
		}
		osnaps, err := c.allSnapshots(ctx, other)
		if err != nil {
			return VolumeUsage{}, err
		}
		for _, s := range osnaps {
			otherHead, err := c.usageHead(ctx, other, s.Head())
			if err != nil {
				return VolumeUsage{}, err
			}
			addHolder(other, otherHead, false)
		}
	}
	for _, s := range snaps {
		su := SnapshotUsage{Name: s.Name, CreatedNs: s.CreatedNs, Class: s.Class, Pinned: s.Pinned(), Chunks: len(s.Head().Chunks)}
		seen := map[string]bool{}
		for _, ref := range s.Head().Chunks {
			id := chunkIdentity(vol, ref.Key)
			if seen[id] {
				continue
			}
			seen[id] = true
			if holders[id] == 1 {
				su.ExclusiveBytes += size[id]
			}
		}
		u.Snapshots = append(u.Snapshots, su)
	}
	sort.Slice(u.Snapshots, func(i, j int) bool {
		if u.Snapshots[i].CreatedNs != u.Snapshots[j].CreatedNs {
			return u.Snapshots[i].CreatedNs > u.Snapshots[j].CreatedNs
		}
		return u.Snapshots[i].Name < u.Snapshots[j].Name
	})

	// Stored: this volume's own chunk-key bindings.
	r, err := NewChunkRemote(c.f, nilBlobs{}, path.Join(c.base, vol))
	if err != nil {
		return VolumeUsage{}, err
	}
	after := ""
	for {
		page, err := c.f.ListEntries(ctx, r.keyDir(), "", after, listPage)
		if err != nil {
			return VolumeUsage{}, remoteErr(err, "list the chunk keys of %s", vol)
		}
		for _, e := range page {
			if e.GetName() <= after {
				return VolumeUsage{}, errors.New("chunkstore: non-advancing chunk key listing")
			}
			after = e.GetName()
			if e.GetIsDirectory() || string(e.Extended[extKind]) != kindChunkKey {
				continue
			}
			key, err := url.PathUnescape(e.GetName())
			if err != nil || strings.HasPrefix(key, "@") {
				continue
			}
			u.StoredBindings++
			if n, ok := size[chunkIdentity(vol, key)]; ok {
				u.StoredBytes += n
			} else {
				u.StoredBytes += u.ChunkBytes
				u.StoredUpperBound = true
			}
		}
		if len(page) < listPage {
			return u, nil
		}
	}
}

// chunkIdentity names the chunk a ref in volume vol points at: an
// owner-qualified "@owner/key" (a fork's reference) is owner's key,
// anything else is vol's.
func chunkIdentity(vol, key string) string {
	if strings.HasPrefix(key, "@") {
		if owner, rest, ok := strings.Cut(key[1:], "/"); ok {
			return owner + "\x00" + rest
		}
	}
	return vol + "\x00" + key
}

func (c *Catalog) usageHead(ctx context.Context, vol string, h storage.ChunkHead) (storage.ChunkHead, error) {
	if h.Segments == nil {
		return h, nil
	}
	var blobs Blobs = c.blobs
	if blobs == nil {
		return h, errors.New("chunkstore: usage segment reader unavailable")
	}
	r, err := NewChunkRemote(c.f, blobs, path.Join(c.base, vol))
	if err != nil {
		return h, err
	}
	resolved, err := r.resolveManifest(ctx, h)
	if err != nil {
		return h, err
	}
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return h, err
	}
	if !ok || h.ChunkBytes != uint64(v.Geometry.ChunkBlocks)*uint64(v.Geometry.BlockSize) {
		return h, errors.New("chunkstore: usage manifest/volume geometry mismatch")
	}
	for idx := range resolved.Chunks {
		if idx >= (v.Geometry.NumBlocks+uint64(v.Geometry.ChunkBlocks)-1)/uint64(v.Geometry.ChunkBlocks) {
			return h, errors.New("chunkstore: usage chunk outside volume")
		}
	}
	return resolved, nil
}
