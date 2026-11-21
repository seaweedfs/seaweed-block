package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// restoreIndependentName is the write-once entry that ends a restore's
// lineage: written only when the restored volume's HEAD holds no qualified
// ref, so from then on nothing of the volume reaches its source's chunks.
const (
	restoreIndependentName = "RESTORE-INDEPENDENT"
	kindRestoreIndependent = "chunkrestoreindependent"
)

// Restore error codes.
const (
	// CodeNotRestore: the volume was not made by Restore.
	CodeNotRestore = "not_restore"
	// CodeRestoreIncomplete: HEAD still holds qualified refs (or no HEAD).
	CodeRestoreIncomplete = "restore_incomplete"
)

// RestoreIndependence is the content of restoreIndependentName.
type RestoreIndependence struct {
	Format   int    `json:"format"`
	VolumeID string `json:"volume_id"`
	HeadGen  uint64 `json:"head_gen"`
	MarkedNs int64  `json:"marked_ns"`
}

// ForeignRefs counts the refs of vol's HEAD h that name ANOTHER volume's
// chunks or segments: a "@<owner>/" key whose owner is not vol. A resolved
// HEAD writes every chunk key in owner-qualified form, vol's own included,
// so a bare "@" prefix does not make a ref foreign; an unqualified key is
// always vol's own.
func ForeignRefs(h storage.ChunkHead, vol string) int {
	foreign := func(key string) bool {
		if !strings.HasPrefix(key, "@") {
			return false
		}
		owner, _, ok := strings.Cut(key[1:], "/")
		return !ok || owner != vol
	}
	n := 0
	for _, ref := range h.Chunks {
		if foreign(ref.Key) {
			n++
		}
	}
	for _, seg := range h.Segments {
		if foreign(seg.Key) {
			n++
		}
	}
	return n
}

// MarkIndependent records that restored volume vol no longer depends on its
// source: refused unless vol was made by Restore and its current HEAD holds
// no qualified ref. Write-once: an existing marker is a replay (ok).
func (c *Catalog) MarkIndependent(ctx context.Context, vol string) (RestoreIndependence, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return RestoreIndependence{}, err
	}
	if m, ok, err := c.ReadIndependence(ctx, vol); err != nil || ok {
		return m, err
	}
	v, ok, err := c.ReadVolume(ctx, vol)
	if err != nil {
		return RestoreIndependence{}, err
	}
	if !ok || v.Origin != OriginRestore {
		return RestoreIndependence{}, catErr(CodeNotRestore, false, nil, "volume %s was not made by restore", vol)
	}
	h, has, err := c.ReadHead(ctx, vol)
	if err != nil {
		return RestoreIndependence{}, err
	}
	if !has || h.State == storage.ChunkHeadAborted || headDeletingOrDeleted(h) {
		return RestoreIndependence{}, catErr(CodeRestoreIncomplete, false, nil, "volume %s has no live HEAD", vol)
	}
	// The manifest segments name chunk refs too: check the resolved HEAD (every
	// segment key and every chunk ref inside), as GC and usage resolve it.
	if c.blobs == nil {
		return RestoreIndependence{}, errors.New("chunkstore: restore marker needs the segment reader")
	}
	r, err := NewChunkRemote(c.f, c.blobs, path.Join(c.base, vol))
	if err != nil {
		return RestoreIndependence{}, err
	}
	resolved, err := r.resolveManifest(ctx, h)
	if err != nil {
		return RestoreIndependence{}, remoteErr(err, "resolve HEAD of %s", vol)
	}
	if q := ForeignRefs(resolved, vol); q > 0 {
		return RestoreIndependence{}, catErr(CodeRestoreIncomplete, false, nil, "volume %s: HEAD gen %d still holds %d refs to another volume", vol, h.Gen, q)
	}
	m := RestoreIndependence{Format: volumeFormat, VolumeID: vol, HeadGen: h.Gen, MarkedNs: c.now().UnixNano()}
	body, err := json.Marshal(m)
	if err != nil {
		return RestoreIndependence{}, err
	}
	now := time.Now().Unix()
	e := &filerpb.Entry{
		Name:       restoreIndependentName,
		Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body)), Mtime: now, Crtime: now, FileMode: 0o644, Mime: "application/json"},
		Extended:   map[string][]byte{extKind: []byte(kindRestoreIndependent)},
		Content:    body,
	}
	if err := c.f.CreateEntry(ctx, root, e, true); err != nil {
		if !errors.Is(err, ErrExists) {
			return RestoreIndependence{}, remoteErr(err, "create %s of %s", restoreIndependentName, vol)
		}
		m, ok, err := c.ReadIndependence(ctx, vol)
		if err == nil && !ok {
			err = remoteErr(nil, "re-read %s of %s", restoreIndependentName, vol)
		}
		return m, err
	}
	return m, nil
}

// ReadIndependence returns vol's independence marker; ok is false when the
// restore has not ended (or vol is not a restore).
func (c *Catalog) ReadIndependence(ctx context.Context, vol string) (RestoreIndependence, bool, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return RestoreIndependence{}, false, err
	}
	e, err := c.f.LookupEntry(ctx, root, restoreIndependentName)
	if errors.Is(err, ErrNotFound) {
		return RestoreIndependence{}, false, nil
	}
	if err != nil {
		return RestoreIndependence{}, false, remoteErr(err, "read %s of %s", restoreIndependentName, vol)
	}
	if k := string(e.Extended[extKind]); k != kindRestoreIndependent {
		return RestoreIndependence{}, false, remoteErr(nil, "%s of %s: kind %q", restoreIndependentName, vol, k)
	}
	var m RestoreIndependence
	if err := json.Unmarshal(e.GetContent(), &m); err != nil || m.VolumeID != vol {
		return RestoreIndependence{}, false, remoteErr(err, "decode %s of %s", restoreIndependentName, vol)
	}
	return m, true, nil
}
