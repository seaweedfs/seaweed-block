package chunkstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// layoutKeyPrefix marks an engine chunk key that is a layout reference.
const layoutKeyPrefix = "layout:"

// LayoutKey is the engine's chunk key of a layout reference:
// "layout:<owner>:<owner id hex>:<chunk name>". The key alone names the
// owner's directory, so a restore target's key reads its source's pool.
func LayoutKey(ref layout.Ref) string {
	return layoutKeyPrefix + ref.Owner + ":" + ref.OwnerID.String() + ":" + ref.Chunk.String()
}

// ParseLayoutKey is LayoutKey's inverse. The owner id and the chunk name
// hold no ':', so the key is split from the right; anything that does not
// round-trip exactly is ErrMalformed.
func ParseLayoutKey(key string) (layout.Ref, error) {
	bad := func(why string) (layout.Ref, error) {
		return layout.Ref{}, fmt.Errorf("%w: chunk key %q: %s", layout.ErrMalformed, key, why)
	}
	rest, ok := strings.CutPrefix(key, layoutKeyPrefix)
	if !ok {
		return bad("not a layout key")
	}
	i := strings.LastIndexByte(rest, ':')
	if i < 0 {
		return bad("no chunk name")
	}
	chunk, err := layout.ParseChunkName(rest[i+1:])
	if err != nil {
		return bad(err.Error())
	}
	rest = rest[:i]
	j := strings.LastIndexByte(rest, ':')
	if j <= 0 {
		return bad("no owner")
	}
	id, err := layout.ParseVolumeID(rest[j+1:])
	if err != nil {
		return bad(err.Error())
	}
	ref := layout.Ref{Owner: rest[:j], OwnerID: id, Chunk: chunk}
	if err := layout.CheckOwner(ref.Owner); err != nil {
		return bad(err.Error())
	}
	if id.IsZero() {
		return bad("zero owner id")
	}
	if LayoutKey(ref) != key {
		return bad("not canonical")
	}
	return ref, nil
}

// LayoutManifest is HEAD's current image as the engine's manifest: every
// chunk the current root names, own or a restore source's, keyed by
// LayoutKey, with the name's hash (the stored bytes are the logical bytes:
// encoding full) and BirthLSN = HEAD's position (nothing reads it). A HEAD
// with no current image has an empty manifest. A root or segment that
// cannot be read is an error, never a partial manifest.
func LayoutManifest(ctx context.Context, r *layout.Reader, h layout.Head) (map[uint64]storage.ChunkRef, error) {
	out := map[uint64]storage.ChunkRef{}
	if h.Current == nil {
		return out, nil
	}
	img, err := r.Open(ctx, *h.Current)
	if err != nil {
		return nil, err
	}
	for _, sh := range img.Shards() {
		seg, ok, err := img.Segment(ctx, sh)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		for idx, ref := range seg.Chunks {
			out[idx] = storage.ChunkRef{Key: LayoutKey(ref), SHA256: ref.Chunk.Hash, Encoding: "full", BirthLSN: h.Position}
		}
	}
	return out, nil
}
