package layout

import (
	"context"
	"errors"
	"fmt"
	"path"
)

// ErrMissing reports an object a root reaches that is not in the store: a
// root, a segment or a chunk. It is never read as zeros.
var ErrMissing = errors.New("layout: object missing")

// ErrCorrupt reports an object whose bytes do not match its name.
var ErrCorrupt = errors.New("layout: object corrupt")

// ErrIdentity reports a reference to a volume whose VOLUME record carries
// another id: the volume was deleted and created again under the name.
var ErrIdentity = errors.New("layout: volume id does not match the reference")

// Fetch reads the whole object at p. A missing object is (nil, false, nil);
// any other failure is an error.
type Fetch func(ctx context.Context, p string) (data []byte, found bool, err error)

// Reader resolves images of one volume: its roots and segments are read
// from <base>/<volume>/, each chunk from the pool of the volume its root
// entry names.
type Reader struct {
	base, vol string
	fetch     Fetch
	ids       map[string]VolumeID // VOLUME ids read so far, by volume name
}

// NewReader returns a Reader of vol under base.
func NewReader(base, vol string, fetch Fetch) (*Reader, error) {
	if err := checkOwner(vol); err != nil {
		return nil, err
	}
	if fetch == nil {
		return nil, errors.New("layout: reader needs a fetch")
	}
	return &Reader{base: base, vol: vol, fetch: fetch, ids: map[string]VolumeID{}}, nil
}

// VolumeID returns the id in the VOLUME record of the named volume, read
// once per Reader. A missing VOLUME is ErrMissing.
func (r *Reader) VolumeID(ctx context.Context, vol string) (VolumeID, error) {
	if id, ok := r.ids[vol]; ok {
		return id, nil
	}
	p := path.Join(r.base, vol, VolumeName)
	b, ok, err := r.fetch(ctx, p)
	if err != nil {
		return VolumeID{}, fmt.Errorf("layout: read %s: %w", p, err)
	}
	if !ok {
		return VolumeID{}, fmt.Errorf("%w: %s", ErrMissing, p)
	}
	v, err := DecodeVolume(b)
	if err != nil {
		return VolumeID{}, fmt.Errorf("%w: %s: %v", ErrCorrupt, p, err)
	}
	r.ids[vol] = v.ID
	return v.ID, nil
}

// get fetches the object at rel under volume vol and checks its bytes hash
// to want.
func (r *Reader) get(ctx context.Context, vol, rel string, want Hash) ([]byte, error) {
	p := path.Join(r.base, vol, rel)
	b, ok, err := r.fetch(ctx, p)
	if err != nil {
		return nil, fmt.Errorf("layout: read %s: %w", p, err)
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrMissing, p)
	}
	if got := Sum(b); got != want {
		return nil, fmt.Errorf("%w: %s hashes to %s", ErrCorrupt, p, got)
	}
	return b, nil
}

// Image is one root of the volume, opened.
type Image struct {
	r    *Reader
	root Root
}

// Open reads and checks the root rn.
func (r *Reader) Open(ctx context.Context, rn RootName) (*Image, error) {
	b, err := r.get(ctx, r.vol, rn.Path(), rn.Hash)
	if err != nil {
		return nil, err
	}
	root, err := DecodeRoot(b)
	if err != nil {
		return nil, fmt.Errorf("%w: root %s: %v", ErrCorrupt, rn, err)
	}
	return &Image{r: r, root: root}, nil
}

// Segment reads the segment of shard; ok is false when the root names no
// segment for it (every index of the shard reads as zeros).
func (img *Image) Segment(ctx context.Context, shard uint64) (seg Segment, ok bool, err error) {
	n, ok := img.root.Segments[shard]
	if !ok {
		return Segment{}, false, nil
	}
	b, err := img.r.get(ctx, img.r.vol, n.Path(), n.Hash)
	if err != nil {
		return Segment{}, false, err
	}
	seg, err = DecodeSegment(b)
	if err != nil {
		return Segment{}, false, fmt.Errorf("%w: segment %s: %v", ErrCorrupt, n, err)
	}
	if seg.Shard != shard {
		return Segment{}, false, fmt.Errorf("%w: segment %s holds shard %d", ErrCorrupt, n, seg.Shard)
	}
	return seg, true, nil
}

// Chunk is a resolved chunk: zeros, or the bytes of Ref.
type Chunk struct {
	Zero bool
	Ref  Ref
	Data []byte
}

// Resolve finds the entry of index: root -> segment -> ref. An index the
// root does not name is zeros; a segment the root names but the store lacks
// is ErrMissing.
func (img *Image) Resolve(ctx context.Context, index uint64) (ref Ref, zero bool, err error) {
	seg, ok, err := img.Segment(ctx, Shard(index))
	if err != nil {
		return Ref{}, false, err
	}
	if !ok {
		return Ref{}, true, nil
	}
	ref, ok = seg.Chunks[index]
	if !ok {
		return Ref{}, true, nil
	}
	return ref, false, nil
}

// Chunk resolves index and reads its bytes from the pool of the volume
// that owns it, in that volume's directory (VolumeDir of the reference's
// owner name and id), after checking the directory's VOLUME id is the one
// the reference carries (ErrIdentity otherwise). A chunk the root names but the
// owner's pool lacks is ErrMissing.
func (img *Image) Chunk(ctx context.Context, index uint64) (Chunk, error) {
	ref, zero, err := img.Resolve(ctx, index)
	if err != nil {
		return Chunk{}, err
	}
	if zero {
		return Chunk{Zero: true}, nil
	}
	dir := VolumeDir(ref.Owner, ref.OwnerID)
	id, err := img.r.VolumeID(ctx, dir)
	if err != nil {
		return Chunk{}, err
	}
	if id != ref.OwnerID {
		return Chunk{}, fmt.Errorf("%w: chunk %d names %s id %s, VOLUME has %s", ErrIdentity, index, ref.Owner, ref.OwnerID, id)
	}
	b, err := img.r.get(ctx, dir, ref.Chunk.Path(), ref.Chunk.Hash)
	if err != nil {
		return Chunk{}, err
	}
	return Chunk{Ref: ref, Data: b}, nil
}
