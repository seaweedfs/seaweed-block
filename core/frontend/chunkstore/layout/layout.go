// Package layout holds the pure, content-named layout of a volume's store
// objects: chunk names, their pool paths, manifest segments and roots.
//
// Under <base>/<volume>/:
//
//	pool/<index/10000>/<index>-<sha256>   a chunk's bytes
//	seg/<shard>-<sha256>                  the manifest segment of one shard
//	root/<sha256>                         the segment names, one per shard
//
// Every name is derived from the object's content, so a name that exists
// names a complete object and writing it again is a no-op. Nothing here
// touches a store except Present and MissingOwn, which only look names up.
package layout

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
)

// ShardSize is the number of chunk indexes per pool directory and per
// manifest segment.
const ShardSize = 10000

const (
	PoolDir = "pool"
	SegDir  = "seg"
	RootDir = "root"
)

// ErrMalformed reports a name or an encoding that is not canonical.
var ErrMalformed = errors.New("layout: malformed")

// Hash is the SHA-256 of an object's bytes.
type Hash [sha256.Size]byte

func (h Hash) String() string { return hex.EncodeToString(h[:]) }

// Sum returns the SHA-256 of b.
func Sum(b []byte) Hash { return sha256.Sum256(b) }

func parseHash(s string) (Hash, error) {
	var h Hash
	if len(s) != 2*len(h) || strings.ToLower(s) != s {
		return h, fmt.Errorf("%w: hash %q is not 64 lower-case hex digits", ErrMalformed, s)
	}
	if _, err := hex.Decode(h[:], []byte(s)); err != nil {
		return h, fmt.Errorf("%w: hash %q: %v", ErrMalformed, s, err)
	}
	return h, nil
}

// parseIndex accepts only canonical decimal: no sign, no leading zero.
func parseIndex(s string) (uint64, error) {
	if s == "" || (len(s) > 1 && s[0] == '0') || strings.TrimLeft(s, "0123456789") != "" {
		return 0, fmt.Errorf("%w: index %q is not canonical decimal", ErrMalformed, s)
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: index %q: %v", ErrMalformed, s, err)
	}
	return n, nil
}

// Shard returns the shard of a chunk index.
func Shard(index uint64) uint64 { return index / ShardSize }

// ChunkName is one chunk of one volume: its index and the hash of its bytes.
type ChunkName struct {
	Index uint64
	Hash  Hash
}

// NameChunk names the chunk at index holding data.
func NameChunk(index uint64, data []byte) ChunkName {
	return ChunkName{Index: index, Hash: Sum(data)}
}

// String is the file name of the chunk: <index>-<sha256>.
func (c ChunkName) String() string {
	return strconv.FormatUint(c.Index, 10) + "-" + c.Hash.String()
}

// ParseChunkName parses <index>-<sha256>.
func ParseChunkName(s string) (ChunkName, error) {
	i, h, ok := strings.Cut(s, "-")
	if !ok {
		return ChunkName{}, fmt.Errorf("%w: chunk name %q", ErrMalformed, s)
	}
	idx, err := parseIndex(i)
	if err != nil {
		return ChunkName{}, err
	}
	hash, err := parseHash(h)
	if err != nil {
		return ChunkName{}, err
	}
	return ChunkName{Index: idx, Hash: hash}, nil
}

// Dir is the chunk's pool directory relative to the volume root:
// pool/<index/10000>.
func (c ChunkName) Dir() string {
	return path.Join(PoolDir, strconv.FormatUint(Shard(c.Index), 10))
}

// Path is the chunk's path relative to the volume root.
func (c ChunkName) Path() string { return path.Join(c.Dir(), c.String()) }

// Verify checks that data is the content c names.
func (c ChunkName) Verify(data []byte) error {
	if got := Sum(data); got != c.Hash {
		return fmt.Errorf("layout: chunk %d: content hash %s does not match its name %s", c.Index, got, c.Hash)
	}
	return nil
}

// Ref is one entry of a manifest: the chunk at an index, held in the pool
// of Owner (the volume itself, or the source volume of a restore).
// OwnerID is that volume's id: a volume created again under Owner's name
// has another id and satisfies no reference to the old one.
type Ref struct {
	Owner   string
	OwnerID VolumeID
	Chunk   ChunkName
}

// CheckOwner refuses an owner name that is not a plain path element
// (ErrMalformed), as every reference's owner must be.
func CheckOwner(o string) error { return checkOwner(o) }

// checkOwner enforces a plain path element, as volume ids are.
func checkOwner(o string) error {
	if o == "" || o == "." || o == ".." || strings.ContainsAny(o, "/\\") {
		return fmt.Errorf("%w: owner %q is not a plain path element", ErrMalformed, o)
	}
	return nil
}

// Segment is the manifest of one shard: the chunks present in it. An index
// with no entry reads as zeros.
type Segment struct {
	Shard  uint64
	Chunks map[uint64]Ref // by chunk index
}

var segMagic = []byte("SWBSEG\x02")
var rootMagic = []byte("SWBROOT\x01")

// Encode returns the canonical bytes of s: entries in index order. It
// refuses an empty segment (a shard with no chunk is left out of the root)
// and an entry outside the shard or keyed by another index than its chunk.
func (s Segment) Encode() ([]byte, error) {
	if len(s.Chunks) == 0 {
		return nil, fmt.Errorf("%w: segment of shard %d is empty", ErrMalformed, s.Shard)
	}
	idx := make([]uint64, 0, len(s.Chunks))
	for i, r := range s.Chunks {
		if r.Chunk.Index != i {
			return nil, fmt.Errorf("%w: segment entry %d names chunk %d", ErrMalformed, i, r.Chunk.Index)
		}
		if Shard(i) != s.Shard {
			return nil, fmt.Errorf("%w: chunk %d is not in shard %d", ErrMalformed, i, s.Shard)
		}
		if err := checkOwner(r.Owner); err != nil {
			return nil, err
		}
		if r.OwnerID.IsZero() {
			return nil, fmt.Errorf("%w: chunk %d names owner %q without its volume id", ErrMalformed, i, r.Owner)
		}
		idx = append(idx, i)
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
	var b bytes.Buffer
	b.Write(segMagic)
	b.Write(binary.AppendUvarint(nil, s.Shard))
	b.Write(binary.AppendUvarint(nil, uint64(len(idx))))
	for _, i := range idx {
		r := s.Chunks[i]
		b.Write(binary.AppendUvarint(nil, i))
		b.Write(binary.AppendUvarint(nil, uint64(len(r.Owner))))
		b.WriteString(r.Owner)
		b.Write(r.OwnerID[:])
		b.Write(r.Chunk.Hash[:])
	}
	return b.Bytes(), nil
}

// reader decodes uvarints and fixed fields, failing on a short input.
type reader struct {
	b   []byte
	err error
}

func (r *reader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = fmt.Errorf("%w: bad varint", ErrMalformed)
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) bytes(n uint64) []byte {
	if r.err != nil {
		return nil
	}
	if uint64(len(r.b)) < n {
		r.err = fmt.Errorf("%w: truncated", ErrMalformed)
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

// DecodeSegment parses canonical segment bytes. Bytes that decode but are
// not what Encode would write (unsorted, duplicate, non-minimal varints,
// trailing bytes) are refused, so a segment has exactly one encoding and
// therefore exactly one name.
func DecodeSegment(b []byte) (Segment, error) {
	if !bytes.HasPrefix(b, segMagic) {
		return Segment{}, fmt.Errorf("%w: not a segment", ErrMalformed)
	}
	r := &reader{b: b[len(segMagic):]}
	s := Segment{Shard: r.uvarint(), Chunks: map[uint64]Ref{}}
	n := r.uvarint()
	if r.err == nil && n > ShardSize {
		return Segment{}, fmt.Errorf("%w: segment claims %d entries", ErrMalformed, n)
	}
	for k := uint64(0); k < n && r.err == nil; k++ {
		i := r.uvarint()
		owner := string(r.bytes(r.uvarint()))
		var oid VolumeID
		copy(oid[:], r.bytes(uint64(len(oid))))
		var h Hash
		copy(h[:], r.bytes(uint64(len(h))))
		if r.err == nil {
			if _, dup := s.Chunks[i]; dup {
				return Segment{}, fmt.Errorf("%w: chunk %d twice", ErrMalformed, i)
			}
			s.Chunks[i] = Ref{Owner: owner, OwnerID: oid, Chunk: ChunkName{Index: i, Hash: h}}
		}
	}
	if r.err != nil {
		return Segment{}, r.err
	}
	again, err := s.Encode()
	if err != nil {
		return Segment{}, err
	}
	if !bytes.Equal(again, b) {
		return Segment{}, fmt.Errorf("%w: segment bytes are not canonical", ErrMalformed)
	}
	return s, nil
}

// SegName names one shard's segment: <shard>-<sha256 of its bytes>.
type SegName struct {
	Shard uint64
	Hash  Hash
}

func (n SegName) String() string {
	return strconv.FormatUint(n.Shard, 10) + "-" + n.Hash.String()
}

// Path is the segment's path relative to the volume root.
func (n SegName) Path() string { return path.Join(SegDir, n.String()) }

// ParseSegName parses <shard>-<sha256>.
func ParseSegName(s string) (SegName, error) {
	c, err := ParseChunkName(s) // same shape
	if err != nil {
		return SegName{}, fmt.Errorf("segment name: %w", err)
	}
	return SegName{Shard: c.Index, Hash: c.Hash}, nil
}

// NameSegment returns the name and bytes of s.
func NameSegment(s Segment) (SegName, []byte, error) {
	b, err := s.Encode()
	if err != nil {
		return SegName{}, nil, err
	}
	return SegName{Shard: s.Shard, Hash: Sum(b)}, b, nil
}

// Root is the image at one moment: the segment of every shard that holds a
// chunk. A shard without a segment reads as zeros.
type Root struct {
	Segments map[uint64]SegName // by shard
}

// Encode returns the canonical bytes of r: segments in shard order. An
// empty root (an all-zero image) is valid.
func (r Root) Encode() ([]byte, error) {
	shards := make([]uint64, 0, len(r.Segments))
	for sh, n := range r.Segments {
		if n.Shard != sh {
			return nil, fmt.Errorf("%w: root entry %d names segment of shard %d", ErrMalformed, sh, n.Shard)
		}
		shards = append(shards, sh)
	}
	sort.Slice(shards, func(a, b int) bool { return shards[a] < shards[b] })
	var b bytes.Buffer
	b.Write(rootMagic)
	b.Write(binary.AppendUvarint(nil, uint64(len(shards))))
	for _, sh := range shards {
		b.Write(binary.AppendUvarint(nil, sh))
		h := r.Segments[sh].Hash
		b.Write(h[:])
	}
	return b.Bytes(), nil
}

// maxRootShards bounds a decoded root: one entry needs at least 33 bytes.
func maxRootShards(n int) uint64 { return uint64(n / (1 + sha256.Size)) }

// DecodeRoot parses canonical root bytes; non-canonical bytes are refused.
func DecodeRoot(b []byte) (Root, error) {
	if !bytes.HasPrefix(b, rootMagic) {
		return Root{}, fmt.Errorf("%w: not a root", ErrMalformed)
	}
	r := &reader{b: b[len(rootMagic):]}
	out := Root{Segments: map[uint64]SegName{}}
	n := r.uvarint()
	if r.err == nil && n > maxRootShards(len(r.b)) {
		return Root{}, fmt.Errorf("%w: root claims %d shards", ErrMalformed, n)
	}
	for k := uint64(0); k < n && r.err == nil; k++ {
		sh := r.uvarint()
		var h Hash
		copy(h[:], r.bytes(uint64(len(h))))
		if r.err == nil {
			if _, dup := out.Segments[sh]; dup {
				return Root{}, fmt.Errorf("%w: shard %d twice", ErrMalformed, sh)
			}
			out.Segments[sh] = SegName{Shard: sh, Hash: h}
		}
	}
	if r.err != nil {
		return Root{}, r.err
	}
	again, err := out.Encode()
	if err != nil {
		return Root{}, err
	}
	if !bytes.Equal(again, b) {
		return Root{}, fmt.Errorf("%w: root bytes are not canonical", ErrMalformed)
	}
	return out, nil
}

// RootName is the name of a root: the SHA-256 of its bytes.
type RootName struct{ Hash Hash }

func (n RootName) String() string { return n.Hash.String() }

// Path is the root's path relative to the volume root.
func (n RootName) Path() string { return path.Join(RootDir, n.String()) }

// ParseRootName parses a root file name.
func ParseRootName(s string) (RootName, error) {
	h, err := parseHash(s)
	if err != nil {
		return RootName{}, fmt.Errorf("root name: %w", err)
	}
	return RootName{Hash: h}, nil
}

// NameRoot returns the name and bytes of r.
func NameRoot(r Root) (RootName, []byte, error) {
	b, err := r.Encode()
	if err != nil {
		return RootName{}, nil, err
	}
	return RootName{Hash: Sum(b)}, b, nil
}

// Build splits a manifest (chunk index -> ref) into one segment per shard
// and the root naming them. It returns the root and every segment with its
// bytes, keyed by name, for the caller to write before the root.
func Build(manifest map[uint64]Ref) (Root, map[SegName][]byte, error) {
	shards := map[uint64]Segment{}
	for i, r := range manifest {
		sh := Shard(i)
		s, ok := shards[sh]
		if !ok {
			s = Segment{Shard: sh, Chunks: map[uint64]Ref{}}
			shards[sh] = s
		}
		s.Chunks[i] = r
	}
	root := Root{Segments: map[uint64]SegName{}}
	segs := map[SegName][]byte{}
	for sh, s := range shards {
		n, b, err := NameSegment(s)
		if err != nil {
			return Root{}, nil, err
		}
		root.Segments[sh] = n
		segs[n] = b
	}
	return root, segs, nil
}

// Lookup reports whether the entry name exists in dir. A missing entry is
// (false, nil); any other failure is an error, never "absent".
type Lookup func(ctx context.Context, dir, name string) (bool, error)

// Present reports whether the object at rel (a Path of this package) exists
// under the volume root. An object exists only once it is complete (data
// first, file entry after), so true means complete.
func Present(ctx context.Context, look Lookup, volRoot, rel string) (bool, error) {
	full := path.Join(volRoot, rel)
	return look(ctx, path.Dir(full), path.Base(full))
}

// MissingOwn returns, in index order, the chunks of manifest held by vol's
// own pool that are not present there: the names a commit naming manifest
// would leave dangling. Chunks of another owner are not looked up (the
// owner's pool holds them).
func MissingOwn(ctx context.Context, look Lookup, volRoot, vol string, manifest map[uint64]Ref) ([]ChunkName, error) {
	idx := make([]uint64, 0, len(manifest))
	for i, r := range manifest {
		if r.Owner == vol {
			idx = append(idx, i)
		}
	}
	sort.Slice(idx, func(a, b int) bool { return idx[a] < idx[b] })
	var missing []ChunkName
	for _, i := range idx {
		c := manifest[i].Chunk
		ok, err := Present(ctx, look, volRoot, c.Path())
		if err != nil {
			return nil, fmt.Errorf("layout: look up chunk %s: %w", c, err)
		}
		if !ok {
			missing = append(missing, c)
		}
	}
	return missing, nil
}
