package chunkstore

// ChunkRemote: the walstore chunk layer's remote (storage.ChunkRemote) on
// SeaweedFS, over the Filer and Blobs interfaces above.
//
// Layout under one volume's root directory:
//
//	<root>/HEAD         the published HEAD: JSON in the entry content,
//	                    {gen, epoch, holder} mirrored in extended attributes
//	<root>/keys/<key>   one write-once entry per chunk key, naming the
//	                    needle (fid) that holds the chunk's bytes
//
// The chunk layer names chunks itself (immutable keys) while SeaweedFS
// assigns needle ids, so a key entry binds the two. It is created with
// o_excl: a key that exists is never rebound, which is the "keys are
// immutable" rule of storage.ChunkRemote.
//
// The HEAD CAS is the filer's conditional update: UpdateEntry with
// expected_extended = {swb.gen, swb.epoch} of the HEAD the caller read.
// The first HEAD is an exclusive create. Only a definite refusal maps to
// storage.ErrChunkHeadPrecondition; every other error is returned as is,
// so the chunk layer treats the outcome as unknown and re-reads HEAD.
//
// No entry ever carries filer chunks: UpdateEntry deletes chunks the new
// entry drops, so needle ids live only in entry content and extended
// attributes, which the filer does not interpret.
//
// Not implemented: reclaiming needles. Delete reports ErrDeleteUnsupported;
// superseded chunks stay stored (the chunk layer never calls Delete).

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Extended attribute keys; the same names the D1 manifest entries use.
const (
	extGen    = "swb.gen"
	extEpoch  = "swb.epoch"
	extHolder = "swb.holder"
	extKind   = "swb.kind"
	extFid    = "swb.fid"

	kindChunkHead = "chunkhead"
	kindChunkKey  = "chunkkey"

	headName = "HEAD"
	keysDir  = "keys"

	// headFormat is the only HEAD document format this code reads.
	headFormat = 1
)

// ErrDeleteUnsupported: this remote does not reclaim chunks.
var ErrDeleteUnsupported = errors.New("chunkstore: chunk delete not supported")

// ChunkRemote implements storage.ChunkRemote for one volume.
type ChunkRemote struct {
	f    Filer
	b    Blobs
	root string

	mu   sync.Mutex
	fids map[string]string // key -> fid; key entries are write-once
}

// NewChunkRemote returns the remote for the volume rooted at root, an
// absolute filer directory owned by that volume alone.
func NewChunkRemote(f Filer, b Blobs, root string) (*ChunkRemote, error) {
	if f == nil || b == nil {
		return nil, errors.New("chunkstore: chunk remote needs a filer and a blob store")
	}
	if !path.IsAbs(root) || path.Clean(root) != root || root == "/" {
		return nil, fmt.Errorf("chunkstore: chunk remote root %q must be a clean absolute directory other than /", root)
	}
	return &ChunkRemote{f: f, b: b, root: root, fids: map[string]string{}}, nil
}

func (r *ChunkRemote) keyDir() string { return path.Join(r.root, keysDir) }

func keyName(key string) (string, error) {
	if key == "" {
		return "", errors.New("chunkstore: empty chunk key")
	}
	return url.PathEscape(key), nil // one flat directory: '/' is escaped
}

// Put stores data as a new needle, then binds key to it with an exclusive
// create. A key that is already bound fails with storage.ErrChunkKeyExists
// and its bytes are untouched.
func (r *ChunkRemote) Put(ctx context.Context, key string, data []byte) error {
	name, err := keyName(key)
	if err != nil {
		return err
	}
	r.mu.Lock()
	_, known := r.fids[key]
	r.mu.Unlock()
	if known {
		return storage.ErrChunkKeyExists
	}
	fid, err := r.b.Put(ctx, data)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	e := &filerpb.Entry{
		Name:       name,
		Attributes: &filerpb.FuseAttributes{Mtime: now, Crtime: now, FileMode: 0o644},
		Extended:   map[string][]byte{extKind: []byte(kindChunkKey), extFid: []byte(fid)},
	}
	if err := r.f.CreateEntry(ctx, r.keyDir(), e, true); err != nil {
		if errors.Is(err, ErrExists) {
			return fmt.Errorf("%w: %s", storage.ErrChunkKeyExists, key)
		}
		return err
	}
	r.mu.Lock()
	r.fids[key] = fid
	r.mu.Unlock()
	return nil
}

// Get returns the bytes bound to key. The chunk layer checks them against
// the SHA-256 in HEAD.
func (r *ChunkRemote) Get(ctx context.Context, key string) ([]byte, error) {
	name, err := keyName(key)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	fid, ok := r.fids[key]
	r.mu.Unlock()
	if !ok {
		e, err := r.f.LookupEntry(ctx, r.keyDir(), name)
		if err != nil {
			return nil, fmt.Errorf("chunkstore: chunk key %q: %w", key, err)
		}
		if len(e.GetChunks()) != 0 || string(e.Extended[extKind]) != kindChunkKey || len(e.Extended[extFid]) == 0 {
			return nil, fmt.Errorf("chunkstore: chunk key %q: entry is not a chunk key binding", key)
		}
		fid = string(e.Extended[extFid])
		r.mu.Lock()
		r.fids[key] = fid
		r.mu.Unlock()
	}
	return r.b.Get(ctx, fid)
}

// Delete is not supported; see ErrDeleteUnsupported.
func (r *ChunkRemote) Delete(context.Context, string) error { return ErrDeleteUnsupported }

type headDoc struct {
	Format    int               `json:"format"`
	Gen       uint64            `json:"gen"`
	Epoch     uint64            `json:"epoch"`
	Holder    string            `json:"holder"`
	RemoteLSN uint64            `json:"remote_lsn"`
	Chunks    map[uint64]refDoc `json:"chunks"`
}

type refDoc struct {
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
}

// GetHead returns the published HEAD, or the zero HEAD (Gen 0) when none
// was ever published. An entry that exists but does not decode is an
// error, never the zero HEAD.
func (r *ChunkRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	e, err := r.f.LookupEntry(ctx, r.root, headName)
	if errors.Is(err, ErrNotFound) {
		return storage.ChunkHead{}, nil
	}
	if err != nil {
		return storage.ChunkHead{}, err
	}
	return decodeHead(path.Join(r.root, headName), e)
}

// CASHead installs next, at generation expected.Gen+1, only if the stored
// HEAD is still at expected. Ownership only moves forward: a proposal that
// lowers the epoch is refused.
func (r *ChunkRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	if next.Epoch < expected.Epoch {
		return 0, storage.ErrChunkHeadPrecondition
	}
	if expected.Gen == 0 && expected.Epoch != 0 {
		return 0, storage.ErrChunkHeadPrecondition // the absent HEAD has epoch 0
	}
	next.Gen = expected.Gen + 1
	e, err := headEntry(next)
	if err != nil {
		return 0, err
	}
	if expected.Gen == 0 {
		err = r.f.CreateEntry(ctx, r.root, e, true)
	} else {
		err = r.f.UpdateEntry(ctx, r.root, e, map[string][]byte{
			extGen:   []byte(strconv.FormatUint(expected.Gen, 10)),
			extEpoch: []byte(strconv.FormatUint(expected.Epoch, 10)),
		})
	}
	switch {
	case err == nil:
		return next.Gen, nil
	case errors.Is(err, ErrPrecondition), errors.Is(err, ErrExists), errors.Is(err, ErrNotFound):
		return 0, fmt.Errorf("%w: %v", storage.ErrChunkHeadPrecondition, err)
	default:
		return 0, err // outcome unknown: the caller re-reads HEAD
	}
}

func headEntry(h storage.ChunkHead) (*filerpb.Entry, error) {
	doc := headDoc{Format: headFormat, Gen: h.Gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.RemoteLSN,
		Chunks: make(map[uint64]refDoc, len(h.Chunks))}
	for idx, ref := range h.Chunks {
		if ref.Key == "" {
			return nil, fmt.Errorf("chunkstore: HEAD chunk %d has no key", idx)
		}
		doc.Chunks[idx] = refDoc{Key: ref.Key, SHA256: hex.EncodeToString(ref.SHA256[:])}
	}
	body, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	return &filerpb.Entry{
		Name: headName,
		Attributes: &filerpb.FuseAttributes{
			FileSize: uint64(len(body)),
			Mtime:    now,
			Crtime:   now,
			FileMode: 0o644,
			Mime:     "application/json",
		},
		Extended: map[string][]byte{
			extGen:    []byte(strconv.FormatUint(h.Gen, 10)),
			extEpoch:  []byte(strconv.FormatUint(h.Epoch, 10)),
			extHolder: []byte(h.Holder),
			extKind:   []byte(kindChunkHead),
		},
		Content: body,
	}, nil
}

func decodeHead(p string, e *filerpb.Entry) (storage.ChunkHead, error) {
	fail := func(format string, a ...any) (storage.ChunkHead, error) {
		return storage.ChunkHead{}, fmt.Errorf("chunkstore: HEAD %s: "+format, append([]any{p}, a...)...)
	}
	if len(e.GetChunks()) != 0 {
		return fail("carries %d filer chunks; refusing (an update would let the filer delete them)", len(e.Chunks))
	}
	if k := string(e.Extended[extKind]); k != kindChunkHead {
		return fail("kind %q, want %q", k, kindChunkHead)
	}
	var doc headDoc
	if err := json.Unmarshal(e.GetContent(), &doc); err != nil {
		return fail("decode: %v", err)
	}
	if doc.Format != headFormat {
		return fail("format %d not supported (want %d)", doc.Format, headFormat)
	}
	if doc.Gen == 0 {
		return fail("generation 0")
	}
	// The CAS compares the extended attributes, so they must agree with the
	// document or a CAS could validate against a different state.
	if string(e.Extended[extGen]) != strconv.FormatUint(doc.Gen, 10) ||
		string(e.Extended[extEpoch]) != strconv.FormatUint(doc.Epoch, 10) ||
		string(e.Extended[extHolder]) != doc.Holder {
		return fail("extended gen/epoch/holder (%q/%q/%q) disagree with the document (%d/%d/%q)",
			e.Extended[extGen], e.Extended[extEpoch], e.Extended[extHolder], doc.Gen, doc.Epoch, doc.Holder)
	}
	h := storage.ChunkHead{Gen: doc.Gen, Epoch: doc.Epoch, Holder: doc.Holder, RemoteLSN: doc.RemoteLSN,
		Chunks: make(map[uint64]storage.ChunkRef, len(doc.Chunks))}
	for idx, ref := range doc.Chunks {
		sum, err := hex.DecodeString(ref.SHA256)
		if err != nil || len(sum) != 32 || ref.Key == "" {
			return fail("chunk %d ref invalid", idx)
		}
		cr := storage.ChunkRef{Key: ref.Key}
		copy(cr.SHA256[:], sum)
		h.Chunks[idx] = cr
	}
	return h, nil
}

// NewSWFSChunkRemote is NewChunkRemote over one SWFS client, which serves
// as both the filer and the blob store.
func NewSWFSChunkRemote(c *SWFS, root string) (*ChunkRemote, error) {
	return NewChunkRemote(c, c, root)
}

var _ storage.ChunkRemote = (*ChunkRemote)(nil)
