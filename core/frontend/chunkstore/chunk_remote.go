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
//	<root>/intents/<id> one entry per live intent (<gen>-<nonce>): the keys a
//	                    holder is about to publish; GC treats them as roots
//
// The chunk layer names chunks itself (immutable keys) while SeaweedFS
// assigns needle ids, so a key entry binds the two.
//
// A key may be owner-qualified, "@<owner>/<key>": the binding then lives in
// the owner volume's keys/ directory, <base>/<owner>/keys/, where <base> is
// the parent of this volume's root. A fork's HEAD names its parent's chunks
// this way, so the fork shares them without copying any binding. The layer
// never generates a key starting with "@"; Get resolves qualified keys and
// Put refuses them (a volume only ever writes its own keys). It is created with
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
	"strings"
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

	kindChunkHead   = "chunkhead"
	kindChunkKey    = "chunkkey"
	kindChunkIntent = "chunkintent"

	headName   = "HEAD"
	keysDir    = "keys"
	intentsDir = "intents"

	// intentFormat is the only intent document format this code reads.
	intentFormat = 1
	// listPage is the page size of a full directory listing.
	listPage = 1000

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

// CheckVolumeID accepts a volume id that is one plain path element, so a
// volume's directory, or a qualified key's owner, stays inside the base.
func CheckVolumeID(id string) error {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\") {
		return fmt.Errorf("chunkstore: volume id %q is not a plain path element", id)
	}
	return nil
}

// keyLocation returns the directory and entry name holding key's binding:
// the owner's keys/ directory for "@<owner>/<key>", this volume's otherwise.
func (r *ChunkRemote) keyLocation(key string) (dir, name string, err error) {
	if err := storage.ValidChunkKey(key); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}
	if !strings.HasPrefix(key, "@") {
		name, err := keyName(key)
		return r.keyDir(), name, err
	}
	owner, own, ok := strings.Cut(key[1:], "/")
	if !ok || own == "" {
		return "", "", fmt.Errorf("%w: %q is not \"@<owner>/<key>\"", ErrInvalidKey, key)
	}
	if err := CheckVolumeID(owner); err != nil {
		return "", "", fmt.Errorf("%w: %q: %v", ErrInvalidKey, key, err)
	}
	return path.Join(path.Dir(r.root), owner, keysDir), url.PathEscape(own), nil
}

// Put stores data as a new needle, then binds key to it with an exclusive
// create. A key that is already bound fails with storage.ErrChunkKeyExists
// and its bytes are untouched.
func (r *ChunkRemote) Put(ctx context.Context, key string, data []byte) error {
	if err := storage.ValidChunkKey(key); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}
	if strings.HasPrefix(key, "@") {
		return fmt.Errorf("%w: %q is owner-qualified; a volume writes only its own keys", ErrInvalidKey, key)
	}
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
	dir, name, err := r.keyLocation(key)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	fid, ok := r.fids[key]
	r.mu.Unlock()
	if !ok {
		e, err := r.f.LookupEntry(ctx, dir, name)
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

type intentDoc struct {
	Segments   map[uint64]storage.ChunkSegment `json:"segments,omitempty"`
	ChunkBytes uint64                          `json:"chunk_bytes,omitempty"`
	Format     int                             `json:"format"`
	Gen        uint64                          `json:"gen"`
	Epoch      uint64                          `json:"epoch"`
	Holder     string                          `json:"holder"`
	Keys       []string                        `json:"keys"`
	CreatedNs  int64                           `json:"created_ns"`
	// DeadlineNs: a copy intent's publication deadline (0: a capture intent).
	DeadlineNs int64 `json:"deadline_ns,omitempty"`
}

func (r *ChunkRemote) intentDir() string { return path.Join(r.root, intentsDir) }

// PutIntent writes <root>/intents/<gen>-<nonce> with an exclusive create.
// The entry is durable when the filer's fsync rule covers the chunk path,
// the same condition as for HEAD.
func (r *ChunkRemote) PutIntent(ctx context.Context, in storage.ChunkIntent) (string, error) {
	return putIntent(ctx, r.f, r.root, in)
}

// putIntent writes an intent under <root>/intents/; the catalog uses it for
// snapshots and forks, which have no ChunkRemote.
func putIntent(ctx context.Context, f Filer, root string, in storage.ChunkIntent) (string, error) {
	for _, k := range in.Keys {
		if err := storage.ValidChunkKey(k); err != nil {
			return "", fmt.Errorf("%w: %v", ErrInvalidKey, err)
		}
	}
	id, err := storage.NewChunkIntentID(in.Gen)
	if err != nil {
		return "", err
	}
	now := time.Now()
	body, err := json.Marshal(intentDoc{Format: intentFormat, Gen: in.Gen, Epoch: in.Epoch, Holder: in.Holder, Keys: in.Keys, CreatedNs: now.UnixNano(), DeadlineNs: in.DeadlineNs, Segments: in.Segments, ChunkBytes: in.ChunkBytes})
	if err != nil {
		return "", err
	}
	e := &filerpb.Entry{
		Name:       id,
		Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body)), Mtime: now.Unix(), Crtime: now.Unix(), FileMode: 0o644, Mime: "application/json"},
		Extended: map[string][]byte{
			extKind:   []byte(kindChunkIntent),
			extGen:    []byte(strconv.FormatUint(in.Gen, 10)),
			extEpoch:  []byte(strconv.FormatUint(in.Epoch, 10)),
			extHolder: []byte(in.Holder),
		},
		Content: body,
	}
	if err := f.CreateEntry(ctx, path.Join(root, intentsDir), e, true); err != nil {
		return "", fmt.Errorf("chunkstore: put intent %s: %w", id, err)
	}
	return id, nil
}

// DeleteIntent removes <root>/intents/<id> and nothing else; a missing
// intent is not an error.
func (r *ChunkRemote) DeleteIntent(ctx context.Context, id string) error {
	if !storage.ValidChunkIntentID(id) {
		return fmt.Errorf("chunkstore: intent id %q is not \"<gen>-<nonce>\"", id)
	}
	return r.f.DeleteEntry(ctx, r.intentDir(), id, nil)
}

// ListIntents lists every entry under <root>/intents/. An entry that is not
// a well-formed intent fails the listing, so GC never misses a root.
func (r *ChunkRemote) ListIntents(ctx context.Context) ([]storage.ChunkIntent, error) {
	var out []storage.ChunkIntent
	after := ""
	for {
		page, err := r.f.ListEntries(ctx, r.intentDir(), "", after, listPage)
		if err != nil {
			return nil, err
		}
		for _, e := range page {
			in, err := decodeIntent(path.Join(r.intentDir(), e.GetName()), e)
			if err != nil {
				return nil, err
			}
			out = append(out, in)
		}
		if len(page) < listPage {
			return out, nil
		}
		after = page[len(page)-1].GetName()
	}
}

func decodeIntent(p string, e *filerpb.Entry) (storage.ChunkIntent, error) {
	fail := func(format string, a ...any) (storage.ChunkIntent, error) {
		return storage.ChunkIntent{}, fmt.Errorf("chunkstore: intent %s: "+format, append([]any{p}, a...)...)
	}
	if len(e.GetChunks()) != 0 {
		return fail("carries %d filer chunks", len(e.Chunks))
	}
	if k := string(e.Extended[extKind]); k != kindChunkIntent {
		return fail("kind %q, want %q", k, kindChunkIntent)
	}
	if !storage.ValidChunkIntentID(e.GetName()) {
		return fail("name is not \"<gen>-<nonce>\"")
	}
	var doc intentDoc
	if err := json.Unmarshal(e.GetContent(), &doc); err != nil {
		return fail("decode: %v", err)
	}
	if doc.Format != intentFormat {
		return fail("format %d not supported (want %d)", doc.Format, intentFormat)
	}
	if !strings.HasPrefix(e.GetName(), strconv.FormatUint(doc.Gen, 10)+"-") ||
		string(e.Extended[extEpoch]) != strconv.FormatUint(doc.Epoch, 10) || string(e.Extended[extHolder]) != doc.Holder {
		return fail("name or extended gen/epoch/holder disagree with the document")
	}
	return storage.ChunkIntent{ID: e.GetName(), Gen: doc.Gen, Epoch: doc.Epoch, Holder: doc.Holder,
		Keys: append([]string(nil), doc.Keys...), CreatedNs: doc.CreatedNs, DeadlineNs: doc.DeadlineNs, Segments: doc.Segments, ChunkBytes: doc.ChunkBytes}, nil
}

type headDoc struct {
	Segments   map[uint64]storage.ChunkSegment `json:"segments,omitempty"`
	ChunkBytes uint64                          `json:"chunk_bytes,omitempty"`
	ChunkCount int                             `json:"chunk_count,omitempty"`
	Format     int                             `json:"format"`
	Gen        uint64                          `json:"gen"`
	Epoch      uint64                          `json:"epoch"`
	Holder     string                          `json:"holder"`
	RemoteLSN  uint64                          `json:"remote_lsn"`
	Chunks     map[uint64]refDoc               `json:"chunks,omitempty"`
	// State is "active" or "detached"; a document without it is active.
	// DetachedBy names the holder whose final cut (this generation) a
	// detached HEAD is; it is absent unless State is detached.
	State      string         `json:"state,omitempty"`
	DetachedBy *detachedByDoc `json:"detachedBy,omitempty"`
}

type detachedByDoc struct {
	Holder string `json:"holder"`
	Epoch  uint64 `json:"epoch"`
	Gen    uint64 `json:"gen"`
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
	h, err := decodeHead(path.Join(r.root, headName), e)
	if err != nil {
		return h, err
	}
	return r.resolveManifest(ctx, h)
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
	// Read the LSN under the same generation/epoch guard used by UpdateEntry.
	// A competing write after this read makes the conditional update fail;
	// no successful CAS can lower the committed lineage frontier.
	current, err := r.GetHead(ctx)
	if err != nil {
		return 0, err
	}
	if current.Gen != expected.Gen || current.Epoch != expected.Epoch || next.RemoteLSN < current.RemoteLSN {
		return 0, storage.ErrChunkHeadPrecondition
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
		State: h.State}
	if h.DetachedBy != nil {
		doc.DetachedBy = &detachedByDoc{Holder: h.DetachedBy.Holder, Epoch: h.DetachedBy.Epoch, Gen: h.DetachedBy.Gen}
	}
	if h.Segments == nil {
		doc.Chunks = make(map[uint64]refDoc, len(h.Chunks))
		for idx, ref := range h.Chunks {
			if ref.Key == "" {
				return nil, fmt.Errorf("chunkstore: HEAD chunk %d has no key", idx)
			}
			doc.Chunks[idx] = refDoc{Key: ref.Key, SHA256: hex.EncodeToString(ref.SHA256[:])}
		}
	}
	if h.Segments != nil {
		if err := validateRoot(h); err != nil {
			return nil, err
		}
		doc.Format = 2
		doc.Chunks = nil
		doc.Segments = h.Segments
		doc.ChunkBytes = h.ChunkBytes
		doc.ChunkCount = h.ManifestChunkCount
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
	h, err := decodeHeadDoc(p, e.GetContent())
	if err != nil {
		return storage.ChunkHead{}, err
	}
	// The CAS compares the extended attributes, so they must agree with the
	// document or a CAS could validate against a different state.
	if string(e.Extended[extGen]) != strconv.FormatUint(h.Gen, 10) ||
		string(e.Extended[extEpoch]) != strconv.FormatUint(h.Epoch, 10) ||
		string(e.Extended[extHolder]) != h.Holder {
		return fail("extended gen/epoch/holder (%q/%q/%q) disagree with the document (%d/%d/%q)",
			e.Extended[extGen], e.Extended[extEpoch], e.Extended[extHolder], h.Gen, h.Epoch, h.Holder)
	}
	return h, nil
}

// decodeHeadDoc decodes and checks a HEAD document: HEAD's own content, or
// the copy a snapshot embeds.
func decodeHeadDoc(p string, content []byte) (storage.ChunkHead, error) {
	fail := func(format string, a ...any) (storage.ChunkHead, error) {
		return storage.ChunkHead{}, fmt.Errorf("chunkstore: HEAD %s: "+format, append([]any{p}, a...)...)
	}
	var doc headDoc
	if err := decodeManifestJSON(content, &doc); err != nil {
		return fail("decode: %v", err)
	}
	if doc.Format != headFormat && doc.Format != 2 {
		return fail("format %d not supported (want %d)", doc.Format, headFormat)
	}
	if doc.Gen == 0 {
		return fail("generation 0")
	}
	switch doc.State {
	case "", storage.ChunkHeadActive:
		if doc.DetachedBy != nil {
			return fail("detachedBy on a HEAD that is not detached")
		}
	case storage.ChunkHeadAborted:
		// A sweep's tombstone over a dead fork's HEAD (gen 1, no chunks).
		if doc.DetachedBy != nil || len(doc.Chunks) != 0 {
			return fail("an aborted HEAD with content")
		}
	case storage.ChunkHeadDetached:
		d := doc.DetachedBy
		if d == nil || d.Gen != doc.Gen || d.Holder != doc.Holder || d.Epoch != doc.Epoch {
			return fail("detached HEAD whose detachedBy %+v is not its own holder, epoch and generation", d)
		}
	default:
		return fail("state %q", doc.State)
	}
	h := storage.ChunkHead{Gen: doc.Gen, Epoch: doc.Epoch, Holder: doc.Holder, RemoteLSN: doc.RemoteLSN,
		Chunks: make(map[uint64]storage.ChunkRef, len(doc.Chunks)), State: doc.State}
	if doc.DetachedBy != nil {
		h.DetachedBy = &storage.ChunkDetachedBy{Holder: doc.DetachedBy.Holder, Epoch: doc.DetachedBy.Epoch, Gen: doc.DetachedBy.Gen}
	}
	for idx, ref := range doc.Chunks {
		sum, err := hex.DecodeString(ref.SHA256)
		if err != nil || len(sum) != 32 || ref.Key == "" {
			return fail("chunk %d ref invalid", idx)
		}
		cr := storage.ChunkRef{Key: ref.Key}
		copy(cr.SHA256[:], sum)
		h.Chunks[idx] = cr
	}
	if doc.Format == 2 {
		if len(doc.Chunks) != 0 {
			return fail("segmented root contains flat chunks")
		}
		h.Segments = doc.Segments
		if h.Segments == nil {
			h.Segments = map[uint64]storage.ChunkSegment{}
		}
		h.ChunkBytes = doc.ChunkBytes
		h.ManifestChunkCount = doc.ChunkCount
		if err := validateRoot(h); err != nil {
			return h, err
		}
	}
	return h, nil
}

// NewSWFSChunkRemote is NewChunkRemote over one SWFS client, which serves
// as both the filer and the blob store.
func NewSWFSChunkRemote(c *SWFS, root string) (*ChunkRemote, error) {
	return NewChunkRemote(c, c, root)
}

var _ storage.ChunkRemote = (*ChunkRemote)(nil)
