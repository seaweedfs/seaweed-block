package chunkstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// LayoutRemote is the chunk engine's remote (storage.ChunkRemote) on the
// content-named layout, for the holder me of one volume. The engine keeps
// its view at H, its copy-on-write and its base + dirty record; this maps
// its remote calls onto the kernel's snapshot transaction:
//   - the first upload of a cut opens a session (Begin; a fresh id, never
//     reused); every upload is EnsureObject of the content-named chunk;
//   - CASHead is the Commit (current root, position, state, and the
//     snapshot named by SetSnapshot), conditional on the generation the
//     engine read: the one before this session's Begin;
//   - a session of this holder found open with no session in memory (a
//     crash) is rolled back before anything else.
//
// Keys are layout:<owner>:<owner id>:<chunk name> (ChunkKey), so a key
// alone resolves the owner's directory and is verified by name.
type LayoutRemote struct {
	st  *LayoutStore
	r   *layout.Reader
	vol VolumeRef
	me  layout.HolderID

	mu        sync.Mutex
	session   string
	beganFrom uint64 // HEAD generation this session's Begin started from
	pending   *pendingSnapshot
}

type pendingSnapshot struct {
	name string
	meta layout.SnapshotMeta
}

var (
	_ storage.ChunkRemote = (*LayoutRemote)(nil)
	_ storage.ChunkKeyer  = (*LayoutRemote)(nil)
)

// NewLayoutRemote returns the remote of volume v for the holder me (the
// line its Start claimed).
func NewLayoutRemote(cat *BCatalog, v VolumeRef, me layout.HolderID) (*LayoutRemote, error) {
	if me.Epoch == 0 || me.Holder == "" || me.ClaimID == "" {
		return nil, errors.New("chunkstore: layout remote needs the holder's epoch, holder and claim id")
	}
	st, r, err := cat.open(v)
	if err != nil {
		return nil, err
	}
	return &LayoutRemote{st: st, r: r, vol: v, me: me}, nil
}

// SetSnapshot names the snapshot the next commit records, with its
// retention facts; a commit consumes it.
func (l *LayoutRemote) SetSnapshot(name string, meta layout.SnapshotMeta) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pending = &pendingSnapshot{name: name, meta: meta}
}

// layoutKeyPrefix marks a key that is a layout reference.
const layoutKeyPrefix = "layout:"

// LayoutKey is the engine key of a layout reference.
func LayoutKey(ref layout.Ref) string {
	return layoutKeyPrefix + ref.Owner + ":" + ref.OwnerID.String() + ":" + ref.Chunk.String()
}

// ParseLayoutKey parses LayoutKey's form.
func ParseLayoutKey(key string) (layout.Ref, error) {
	rest, ok := strings.CutPrefix(key, layoutKeyPrefix)
	parts := strings.Split(rest, ":")
	if !ok || len(parts) != 3 {
		return layout.Ref{}, fmt.Errorf("%w: %q is not a layout key", storage.ErrChunkKeyInvalid, key)
	}
	id, err := layout.ParseVolumeID(parts[1])
	if err != nil {
		return layout.Ref{}, err
	}
	name, err := layout.ParseChunkName(parts[2])
	if err != nil {
		return layout.Ref{}, err
	}
	return layout.Ref{Owner: parts[0], OwnerID: id, Chunk: name}, nil
}

// ChunkKey names the key of this volume's chunk idx whose bytes hash to sum.
func (l *LayoutRemote) ChunkKey(idx uint64, sum [32]byte) string {
	return LayoutKey(l.own(idx, sum))
}

func (l *LayoutRemote) own(idx uint64, sum [32]byte) layout.Ref {
	return layout.Ref{Owner: l.vol.Name, OwnerID: l.vol.ID, Chunk: layout.ChunkName{Index: idx, Hash: sum}}
}

// openSessionLocked opens this cut's session if none is open: a session of
// this holder left open by a crash is rolled back first.
func (l *LayoutRemote) openSessionLocked(ctx context.Context) error {
	if l.session != "" {
		return nil
	}
	if _, err := l.rollBackLocked(ctx); err != nil {
		return err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return err
	}
	s := "s-" + hex.EncodeToString(b[:])
	h, err := layout.RunBegin(ctx, l.st, l.me, s)
	if err != nil {
		return layoutRemoteErr(err)
	}
	l.session, l.beganFrom = s, h.Gen-1
	return nil
}

// rollBackLocked rolls back a session of this holder that HEAD shows open
// but this process does not hold (it died with the process), and returns
// HEAD.
func (l *LayoutRemote) rollBackLocked(ctx context.Context) (layout.Head, error) {
	h, found, err := l.st.ReadHead(ctx)
	if err != nil {
		return layout.Head{}, err
	}
	if !found {
		return layout.Head{}, layout.ErrHeadAbsent
	}
	if h.Activity.Kind == layout.ActSnap && h.Activity.Session != l.session && h.Epoch == l.me.Epoch && h.Holder == l.me.Holder && h.ClaimID == l.me.ClaimID {
		if h, err = layout.RunAbort(ctx, l.st, l.me, h.Activity.Session); err != nil {
			return layout.Head{}, layoutRemoteErr(err)
		}
	}
	return h, nil
}

// Put stores the chunk under its content name in this cut's session; it
// returns only once the object entry exists.
func (l *LayoutRemote) Put(ctx context.Context, key string, data []byte) error {
	ref, err := ParseLayoutKey(key)
	if err != nil {
		return err
	}
	if ref.OwnerID != l.vol.ID || ref.Chunk != layout.NameChunk(ref.Chunk.Index, data) {
		return fmt.Errorf("%w: %q is not this volume's name for these bytes", storage.ErrChunkKeyInvalid, key)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.openSessionLocked(ctx); err != nil {
		return err
	}
	return layoutRemoteErr(layout.EnsureObject(ctx, l.st, ref.Chunk.Path(), data, l.session))
}

// Get reads a chunk by its key through the volume's reader: the owner's
// directory, its VOLUME id checked, the bytes checked against the name.
func (l *LayoutRemote) Get(ctx context.Context, key string) ([]byte, error) {
	ref, err := ParseLayoutKey(key)
	if err != nil {
		return nil, err
	}
	b, err := l.r.ReadRef(ctx, ref)
	return b, layoutRemoteErr(err)
}

// GetHead reads HEAD as the engine's view: the generation the engine last
// saw (the one before this process's open session), the position, and the
// current root resolved to the chunk map. A session of this holder left
// open by a crash is rolled back first.
func (l *LayoutRemote) GetHead(ctx context.Context) (storage.ChunkHead, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, err := l.rollBackLocked(ctx)
	if err != nil {
		return storage.ChunkHead{}, err
	}
	if h.State == layout.StateDeleting || h.State == layout.StateDeleted {
		return storage.ChunkHead{}, storage.ErrChunkHeadDeleted
	}
	gen := h.Gen
	if l.session != "" && h.Activity == (layout.Activity{Kind: layout.ActSnap, Session: l.session}) {
		gen = l.beganFrom
	}
	out := storage.ChunkHead{Gen: gen, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.Position, Chunks: map[uint64]storage.ChunkRef{}, State: "active"}
	if h.State == layout.StateDetached {
		out.State = "detached"
		out.DetachedBy = &storage.ChunkDetachedBy{Holder: h.Holder, Epoch: h.Epoch, Gen: h.Gen}
	}
	if h.Current == nil {
		return out, nil
	}
	m, err := l.manifest(ctx, *h.Current)
	if err != nil {
		return storage.ChunkHead{}, err
	}
	for idx, ref := range m {
		out.Chunks[idx] = storage.ChunkRef{Key: LayoutKey(ref), SHA256: ref.Chunk.Hash, Encoding: "full", BirthLSN: h.Position}
	}
	return out, nil
}

// manifest is the chunk map root names.
func (l *LayoutRemote) manifest(ctx context.Context, rn layout.RootName) (map[uint64]layout.Ref, error) {
	img, err := l.r.Open(ctx, rn)
	if err != nil {
		return nil, layoutRemoteErr(err)
	}
	m := map[uint64]layout.Ref{}
	for _, sh := range img.Shards() {
		seg, _, err := img.Segment(ctx, sh)
		if err != nil {
			return nil, layoutRemoteErr(err)
		}
		for idx, ref := range seg.Chunks {
			m[idx] = ref
		}
	}
	return m, nil
}

// CASHead commits next as this cut's session: the root of next's chunk
// map, position next.RemoteLSN, detached when next is, and the pending
// snapshot name. It applies only while HEAD is still at expected (before
// this session's Begin; Begin and Commit back to back when no upload
// opened a session); any other HEAD is storage.ErrChunkHeadPrecondition.
func (l *LayoutRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if expected.Epoch != l.me.Epoch || next.Epoch != l.me.Epoch {
		return 0, storage.ErrChunkHeadPrecondition
	}
	h, found, err := l.st.ReadHead(ctx)
	if err != nil {
		return 0, err
	}
	if !found {
		return 0, layout.ErrHeadAbsent
	}
	switch {
	case l.session != "" && h.Activity == (layout.Activity{Kind: layout.ActSnap, Session: l.session}) && l.beganFrom == expected.Gen:
	case l.session == "" && h.Gen == expected.Gen:
	default:
		return 0, storage.ErrChunkHeadPrecondition
	}
	prev, nextM, err := l.manifests(ctx, h, next)
	if err != nil {
		return 0, err
	}
	if err := l.openSessionLocked(ctx); err != nil {
		return 0, err
	}
	if err := l.writeRootLocked(ctx, nextM); err != nil {
		return 0, err
	}
	req := layout.CommitRequest{Session: l.session, Prev: prev, Next: nextM, Position: next.RemoteLSN, State: layout.StateLive}
	if next.State == "detached" {
		req.State = layout.StateDetached
	}
	if l.pending != nil {
		req.Snapshot, req.Meta = l.pending.name, l.pending.meta
	}
	got, err := layout.RunCommit(ctx, l.st, l.st, l.vol.Name, l.me, req)
	if err != nil {
		if errors.Is(err, layout.ErrStaleBase) {
			return 0, storage.ErrChunkHeadPrecondition
		}
		return 0, layoutRemoteErr(err)
	}
	l.session, l.beganFrom, l.pending = "", 0, nil
	return got.Gen, nil
}

// manifests are HEAD's current chunk map and next's, as layout references:
// a key the engine got from HEAD keeps its reference (also another
// volume's); a chunk the engine uploaded is this volume's by name.
func (l *LayoutRemote) manifests(ctx context.Context, h layout.Head, next storage.ChunkHead) (map[uint64]layout.Ref, map[uint64]layout.Ref, error) {
	prev := map[uint64]layout.Ref{}
	if h.Current != nil {
		m, err := l.manifest(ctx, *h.Current)
		if err != nil {
			return nil, nil, err
		}
		prev = m
	}
	nm := make(map[uint64]layout.Ref, len(next.Chunks))
	for idx, c := range next.Chunks {
		ref, err := ParseLayoutKey(c.Key)
		if err != nil {
			return nil, nil, err
		}
		if ref.Chunk.Index != idx || ref.Chunk.Hash != c.SHA256 {
			return nil, nil, fmt.Errorf("%w: chunk %d key %q does not name its bytes", storage.ErrChunkKeyInvalid, idx, c.Key)
		}
		nm[idx] = ref
	}
	return prev, nm, nil
}

// writeRootLocked writes next's segments and root in this session, so the
// commit finds them prepared.
func (l *LayoutRemote) writeRootLocked(ctx context.Context, next map[uint64]layout.Ref) error {
	root, segs, err := layout.Build(next)
	if err != nil {
		return err
	}
	names := make([]layout.SegName, 0, len(segs))
	for n := range segs {
		names = append(names, n)
	}
	sort.Slice(names, func(a, b int) bool { return names[a].Shard < names[b].Shard })
	for _, n := range names {
		if err := layout.EnsureObject(ctx, l.st, n.Path(), segs[n], l.session); err != nil {
			return layoutRemoteErr(err)
		}
	}
	rn, rb, err := layout.NameRoot(root)
	if err != nil {
		return err
	}
	return layoutRemoteErr(layout.EnsureObject(ctx, l.st, rn.Path(), rb, l.session))
}

// The engine's intents are the session on the layout: these exist on the
// cutover branch only and go with the old store.
func (l *LayoutRemote) PutIntent(ctx context.Context, in storage.ChunkIntent) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.openSessionLocked(ctx); err != nil {
		return "", err
	}
	return storage.NewChunkIntentID(in.Gen)
}

func (l *LayoutRemote) DeleteIntent(context.Context, string) error { return nil }

func (l *LayoutRemote) ListIntents(context.Context) ([]storage.ChunkIntent, error) { return nil, nil }

// Delete is not a removal path on the layout (cleaning removes names).
func (l *LayoutRemote) Delete(context.Context, string) error { return nil }

// layoutRemoteErr maps a layout precondition to the engine's.
func layoutRemoteErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, layout.ErrPrecondition), errors.Is(err, layout.ErrNotHolder), errors.Is(err, layout.ErrNoSession):
		return fmt.Errorf("%w: %w", storage.ErrChunkHeadPrecondition, err)
	case errors.Is(err, layout.ErrVolumeDeleting):
		return fmt.Errorf("%w: %w", storage.ErrChunkHeadDeleted, err)
	}
	return err
}
