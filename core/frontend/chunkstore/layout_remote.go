package chunkstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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
// sessionStore is what the remote writes through: HEAD and the objects.
type sessionStore interface {
	layout.HeadStore
	layout.ObjectStore
}

type LayoutRemote struct {
	st  sessionStore
	r   *layout.Reader
	vol VolumeRef
	me  layout.HolderID

	mu      sync.Mutex
	session string
	// visible is the HEAD generation the engine sees: it moves with this
	// remote's commits and with writes of anyone else, never with this
	// remote's own Begin, Abort or cleaning; ownGen is HEAD's generation
	// after this remote's last own write.
	visible, ownGen uint64
	// seen is the image HEAD named when visible was last set: a write of
	// anyone else that leaves it equal (a snapshot delete, retention, a
	// dependent added) does not move visible.
	seen imageTuple
	// forceView moves visible at the next HEAD read even when the image is
	// unchanged: this remote's session ended without its commit, so what
	// the engine staged in it is void and must be dropped.
	forceView bool
	// abandoned is set by the engine when a cut ends without a commit
	// (Abort): the next call drops the session and the pending name.
	abandoned atomic.Bool
	// checked is the (session, root) whose commit's object check passed
	// (CheckCommit), in that open session: CASHead skips the lookups only for
	// exactly that pair. A new session has a new id, so it never matches.
	checked struct {
		session string
		root    layout.RootName
	}
	pending *pendingSnapshot
	// snapErr is why the last commit could not name its snapshot (the cut
	// itself committed unnamed); TakeSnapshotErr hands it to the request.
	snapErr error
	// cleaning is this process's open cleaning: its session and the way to
	// end it early, so a cut or a stop never waits on it or fails busy.
	cleaning    string
	cleanCancel context.CancelFunc
	cleanDone   chan struct{}
	lastCommit  layout.Head
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

// DropSnapshot withdraws a pending snapshot name whose request gave up (its
// cut failed): the name never rides onto a later cut, which would name an
// image the requester was told was not taken. A cut that keeps its session
// for a retry does not drop it by itself.
func (l *LayoutRemote) DropSnapshot() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pending = nil
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
		return layout.Ref{}, fmt.Errorf("%w: %q: %w", storage.ErrChunkKeyInvalid, key, err)
	}
	name, err := layout.ParseChunkName(parts[2])
	if err != nil {
		return layout.Ref{}, fmt.Errorf("%w: %q: %w", storage.ErrChunkKeyInvalid, key, err)
	}
	ref := layout.Ref{Owner: parts[0], OwnerID: id, Chunk: name}
	// One key per reference: a key that does not round-trip (an uppercase
	// id, a padded index, ...) or names no volume is not a layout key.
	if id.IsZero() || parts[0] == "" || LayoutKey(ref) != key {
		return layout.Ref{}, fmt.Errorf("%w: %q is not a canonical layout key", storage.ErrChunkKeyInvalid, key)
	}
	return ref, nil
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
	if err := l.stopCleaningLocked(ctx); err != nil {
		return err
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
	l.session, l.ownGen = s, h.Gen
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
	l.viewLocked(h)
	mine := h.Epoch == l.me.Epoch && h.Holder == l.me.Holder && h.ClaimID == l.me.ClaimID
	switch {
	case mine && h.Activity.Kind == layout.ActSnap && h.Activity.Session != l.session:
		if h, err = layout.RunAbort(ctx, l.st, l.me, h.Activity.Session); err != nil {
			return layout.Head{}, layoutRemoteErr(err)
		}
		l.ownGen = h.Gen
	case mine && h.Activity.Kind == layout.ActClean && h.Activity.Session != l.cleaning:
		if h, err = layout.RunEndClean(ctx, l.st, l.me, h.Activity.Session); err != nil {
			return layout.Head{}, layoutRemoteErr(err)
		}
		l.ownGen = h.Gen
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
	l.takeAbandonLocked()
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
	l.takeAbandonLocked()
	h, err := l.rollBackLocked(ctx)
	if err != nil {
		return storage.ChunkHead{}, err
	}
	if h.State == layout.StateDeleting || h.State == layout.StateDeleted {
		return storage.ChunkHead{}, storage.ErrChunkHeadDeleted
	}
	// Another claim at this holder's own epoch is another owner of HEAD: the
	// head the engine sees carries no claim id, so it must not read as its own.
	if h.Epoch == l.me.Epoch && h.Holder == l.me.Holder && h.ClaimID != l.me.ClaimID {
		return storage.ChunkHead{}, fmt.Errorf("%w: HEAD is held by %s at epoch %d (claim %q)", storage.ErrChunkLayerFenced, h.Holder, h.Epoch, h.ClaimID)
	}
	out := storage.ChunkHead{Gen: l.visible, Epoch: h.Epoch, Holder: h.Holder, RemoteLSN: h.Position, Chunks: map[uint64]storage.ChunkRef{}, State: "active"}
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
		out.Chunks[idx] = storage.ChunkRef{Key: LayoutKey(ref), SHA256: ref.Chunk.Hash, Encoding: "full"}
	}
	return out, nil
}

// manifest is the chunk map root names.
func (l *LayoutRemote) manifest(ctx context.Context, rn layout.RootName) (map[uint64]layout.Ref, error) {
	m := map[uint64]layout.Ref{}
	err := l.r.EachRef(ctx, rn, layout.AllShards, func(idx uint64, ref layout.Ref) bool {
		m[idx] = ref
		return true
	})
	if err != nil {
		return nil, layoutRemoteErr(err)
	}
	return m, nil
}

// CASHead commits next as this cut's session: the root of next's chunk
// map, position next.RemoteLSN, detached when next is, and the pending
// snapshot name. It uses the engine view from before this session's Begin.
// A concurrent operator-only HEAD update is rebased with a bounded retry;
// a foreign claim or detached/deleted state remains a final refusal.
// prepareCommitLocked reads HEAD and builds next's commit request in the
// open session, its root and segments written (CASHead and CheckCommit).
func (l *LayoutRemote) prepareCommitLocked(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (layout.Head, layout.CommitRequest, error) {
	l.takeAbandonLocked()
	if expected.Epoch != l.me.Epoch || next.Epoch != l.me.Epoch {
		return layout.Head{}, layout.CommitRequest{}, storage.ErrChunkHeadPrecondition
	}
	h, found, err := l.st.ReadHead(ctx)
	if err != nil {
		return layout.Head{}, layout.CommitRequest{}, err
	}
	if !found {
		return layout.Head{}, layout.CommitRequest{}, layout.ErrHeadAbsent
	}
	l.viewLocked(h)
	if expected.Gen != l.visible {
		return layout.Head{}, layout.CommitRequest{}, storage.ErrChunkHeadPrecondition
	}
	prev, nextM, err := l.manifests(ctx, h, next)
	if err != nil {
		return layout.Head{}, layout.CommitRequest{}, err
	}
	if err := l.openSessionLocked(ctx); err != nil {
		return layout.Head{}, layout.CommitRequest{}, err
	}
	if err := l.writeRootLocked(ctx, nextM); err != nil {
		return layout.Head{}, layout.CommitRequest{}, err
	}
	req := layout.CommitRequest{Session: l.session, Prev: prev, Next: nextM, Position: next.RemoteLSN, State: layout.StateLive}
	if next.State == "detached" {
		req.State = layout.StateDetached
	}
	return h, req, nil
}

// CheckCommit runs the object check of next's commit (every new chunk,
// segment and root written by this session and unmarked) with ctx, the
// cut's budget, before the bounded HEAD update. It runs after the cut's
// last upload, in the same open session; it writes no HEAD. On success
// CASHead skips those lookups for this (session, root). A failed check
// leaves the session open for a retry.
func (l *LayoutRemote) CheckCommit(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.checked.session, l.checked.root = "", layout.RootName{}
	h, req, err := l.prepareCommitLocked(ctx, expected, next)
	if err != nil {
		return err
	}
	root, err := layout.CommitRoot(req.Next)
	if err != nil {
		return err
	}
	if err := layout.CheckCommitObjects(ctx, h, l.st, l.vol.Name, l.me, req); err != nil {
		if errors.Is(err, layout.ErrNotPrepared) {
			l.forceView = true // as CASHead: the re-read drops what the ended session staged
			return fmt.Errorf("%w: %w", storage.ErrChunkHeadPrecondition, err)
		}
		return layoutRemoteErr(err)
	}
	l.checked.session, l.checked.root = req.Session, root
	return nil
}

func (l *LayoutRemote) CASHead(ctx context.Context, expected storage.ChunkHeadVersion, next storage.ChunkHead) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, req, err := l.prepareCommitLocked(ctx, expected, next)
	if err != nil {
		return 0, err
	}
	if root, rerr := layout.CommitRoot(req.Next); rerr == nil && l.checked.session != "" && l.checked.session == req.Session && l.checked.root == root {
		req.ObjectsChecked = true
	}
	l.checked.session, l.checked.root = "", layout.RootName{}
	if l.pending != nil {
		if err := layout.CheckSnapshot(l.pending.name, l.pending.meta); err != nil {
			// A name or facts the commit would refuse: the cut commits
			// unnamed and the request gets the refusal.
			l.snapErr, l.pending = err, nil
		} else {
			req.Snapshot, req.Meta = l.pending.name, l.pending.meta
		}
	}
	var got layout.Head
	// An operator may change only HEAD's snapshot/dependent table between
	// Commit's read and conditional write. Re-read and retry that same image
	// with the same session; a different claim is a final fence. The bound
	// keeps a busy operator from starving the holder's cut indefinitely.
	const maxMetadataCommitAttempts = 4
	for attempt := 0; attempt < maxMetadataCommitAttempts; attempt++ {
		got, err = layout.RunCommit(ctx, l.st, l.st, l.vol.Name, l.me, req)
		if err != nil && req.Snapshot != "" && snapshotRefusal(err) {
			// This snapshot cannot be named; the cut still commits unnamed and
			// the request gets the typed refusal.
			l.snapErr, l.pending = err, nil
			req.Snapshot, req.Meta = "", layout.SnapshotMeta{}
			got, err = layout.RunCommit(ctx, l.st, l.st, l.vol.Name, l.me, req)
		}
		if !errors.Is(err, layout.ErrPrecondition) {
			break
		}
		cur, found, readErr := l.st.ReadHead(ctx)
		if readErr != nil {
			return 0, layoutRemoteErr(readErr)
		}
		if !found {
			return 0, storage.ErrChunkHeadDeleted
		}
		if cur.Epoch != l.me.Epoch || cur.Holder != l.me.Holder || cur.ClaimID != l.me.ClaimID {
			return 0, fmt.Errorf("%w: layout HEAD claim changed during commit", storage.ErrChunkLayerFenced)
		}
		if cur.State == layout.StateDetached {
			return 0, storage.ErrChunkLayerFenced
		}
		if cur.State == layout.StateDeleting || cur.State == layout.StateDeleted {
			return 0, storage.ErrChunkHeadDeleted
		}
		if imageOf(cur) != imageOf(h) || cur.Activity != (layout.Activity{Kind: layout.ActSnap, Session: l.session}) {
			return 0, storage.ErrChunkHeadPrecondition
		}
		l.viewLocked(cur)
	}
	if err != nil {
		if errors.Is(err, layout.ErrStaleBase) {
			return 0, storage.ErrChunkHeadPrecondition
		}
		if errors.Is(err, layout.ErrNotPrepared) {
			// An upload of an ended session (a staged pre-copy): forget the
			// engine's view so its re-read finds a newer generation and
			// drops what it staged; the retry uploads again.
			l.forceView = true
			return 0, fmt.Errorf("%w: %w", storage.ErrChunkHeadPrecondition, err)
		}
		// An unknown outcome is decided by the engine's next HEAD read,
		// which also re-derives this session from HEAD (syncSessionLocked).
		return 0, layoutRemoteErr(err)
	}
	l.session, l.pending, l.lastCommit = "", nil, got
	l.ownGen, l.visible, l.seen = got.Gen, got.Gen, imageOf(got)
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
		if c.Encoding != "full" {
			return nil, nil, fmt.Errorf("%w: chunk %d encoding %q (only full: the stored bytes are the logical bytes)", ErrUnsupportedEncoding, idx, c.Encoding)
		}
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
	_, err = layout.PrepareRoot(ctx, l.st, root, segs, l.session)
	return layoutRemoteErr(err)
}

// Begin opens the cut's session on HEAD (the kernel's Begin), or replays
// the open one, and returns it.
func (l *LayoutRemote) Begin(ctx context.Context) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.takeAbandonLocked()
	if err := l.openSessionLocked(ctx); err != nil {
		return "", err
	}
	return l.session, nil
}

// Delete is not a removal path on the layout (cleaning removes names).
func (l *LayoutRemote) Delete(context.Context, string) error { return nil }

// ErrRemoteDamaged: the remote image is damaged (an object HEAD names is
// missing or does not hash to its name, or a VOLUME id does not match):
// never retried as unavailable. The block keeps serving locally; its
// backup is degraded and the operator sees the error.
var ErrRemoteDamaged = errors.New("chunkstore: the remote image is damaged")

// ErrUnsupportedEncoding: a chunk encoding other than full (the content
// name hashes the stored bytes, which must be the logical bytes).
var ErrUnsupportedEncoding = errors.New("chunkstore: unsupported chunk encoding on the layout")

// snapshotRefusal reports a commit-time refusal of the snapshot name alone
// (the name's own form is checked before the commit, CheckSnapshot).
func snapshotRefusal(err error) bool {
	return errors.Is(err, layout.ErrTooManySnapshots) || errors.Is(err, layout.ErrSnapshotExists)
}

// kernelRefusals are answers of the store (a condition failed), as opposed
// to the store not answering.
var kernelRefusals = []error{layout.ErrPrecondition, layout.ErrNotHolder, layout.ErrNoSession, layout.ErrBusy, layout.ErrDetached,
	layout.ErrVolumeDeleting, layout.ErrStaleEpoch, layout.ErrNotPrepared, layout.ErrStaleBase, layout.ErrHeadAbsent,
	layout.ErrMalformed, layout.ErrGeometryMismatch, layout.ErrTooManySnapshots, layout.ErrSnapshotExists, layout.ErrPositionBehind}

func isKernelRefusal(err error) bool {
	for _, k := range kernelRefusals {
		if errors.Is(err, k) {
			return true
		}
	}
	return false
}

// CommitPendingSnapshot names HEAD's current image with the snapshot set
// by SetSnapshot when no cut committed it (the engine had nothing to
// publish): one session, the current root and position again, the name.
// Nothing pending is a no-op. A name or facts the kernel refuses is the
// typed refusal.
func (l *LayoutRemote) CommitPendingSnapshot(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.takeAbandonLocked()
	if l.pending == nil {
		return nil
	}
	p := l.pending
	if err := layout.CheckSnapshot(p.name, p.meta); err != nil {
		l.pending = nil
		return err
	}
	if err := l.openSessionLocked(ctx); err != nil {
		return err
	}
	h, found, err := l.st.ReadHead(ctx)
	if err != nil {
		return layoutRemoteErr(err)
	}
	if !found {
		return layout.ErrHeadAbsent
	}
	l.viewLocked(h)
	m := map[uint64]layout.Ref{}
	if h.Current != nil {
		if m, err = l.manifest(ctx, *h.Current); err != nil {
			return err
		}
	}
	if err := l.writeRootLocked(ctx, m); err != nil {
		return err
	}
	got, err := layout.RunCommit(ctx, l.st, l.st, l.vol.Name, l.me, layout.CommitRequest{Session: l.session, Prev: m, Next: m, Position: h.Position, State: layout.StateLive, Snapshot: p.name, Meta: p.meta})
	if err != nil {
		if snapshotRefusal(err) {
			l.pending = nil
			return err
		}
		return layoutRemoteErr(err)
	}
	// The image is the one the engine already holds (same root and
	// position): an own write that leaves the engine's view where it is.
	// On a volume never cut it is HEAD's first root (the empty image): seen
	// takes it too, so a later write by anyone else that leaves this image
	// in place does not move visible under the engine's base.
	l.session, l.pending, l.lastCommit = "", nil, got
	l.ownGen, l.seen = got.Gen, imageOf(got)
	return nil
}

// TakeSnapshotErr returns, and clears, why the last commit could not name
// its snapshot (nil when it did, or none was asked).
func (l *LayoutRemote) TakeSnapshotErr() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	err := l.snapErr
	l.snapErr = nil
	return err
}

// The outcomes of AbortOpenSession.
const (
	SessionDetached = "detached" // HEAD is this holder's detached commit: the stop is done
	SessionAborted  = "aborted"  // an open session of this holder was rolled back
	SessionIdle     = "idle"     // nothing of this holder was open
)

// AbortOpenSession settles this holder's open transaction before a stop
// resumes serving or retries. It takes no lock of the remote, so it never
// waits behind a commit in flight (the very one it may abort) and honours
// ctx; the kernel's conditional writes decide between them: an abort that
// lands first makes the commit's update fail, a commit that lands first is
// what HEAD then reports. A cleaning of this holder that HEAD shows open is
// ended, then a session of this holder is rolled back (held here or left by
// a dead process); the remote drops the session and its pending name at its
// next call. HEAD of another holder, epoch or claim id is a fence, never
// idle.
func (l *LayoutRemote) AbortOpenSession(ctx context.Context) (string, error) {
	for {
		h, found, err := l.st.ReadHead(ctx)
		if err != nil {
			return "", layoutRemoteErr(err)
		}
		if !found {
			return "", layout.ErrHeadAbsent
		}
		if h.Epoch != l.me.Epoch || h.Holder != l.me.Holder || h.ClaimID != l.me.ClaimID {
			return "", fmt.Errorf("%w: HEAD is held by %s at epoch %d (claim %q)", storage.ErrChunkLayerFenced, h.Holder, h.Epoch, h.ClaimID)
		}
		switch {
		case h.State == layout.StateDetached:
			return SessionDetached, nil
		case h.State != layout.StateLive:
			return "", layoutRemoteErr(fmt.Errorf("%w: HEAD is %s", layout.ErrVolumeDeleting, h.State))
		case h.Activity.Kind == layout.ActClean:
			if _, err := layout.RunEndClean(ctx, l.st, l.me, h.Activity.Session); err != nil && !errors.Is(err, layout.ErrPrecondition) {
				return "", layoutRemoteErr(err)
			}
			continue // read HEAD again: a session may follow
		case h.Activity.Kind == layout.ActSnap:
			if _, err := layout.RunAbort(ctx, l.st, l.me, h.Activity.Session); err != nil {
				if errors.Is(err, layout.ErrPrecondition) {
					continue // HEAD moved (the commit landed): read it again
				}
				return "", layoutRemoteErr(err)
			}
			l.abandoned.Store(true)
			return SessionAborted, nil
		}
		return SessionIdle, nil
	}
}

// ReadHead reads the volume's HEAD as the kernel holds it (a stop's
// unknown outcome is decided from it: detached at my epoch and position).
func (l *LayoutRemote) ReadHead(ctx context.Context) (layout.Head, error) {
	h, found, err := l.st.ReadHead(ctx)
	if err != nil {
		return layout.Head{}, err
	}
	if !found {
		return layout.Head{}, layout.ErrHeadAbsent
	}
	return h, nil
}

// LastCommit is HEAD as this remote's last commit left it (for a stop's
// unknown outcome, read HEAD again).
func (l *LayoutRemote) LastCommit() layout.Head {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastCommit
}

// Clean runs one cleaning of this holder between sessions: clean(c) opens
// HEAD, run removes unreachable names under session c, and clean(c) ends.
// A cut or a stop that needs HEAD meanwhile cancels run's context and
// waits for it to end: neither waits on a cleaning nor fails busy. A pass
// cancelled between its mark and its delete leaves a mark the next
// cleaning takes over (the removal plans every found name).
func (l *LayoutRemote) Clean(ctx context.Context, run func(ctx context.Context, me layout.HolderID, session string) error) error {
	l.mu.Lock()
	l.takeAbandonLocked()
	if l.session != "" || l.cleaning != "" {
		l.mu.Unlock()
		return layout.ErrBusy
	}
	if _, err := l.rollBackLocked(ctx); err != nil {
		l.mu.Unlock()
		return err
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		l.mu.Unlock()
		return err
	}
	c := "c-" + hex.EncodeToString(b[:])
	bh, err := layout.RunBeginClean(ctx, l.st, l.me, c)
	if err != nil {
		l.mu.Unlock()
		return layoutRemoteErr(err)
	}
	l.ownGen = bh.Gen
	cctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	l.cleaning, l.cleanCancel, l.cleanDone = c, cancel, done
	l.mu.Unlock()

	runErr := run(cctx, l.me, c)
	// Bounded by the caller's context: a stop on a hung filer does not
	// wait for the close; a cleaning left open is ended by the next cut
	// (rollBackLocked) or the holder's next claim.
	eh, endErr := layout.RunEndClean(ctx, l.st, l.me, c)
	cancel()

	l.mu.Lock()
	if endErr == nil {
		l.ownGen = eh.Gen
	}
	l.cleaning, l.cleanCancel = "", nil
	close(done)
	l.mu.Unlock()
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return layoutRemoteErr(endErr)
}

// syncSessionLocked drops the in-memory session unless HEAD still shows it
// open: a commit that applied and then lost its reply, or a session another
// actor ended, never leaves a stale session behind.
func (l *LayoutRemote) syncSessionLocked(h layout.Head) {
	if l.session != "" && h.Activity != (layout.Activity{Kind: layout.ActSnap, Session: l.session}) {
		l.session, l.forceView = "", true
	}
}

// viewLocked takes HEAD h as read: a generation this remote did not write
// itself (someone else's write, or the first read) becomes the engine's
// view; the session is re-derived from h.
func (l *LayoutRemote) viewLocked(h layout.Head) {
	l.syncSessionLocked(h)
	// head-generation-cache-exception: imageOf controls view changes
	if h.Gen != l.ownGen || l.forceView {
		if t := imageOf(h); l.forceView || l.visible == 0 || t != l.seen {
			l.visible, l.seen = h.Gen, t
		}
		l.ownGen, l.forceView = h.Gen, false
	}
}

// imageTuple is what the engine's view of HEAD depends on: the image and
// who holds it, not the snapshot table.
type imageTuple struct {
	current  layout.RootName
	hasRoot  bool
	position uint64
	state    string
	epoch    uint64
	holder   string
	claimID  string
}

func imageOf(h layout.Head) imageTuple {
	t := imageTuple{position: h.Position, state: h.State, epoch: h.Epoch, holder: h.Holder, claimID: h.ClaimID}
	if h.Current != nil {
		t.current, t.hasRoot = *h.Current, true
	}
	return t
}

// Abort is the engine's notice that a cut ended without a commit. It
// never blocks: the next call drops the session (HEAD's read then rolls
// it back) and the pending snapshot name, which never rides onto a later
// cut.
func (l *LayoutRemote) Abort() { l.abandoned.Store(true) }

func (l *LayoutRemote) takeAbandonLocked() {
	if l.abandoned.Swap(false) {
		// Uploads the engine staged in the dropped session are caught by the
		// commit (not prepared), which forces the view then.
		l.session, l.pending = "", nil
	}
}

// stopCleaningLocked cancels this process's open cleaning and waits for
// it to end, at most until ctx ends (it releases mu while waiting).
func (l *LayoutRemote) stopCleaningLocked(ctx context.Context) error {
	for l.cleaning != "" {
		cancel, done := l.cleanCancel, l.cleanDone
		cancel()
		l.mu.Unlock()
		select {
		case <-done:
		case <-ctx.Done():
			l.mu.Lock()
			return fmt.Errorf("%w: waiting for this process's cleaning to end: %w", storage.ErrChunkRemoteUnavailable, ctx.Err())
		}
		l.mu.Lock()
	}
	return nil
}

// layoutRemoteErr maps a kernel result to the engine's action (the seam's
// closure table):
//   - not the holder, a stale epoch, detached: fenced, final (no retry);
//   - generation moved, my session gone: HEAD precondition (the engine
//     re-reads HEAD and decides);
//   - the volume is being deleted: HEAD deleted (the engine stops);
//   - my own cleaning open (busy): remote unavailable (retry later; never a
//     fence);
//   - damage (corrupt, missing, another volume's id): ErrRemoteDamaged, a
//     hard reported error, never a retry;
//   - any other refusal is returned as it is;
//   - the store does not answer: remote unavailable (the session is
//     dropped locally; the next cut rolls it back).
//
// LayoutErr classifies a layout error from a call made outside the remote
// (the holder's cleaning) the way the remote classifies its own: an
// identity-moved refusal is storage.ErrChunkLayerFenced, a volume being
// deleted storage.ErrChunkHeadDeleted.
func LayoutErr(err error) error { return layoutRemoteErr(err) }

func layoutRemoteErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, layout.ErrNotHolder), errors.Is(err, layout.ErrStaleEpoch), errors.Is(err, layout.ErrDetached),
		errors.Is(err, layout.ErrPositionBehind):
		return fmt.Errorf("%w: %w", storage.ErrChunkLayerFenced, err)
	case errors.Is(err, layout.ErrPrecondition), errors.Is(err, layout.ErrNoSession):
		return fmt.Errorf("%w: %w", storage.ErrChunkHeadPrecondition, err)
	case errors.Is(err, layout.ErrVolumeDeleting):
		return fmt.Errorf("%w: %w", storage.ErrChunkHeadDeleted, err)
	case errors.Is(err, layout.ErrBusy):
		return fmt.Errorf("%w: %w", storage.ErrChunkRemoteUnavailable, err)
	case errors.Is(err, layout.ErrCorrupt), errors.Is(err, layout.ErrIdentity), errors.Is(err, layout.ErrMissing):
		return fmt.Errorf("%w: %w", ErrRemoteDamaged, err)
	case isKernelRefusal(err):
		return err
	}
	return fmt.Errorf("%w: %w", storage.ErrChunkRemoteUnavailable, err)
}
