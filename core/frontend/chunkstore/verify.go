package chunkstore

// Verify: a read-only check of one volume's remote image against what its
// HEAD (and, on request, its snapshots) say. It writes nothing to the filer
// or the blob store: no intent, no mark, no tombstone, no binding.
//
// Depths, each including the one before:
//
//   - meta: the HEAD root decodes and is self-consistent; its geometry
//     matches VOLUME; a snapshot's document names the volume and the
//     snapshot it was found under; every segment a selected root names is
//     readable and passes its SHA-256 and strict decode; a fork's record
//     matches the parent snapshot it was made from. References to one
//     chunk with different SHA-256s disagree (CONTENT_MISMATCH).
//   - bindings (the default): every chunk key a selected root references
//     has a sound binding in its owner's keys/ (one listing per owner),
//     not tombstoned by GC.
//   - data: every distinct chunk (owner + key, across all selected roots)
//     is fetched once; its length must be the chunk size and its SHA-256
//     each reference's (the rule the chunk layer applies on a read miss).
//
// The pass runs beside a live owner and beside GC, so what it reports about
// chunks comes from a small automaton (the V.* names on the functions):
//
//   - V.Discover: a chunk (owner, key) a root view names that the pass has
//     not seen is queued for one check.
//   - V.Settle: the check records the chunk's outcome and reports nothing;
//     a settled chunk is never checked again.
//   - V.Refresh: HEAD is read again at least every RefreshInterval; a newer
//     generation replaces the HEAD view (an older one is ignored), the keys
//     the old view named and the new one does not are marked dropped, and
//     its new keys are discovered.
//   - V.Round: once every queued chunk is settled, one closing read of HEAD
//     and of each selected snapshot's state; if it names a chunk not seen,
//     the pass checks it and reads again, at most ClosingRounds times
//     (V.GiveUp: then the pass is incomplete).
//   - V.Close: a closing read that names no chunk not seen is the final view.
//   - V.Reconcile: the findings about chunks and segments are computed from
//     the outcomes and the final view alone, nowhere else: a bad chunk is
//     one finding under the first final root naming its key (HEAD, then the
//     snapshots not gone, in order) at that root's first index; a key HEAD
//     dropped and names again is ROOT_INVALID, whatever its outcome.
//
// The pass therefore reports the state of each named chunk at its check,
// for the roots at the final read; a chunk damaged after its check is the
// next pass's finding. A chunk that left HEAD can be collected after GC's
// grace, and a deleted snapshot's chunks are kept only for the grace, so a
// bad chunk that no root names at the final read is not a finding.
//
// A needle is NEEDLE_MISSING only when the store reports it absent
// (ErrBlobNotFound). Any other read failure is retried; if it persists it is
// READ_ERROR, which means "not verified", not "damaged".
//
// A finding never stops the pass. Only an inability to continue does (the
// context ended, the filer failed) or a HEAD that keeps naming new chunks:
// Verify then returns the result with Complete false and the reason in
// Incomplete, the findings computed from what was checked, and the error.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// VerifyDepth selects how far Verify checks.
type VerifyDepth string

const (
	VerifyMeta     VerifyDepth = "meta"
	VerifyBindings VerifyDepth = "bindings"
	VerifyData     VerifyDepth = "data"
)

const (
	// DefaultVerifyConcurrency bounds the chunk reads in flight.
	DefaultVerifyConcurrency = 2
	// DefaultVerifyBytesPerSecond paces chunk reads.
	DefaultVerifyBytesPerSecond int64 = 32 << 20
	// DefaultVerifyRefresh is the longest a pass works from one HEAD view;
	// it stays below the operation deadline, the shortest time a chunk
	// that just left HEAD is still guaranteed to be kept by GC.
	DefaultVerifyRefresh = 2 * time.Minute
	// DefaultVerifyIntentStaleAge is the age past which a capture intent
	// below HEAD's epoch is reported as stale (the gc --intent-ttl default).
	DefaultVerifyIntentStaleAge = time.Hour
	// DefaultVerifyClosingRounds is the default ClosingRounds.
	DefaultVerifyClosingRounds = 3
)

// Finding classes.
const (
	FindingRootInvalid        = "ROOT_INVALID"
	FindingGeometry           = "GEOMETRY"
	FindingSegmentUnreadable  = "SEGMENT_UNREADABLE"
	FindingBindingMissing     = "BINDING_MISSING"
	FindingBindingTombstoned  = "BINDING_TOMBSTONED"
	FindingNeedleMissing      = "NEEDLE_MISSING"
	FindingContentMismatch    = "CONTENT_MISMATCH"
	FindingSizeMismatch       = "SIZE_MISMATCH"
	FindingForkRecordMismatch = "FORK_RECORD_MISMATCH"
	FindingIntentStale        = "INTENT_STALE"
	// FindingReadError is not proof of damage: a needle (chunk or segment)
	// could not be read for a reason other than the store reporting it
	// absent, after retries. That object was not verified; re-run.
	FindingReadError = "READ_ERROR"
)

// A read the store does not answer with "not found" is tried this many
// times in all, the retries spaced by verifyRetryBackoff times the attempt.
const verifyReadAttempts = 3

var verifyRetryBackoff = 50 * time.Millisecond

// VerifyOptions configures a Verify pass. Zero values take the defaults.
type VerifyOptions struct {
	// Depth is meta, bindings (default) or data.
	Depth VerifyDepth
	// Snapshot adds the named snapshot to the roots checked; AllSnapshots
	// adds every published snapshot not marked deleting. HEAD is always
	// checked. At most one of the two may be set.
	Snapshot     string
	AllSnapshots bool
	// Concurrency bounds the chunk reads in flight (depth data).
	Concurrency int
	// BytesPerSecond paces chunk reads: 0 is the default, a negative value
	// leaves them unpaced.
	BytesPerSecond int64
	// RefreshInterval is the longest the pass works from one HEAD view.
	RefreshInterval time.Duration
	// IntentStaleAge is the age past which a capture intent below HEAD's
	// epoch that names no key HEAD references is reported (a hint only).
	IntentStaleAge time.Duration
	// ClosingRounds bounds the closing reads that may name chunks not seen
	// before; past it the pass is incomplete.
	ClosingRounds int

	// hook, when set, runs at the named stages of a pass (a test seam).
	hook func(stage string)
}

// Verify stages passed to the hook.
const (
	verifyHookHeadWalked   = "head-walked"   // the first HEAD view has been read and walked
	verifyHookRootsWalked  = "roots-walked"  // the selected snapshots have been read and walked too
	verifyHookBatchChecked = "batch-checked" // a batch of checks has settled
	verifyHookClosingRead  = "closing-read"  // a closing read is about to be made
)

// VerifyIncomplete is why a pass did not complete.
type VerifyIncomplete string

const (
	VerifyIncompleteDeadline  VerifyIncomplete = "deadline"                 // the caller's deadline passed
	VerifyIncompleteCancelled VerifyIncomplete = "cancelled"                // the caller cancelled
	VerifyIncompleteRounds    VerifyIncomplete = "closing_rounds_exhausted" // every closing read named new chunks
	VerifyIncompleteError     VerifyIncomplete = "error"                    // a filer or blob store failure, or a refused request
)

// CodeVerifyIncomplete: every closing read named chunks not seen before.
const CodeVerifyIncomplete = "verify_incomplete"

// VerifyFinding is one problem found. Segment is set for a segment's own
// problem, Index for a chunk's.
type VerifyFinding struct {
	Volume  string  `json:"volume"`
	Root    string  `json:"root"`
	Segment *uint64 `json:"segment,omitempty"`
	Index   *uint64 `json:"index,omitempty"`
	Owner   string  `json:"owner,omitempty"`
	Key     string  `json:"key,omitempty"`
	FID     string  `json:"fid,omitempty"`
	Class   string  `json:"class"`
	Detail  string  `json:"detail"`
}

// VerifyResult is a pass's summary and findings. Complete is false when the
// pass could not finish (Incomplete says why); the counters and findings
// then cover what it checked.
type VerifyResult struct {
	VolumeID string      `json:"volumeID"`
	Depth    VerifyDepth `json:"depth"`
	Complete bool        `json:"complete"`
	// Incomplete is why the pass did not complete ("" when it did).
	Incomplete VerifyIncomplete `json:"incomplete,omitempty"`
	// HeadGens are the HEAD generations the pass read, in order; the last
	// is the final view's.
	HeadGens []uint64 `json:"headGens"`
	// Snapshots are the snapshots checked, as name/attempt.
	Snapshots []string `json:"snapshots"`
	// SegmentsChecked counts segment references checked (per root walked);
	// SegmentReads the segment objects actually read.
	SegmentsChecked int `json:"segmentsChecked"`
	SegmentReads    int `json:"segmentReads"`
	// ChunksChecked counts the chunk references (root, index) of the final
	// view; DistinctChunks the distinct chunks (owner, key) the pass saw.
	ChunksChecked  int `json:"chunksChecked"`
	DistinctChunks int `json:"distinctChunks"`
	// DistinctChunksRead and BytesRead count the chunk data read.
	DistinctChunksRead int   `json:"distinctChunksRead"`
	BytesRead          int64 `json:"bytesRead"`
	// SupersededDuringVerify counts bad chunks and segments no root names
	// at the final read (not findings); RootDeletedDuringVerify those of
	// them that a selected snapshot deleted during the pass names.
	SupersededDuringVerify  int `json:"supersededDuringVerify"`
	RootDeletedDuringVerify int `json:"rootDeletedDuringVerify"`
	// SnapshotsDeletedDuringVerify lists, as name/attempt, the checked
	// snapshots gone at the final read: they are not fully covered.
	SnapshotsDeletedDuringVerify []string `json:"snapshotsDeletedDuringVerify"`
	// ForkSourceGone: this volume is a fork whose source volume no longer
	// exists, so its fork record could not be compared with the parent
	// snapshot. Not a finding.
	ForkSourceGone bool `json:"forkSourceGone,omitempty"`
	// ChunksNotRead counts the READ_ERROR findings: chunks or segments that
	// could not be read, so were not verified. ReadRetries counts the
	// retried reads.
	ChunksNotRead   int             `json:"chunksNotRead"`
	ReadRetries     int             `json:"readRetries"`
	FindingsByClass map[string]int  `json:"findingsByClass"`
	Findings        []VerifyFinding `json:"findings"`
}

// Verify checks vol's remote image at opts.Depth; see the top of this file.
func (c *Catalog) Verify(ctx context.Context, vol string, opts VerifyOptions) (VerifyResult, error) {
	res := VerifyResult{VolumeID: vol, HeadGens: []uint64{}, Snapshots: []string{}, SnapshotsDeletedDuringVerify: []string{},
		FindingsByClass: map[string]int{}, Findings: []VerifyFinding{}}
	refuse := func(err error) (VerifyResult, error) {
		res.Incomplete = VerifyIncompleteError
		return res, err
	}
	if opts.Depth == "" {
		opts.Depth = VerifyBindings
	}
	res.Depth = opts.Depth
	if opts.Depth != VerifyMeta && opts.Depth != VerifyBindings && opts.Depth != VerifyData {
		return refuse(catErr(CodeInvalidName, false, nil, "verify depth %q: meta, bindings or data", opts.Depth))
	}
	if opts.Snapshot != "" && opts.AllSnapshots {
		return refuse(catErr(CodeInvalidName, false, nil, "verify: a named snapshot and all snapshots do not combine"))
	}
	if opts.Snapshot != "" {
		if err := CheckSnapshotName(opts.Snapshot); err != nil {
			return refuse(err)
		}
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultVerifyConcurrency
	}
	if opts.BytesPerSecond == 0 {
		opts.BytesPerSecond = DefaultVerifyBytesPerSecond
	}
	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = DefaultVerifyRefresh
	}
	if opts.IntentStaleAge <= 0 {
		opts.IntentStaleAge = DefaultVerifyIntentStaleAge
	}
	if opts.ClosingRounds <= 0 {
		opts.ClosingRounds = DefaultVerifyClosingRounds
	}
	root, err := c.volRoot(vol)
	if err != nil {
		return refuse(err)
	}
	v := &verifier{c: c, vol: vol, root: root, opts: opts, res: &res, blobs: c.blobs,
		segs: map[verifySegKey]*verifiedSegment{}, chunks: map[string]*verifyChunk{}, dropped: map[string]bool{},
		pacer: &verifyPacer{rate: opts.BytesPerSecond}}
	err = v.run(ctx)
	v.reconcile()
	sort.SliceStable(res.Findings, func(i, j int) bool { return findingLess(res.Findings[i], res.Findings[j]) })
	for _, f := range res.Findings {
		res.FindingsByClass[f.Class]++
	}
	res.DistinctChunks = len(v.chunks)
	if err != nil {
		switch {
		case CodeOf(err) == CodeVerifyIncomplete:
			res.Incomplete = VerifyIncompleteRounds
		case errors.Is(err, context.DeadlineExceeded):
			res.Incomplete = VerifyIncompleteDeadline
		case errors.Is(err, context.Canceled):
			res.Incomplete = VerifyIncompleteCancelled
		default:
			res.Incomplete = VerifyIncompleteError
		}
		return res, err
	}
	res.Complete = true
	return res, nil
}

// verifySegKey names a segment reference: a segment object is immutable, so
// one read under the same reference needs no second read.
type verifySegKey struct {
	group, chunkBytes uint64
	ref               storage.ChunkSegment
}

// verifiedSegment is a segment as read once: its chunks, or the problems
// that make it unusable (issue) or doomed (tombstoned binding).
type verifiedSegment struct {
	chunks     map[uint64]storage.ChunkRef
	fid        string
	tombstoned bool
	issue      *segIssue
}

// segIssue is why a segment could not be used.
type segIssue struct {
	class, fid, detail string
}

// verifyChunk is one distinct chunk (owner volume + the owner's own key):
// unseen while absent from the verifier's map, then queued, then settled.
type verifyChunk struct {
	id         string
	owner, key string
	fid        string
	bound      bool // a sound, untombstoned binding was found

	// The outcome, under the verifier's lock until the checks are joined:
	// class and detail of a bad outcome (binding missing or tombstoned,
	// needle missing, read error, size mismatch), or the SHA-256 of the
	// content read (read).
	settled       bool
	class, detail string
	read          bool
	digest        [32]byte
}

// verifyView is one root as last read: its references by index, and its
// root document (for the segments it names).
type verifyView struct {
	label  string
	h      storage.ChunkHead
	chunks map[uint64]storage.ChunkRef
	// snapshots only: the identity they were found under, and whether the
	// final round read them gone.
	name, attempt string
	gone          bool
}

type verifier struct {
	c     *Catalog
	vol   string
	root  string
	opts  VerifyOptions
	blobs Blobs
	pacer *verifyPacer

	geo        Geometry
	chunkBytes uint64
	nChunks    uint64

	mu      sync.Mutex
	res     *VerifyResult
	segs    map[verifySegKey]*verifiedSegment // segments read, by reference
	chunks  map[string]*verifyChunk           // the chunks seen, by identity
	pending []*verifyChunk                    // queued, not yet checked
	listed  bool                              // the owners' keys/ were listed once

	head        *verifyView     // the HEAD view; nil without a decodable HEAD
	dropped     map[string]bool // keys a HEAD view named and a later one did not
	snaps       []*verifyView   // the selected snapshots, in order
	lastRefresh time.Time
	badRefresh  bool
}

func (v *verifier) hook(stage string) {
	if v.opts.hook != nil {
		v.opts.hook(stage)
	}
}

func (v *verifier) finding(f VerifyFinding) {
	f.Volume = v.vol
	v.mu.Lock()
	v.res.Findings = append(v.res.Findings, f)
	if f.Class == FindingReadError {
		v.res.ChunksNotRead++
	}
	v.mu.Unlock()
}

func isContextErr(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// fetch reads one needle. A needle the store reports absent (ErrBlobNotFound)
// and a context error are returned at once; any other failure is retried,
// up to verifyReadAttempts reads in all, before it is returned.
func (v *verifier) fetch(ctx context.Context, fid string) ([]byte, error) {
	for attempt := 1; ; attempt++ {
		data, err := v.blobs.Get(ctx, fid)
		if err == nil || errors.Is(err, ErrBlobNotFound) || isContextErr(ctx, err) || attempt == verifyReadAttempts {
			return data, err
		}
		v.mu.Lock()
		v.res.ReadRetries++
		v.mu.Unlock()
		t := time.NewTimer(verifyRetryBackoff * time.Duration(attempt))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return nil, ctx.Err()
		}
	}
}

// readErrorDetail describes a read that failed for a reason other than "not found".
func readErrorDetail(err error) string {
	return fmt.Sprintf("not verified: could not read the needle in %d attempts (re-run): %v", verifyReadAttempts, err)
}

// verifyPrefetched serves one needle already read, so the shared segment
// decoder verifies exactly the bytes fetch returned.
type verifyPrefetched struct {
	fid  string
	body []byte
}

func (p verifyPrefetched) Put(context.Context, []byte) (string, error) {
	return "", errors.New("chunkstore: verify is read-only")
}

func (p verifyPrefetched) Get(_ context.Context, fid string) ([]byte, error) {
	if fid != p.fid {
		return nil, fmt.Errorf("chunkstore: verify: unexpected needle %s", fid)
	}
	return p.body, nil
}

// abortErr turns a failure that stops the pass into its error: a context
// error stays one (the CLI reports deadline_exceeded), anything else from
// the filer is remote_unavailable.
func abortErr(ctx context.Context, err error, format string, a ...any) error {
	if ctx.Err() != nil {
		return fmt.Errorf("chunkstore: verify: %s: %w", fmt.Sprintf(format, a...), ctx.Err())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("chunkstore: verify: %s: %w", fmt.Sprintf(format, a...), err)
	}
	if CodeOf(err) != "" {
		return err
	}
	return remoteErr(err, format, a...)
}

func headLabel(gen uint64) string { return fmt.Sprintf("HEAD gen %d", gen) }

func (v *verifier) run(ctx context.Context) error {
	vi, ok, err := v.c.ReadVolume(ctx, v.vol)
	if err != nil {
		if CodeOf(err) == CodeRemoteUnavailable || ctx.Err() != nil {
			return abortErr(ctx, err, "read VOLUME of %s", v.vol)
		}
		v.finding(VerifyFinding{Root: "VOLUME", Class: FindingRootInvalid, Detail: err.Error()})
		return nil
	}
	if !ok {
		return catErr(CodeNotChunkVolume, false, nil, "volume %s has no VOLUME entry", v.vol)
	}
	v.geo = vi.Geometry
	v.chunkBytes = uint64(v.geo.ChunkBlocks) * uint64(v.geo.BlockSize)
	if v.geo.ChunkBlocks > 0 {
		v.nChunks = (v.geo.NumBlocks + uint64(v.geo.ChunkBlocks) - 1) / uint64(v.geo.ChunkBlocks)
	}
	if v.chunkBytes == 0 {
		v.finding(VerifyFinding{Root: "VOLUME", Class: FindingGeometry, Detail: fmt.Sprintf("VOLUME geometry %+v has no chunk size", v.geo)})
	}
	if err := v.forkRecord(ctx, vi); err != nil {
		return err
	}

	// HEAD: the root once, then the segments it names.
	e, err := v.c.f.LookupEntry(ctx, v.root, headName)
	switch {
	case errors.Is(err, ErrNotFound):
		v.finding(VerifyFinding{Root: "HEAD", Class: FindingRootInvalid, Detail: "the volume has a VOLUME entry but no HEAD"})
	case err != nil:
		return abortErr(ctx, err, "read HEAD of %s", v.vol)
	default:
		h, derr := decodeHead(path.Join(v.root, headName), e)
		if derr != nil {
			v.finding(VerifyFinding{Root: "HEAD", Class: FindingRootInvalid, Detail: derr.Error()})
			break
		}
		v.lastRefresh = v.c.now()
		if _, err := v.refresh(ctx, h); err != nil {
			return err
		}
		if err := v.intents(ctx, h, v.head.chunks); err != nil {
			return err
		}
	}
	v.hook(verifyHookHeadWalked)

	if err := v.snapshots(ctx); err != nil {
		return err
	}
	v.hook(verifyHookRootsWalked)
	for rounds := 0; ; {
		if err := v.drain(ctx); err != nil {
			return err
		}
		named, err := v.round(ctx)
		if err != nil {
			return err
		}
		if !named {
			return nil // V.Close: this read is the final view
		}
		if rounds++; rounds == v.opts.ClosingRounds {
			return catErr(CodeVerifyIncomplete, true, nil, // V.GiveUp
				"verify of %s: each of %d closing reads of HEAD named chunks not seen before; the pass is incomplete (re-run when the volume is quieter)",
				v.vol, rounds)
		}
	}
}

// drain checks every queued chunk, batch by batch, refreshing HEAD between
// batches (a refresh can queue more).
func (v *verifier) drain(ctx context.Context) error {
	for len(v.pending) > 0 {
		batch := v.pending
		v.pending = nil
		if err := v.checkBindings(ctx, batch, !v.listed); err != nil {
			return err
		}
		v.listed = true
		if v.opts.Depth == VerifyData {
			if err := v.checkData(ctx, batch); err != nil {
				return err
			}
		}
		v.hook(verifyHookBatchChecked)
		if err := v.maybeRefresh(ctx); err != nil {
			return err
		}
	}
	return nil
}

// V.Round: once every queued chunk is settled, read HEAD and each selected snapshot's state once; report whether HEAD named a chunk not seen.
func (v *verifier) round(ctx context.Context) (bool, error) {
	v.hook(verifyHookClosingRead)
	named := false
	if v.head != nil {
		var err error
		if named, err = v.readHead(ctx); err != nil {
			return false, err
		}
	}
	for _, s := range v.snaps {
		active, err := v.snapshotActive(ctx, s)
		if err != nil {
			return false, err
		}
		s.gone = !active
	}
	return named, nil
}

// ---- roots and segments ----

// segment returns h's segment group as read once by this pass (keys
// owner-qualified, verified through the decoder GC and the chunk layer
// use), reading it the first time. An error stops the pass.
func (v *verifier) segment(ctx context.Context, h storage.ChunkHead, group uint64) (*verifiedSegment, error) {
	ref := h.Segments[group]
	k := verifySegKey{group: group, chunkBytes: h.ChunkBytes, ref: ref}
	if s := v.segs[k]; s != nil {
		return s, nil
	}
	s, err := v.readSegment(ctx, h, group, ref)
	if err != nil {
		return nil, err
	}
	v.segs[k] = s
	return s, nil
}

func (v *verifier) readSegment(ctx context.Context, h storage.ChunkHead, group uint64, ref storage.ChunkSegment) (*verifiedSegment, error) {
	if v.blobs == nil {
		return nil, errors.New("chunkstore: verify needs a blob reader for segmented roots")
	}
	r, err := NewChunkRemote(v.c.f, v.blobs, v.root)
	if err != nil {
		return nil, err
	}
	dir, name, err := r.keyLocation(ref.Key)
	if err != nil {
		return &verifiedSegment{issue: &segIssue{FindingSegmentUnreadable, "", err.Error()}}, nil
	}
	e, err := v.c.f.LookupEntry(ctx, dir, name)
	if errors.Is(err, ErrNotFound) {
		return &verifiedSegment{issue: &segIssue{FindingSegmentUnreadable, "", "segment binding not found"}}, nil
	}
	if err != nil {
		return nil, abortErr(ctx, err, "read segment binding %s/%s", dir, name)
	}
	if bad := bindingProblem(e); bad != "" {
		return &verifiedSegment{issue: &segIssue{FindingSegmentUnreadable, string(e.Extended[extFid]), "segment binding: " + bad}}, nil
	}
	s := &verifiedSegment{fid: string(e.Extended[extFid]), tombstoned: len(e.Extended[extGCDeleted]) != 0}
	v.mu.Lock()
	v.res.SegmentReads++
	v.mu.Unlock()
	body, err := v.fetch(ctx, s.fid)
	switch {
	case err == nil:
	case isContextErr(ctx, err):
		return nil, abortErr(ctx, err, "read segment %s", ref.Key)
	case errors.Is(err, ErrBlobNotFound):
		s.issue = &segIssue{FindingSegmentUnreadable, s.fid, "segment needle not found: " + err.Error()}
		return s, nil
	default:
		s.issue = &segIssue{FindingReadError, s.fid, readErrorDetail(err)}
		return s, nil
	}
	r, err = NewChunkRemote(v.c.f, verifyPrefetched{s.fid, body}, v.root)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.fids[ref.Key] = s.fid
	r.mu.Unlock()
	one := storage.ChunkHead{Segments: map[uint64]storage.ChunkSegment{group: ref}, ChunkBytes: h.ChunkBytes,
		ManifestChunkCount: ref.Count, RemoteLSN: h.RemoteLSN, State: storage.ChunkHeadActive}
	resolved, err := r.resolveManifest(ctx, one)
	if err != nil {
		s.issue = &segIssue{FindingSegmentUnreadable, s.fid, err.Error()}
		return s, nil
	}
	s.chunks = resolved.Chunks
	return s, nil
}

// bindingProblem is what makes e unusable as a chunk key binding, or "".
func bindingProblem(e *filerpb.Entry) string {
	switch {
	case e.GetIsDirectory():
		return "is a directory"
	case len(e.GetChunks()) != 0:
		return fmt.Sprintf("carries %d filer chunks", len(e.GetChunks()))
	case string(e.Extended[extKind]) != kindChunkKey:
		return fmt.Sprintf("kind %q, want %q", e.Extended[extKind], kindChunkKey)
	case len(e.Extended[extFid]) == 0:
		return "no fid"
	}
	return ""
}

// walk reads the segments root h names and returns its chunk references by
// index, and the segment groups it could not use (their chunks are unknown).
func (v *verifier) walk(ctx context.Context, h storage.ChunkHead) (map[uint64]storage.ChunkRef, map[uint64]bool, error) {
	out := map[uint64]storage.ChunkRef{}
	unusable := map[uint64]bool{}
	if h.Segments == nil {
		for idx, ref := range h.Chunks {
			out[idx] = ref
		}
		return out, unusable, nil
	}
	groups := make([]uint64, 0, len(h.Segments))
	for g := range h.Segments {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
	for _, g := range groups {
		v.mu.Lock()
		v.res.SegmentsChecked++
		v.mu.Unlock()
		s, err := v.segment(ctx, h, g)
		if err != nil {
			return out, unusable, err
		}
		if s.issue != nil {
			unusable[g] = true
			continue
		}
		for idx, cr := range s.chunks {
			out[idx] = cr
		}
	}
	return out, unusable, nil
}

// geometry reports a root's chunk size and chunk indices against VOLUME.
func (v *verifier) geometry(label string, h storage.ChunkHead, chunks map[uint64]storage.ChunkRef) {
	if h.Segments != nil && h.ChunkBytes != v.chunkBytes {
		v.finding(VerifyFinding{Root: label, Class: FindingGeometry,
			Detail: fmt.Sprintf("root chunk_bytes %d, VOLUME chunk size %d", h.ChunkBytes, v.chunkBytes)})
	}
	for _, idx := range sortedIndexes(chunks) {
		if idx >= v.nChunks {
			i := idx
			owner, key := splitIdentity(chunkIdentity(v.vol, chunks[idx].Key))
			v.finding(VerifyFinding{Root: label, Index: &i, Owner: owner, Key: key, Class: FindingGeometry,
				Detail: fmt.Sprintf("chunk index %d outside the volume (%d chunks)", idx, v.nChunks)})
		}
	}
}

// V.Discover: queue one check of each chunk the references name that this pass has not seen; report whether there was one.
func (v *verifier) discover(chunks map[uint64]storage.ChunkRef) bool {
	found := false
	for _, idx := range sortedIndexes(chunks) {
		id := chunkIdentity(v.vol, chunks[idx].Key)
		if v.chunks[id] != nil {
			continue
		}
		owner, key := splitIdentity(id)
		w := &verifyChunk{id: id, owner: owner, key: key}
		v.chunks[id] = w
		found = true
		if v.opts.Depth == VerifyMeta {
			w.settled = true // nothing to check at this depth
			continue
		}
		v.pending = append(v.pending, w)
	}
	return found
}

// V.Settle: record w's outcome (a bad outcome's class and detail, or none); report nothing.
func (v *verifier) settle(w *verifyChunk, class, detail string) {
	v.mu.Lock()
	w.settled, w.class, w.detail = true, class, detail
	v.mu.Unlock()
}

// snapshotActive reads snapshot s's state again (part of V.Round): its name
// record must still name s's attempt, and the attempt entry must still
// exist, neither aborted nor marked deleting (the attempt path is never
// reused, so a snapshot read gone stays gone).
func (v *verifier) snapshotActive(ctx context.Context, s *verifyView) (bool, error) {
	ne, err := v.c.f.LookupEntry(ctx, path.Join(v.root, snapNamesDir), s.name)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, abortErr(ctx, err, "re-read snapshot name %s/%s", v.vol, s.name)
	}
	if rec, err := decodeNameRecord(path.Join(v.root, snapNamesDir, s.name), ne); err == nil && rec.Attempt != s.attempt {
		return false, nil
	}
	e, err := v.c.f.LookupEntry(ctx, path.Join(v.root, snapsDir, s.name), s.attempt)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, abortErr(ctx, err, "re-read snapshot %s/%s/%s", v.vol, s.name, s.attempt)
	}
	st := string(e.Extended[extSnapState])
	return st != SnapAborted && st != SnapDeleting && len(e.Extended[extSnapDeletedNs]) == 0, nil
}

func sortedIndexes(m map[uint64]storage.ChunkRef) []uint64 {
	out := make([]uint64, 0, len(m))
	for idx := range m {
		out = append(out, idx)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// splitIdentity splits a chunkIdentity into the owner volume and its key.
func splitIdentity(id string) (owner, key string) {
	owner, key, _ = strings.Cut(id, "\x00")
	return owner, key
}

// ---- snapshots and fork record ----

func (v *verifier) snapshots(ctx context.Context) error {
	if v.opts.Snapshot != "" {
		ref, ok, err := v.c.resolveSnap(ctx, v.vol, v.opts.Snapshot)
		if err != nil {
			if CodeOf(err) == CodeRemoteUnavailable || ctx.Err() != nil {
				return abortErr(ctx, err, "read snapshot %s/%s", v.vol, v.opts.Snapshot)
			}
			v.finding(VerifyFinding{Root: "snapshot " + v.opts.Snapshot, Class: FindingRootInvalid, Detail: err.Error()})
			return nil
		}
		if !ok || ref.entry == nil || ref.info.State == SnapAborted {
			return catErr(CodeSnapshotNotFound, false, nil, "snapshot %s/%s", v.vol, v.opts.Snapshot)
		}
		if ref.info.State == SnapDeleting {
			return catErr(CodeSnapshotDeleting, false, nil, "snapshot %s/%s is being deleted; GC may already reclaim its chunks", v.vol, v.opts.Snapshot)
		}
		return v.snapshot(ctx, v.opts.Snapshot, ref)
	}
	if !v.opts.AllSnapshots {
		return nil
	}
	dir := path.Join(v.root, snapNamesDir)
	after := ""
	for {
		page, err := v.c.f.ListEntries(ctx, dir, "", after, listPage)
		if err != nil {
			return abortErr(ctx, err, "list snapshots of %s", v.vol)
		}
		for _, e := range page {
			if e.GetName() <= after {
				return remoteErr(nil, "non-advancing snapshot listing of %s", v.vol)
			}
			after = e.GetName()
			ref, ok, err := v.c.resolveSnap(ctx, v.vol, e.GetName())
			if err != nil {
				if CodeOf(err) == CodeRemoteUnavailable || ctx.Err() != nil {
					return abortErr(ctx, err, "read snapshot %s/%s", v.vol, e.GetName())
				}
				v.finding(VerifyFinding{Root: "snapshot " + e.GetName(), Class: FindingRootInvalid, Detail: err.Error()})
				continue
			}
			if !ok || ref.entry == nil || ref.info.State == SnapAborted || ref.info.State == SnapDeleting {
				continue
			}
			if err := v.snapshot(ctx, e.GetName(), ref); err != nil {
				return err
			}
		}
		if len(page) < listPage {
			return nil
		}
	}
}

// snapshot selects the snapshot found under name, at the attempt its name
// record names: that path is its identity (label, state reads), and a
// document naming another volume or snapshot is ROOT_INVALID. Its
// references are fixed; its chunks are discovered (V.Discover).
func (v *verifier) snapshot(ctx context.Context, name string, ref snapRef) error {
	s := &verifyView{label: "snapshot " + name + "/" + ref.rec.Attempt, h: ref.info.Head(), name: name, attempt: ref.rec.Attempt}
	v.res.Snapshots = append(v.res.Snapshots, name+"/"+s.attempt)
	v.snaps = append(v.snaps, s)
	if ref.info.VolumeID != v.vol || ref.info.Name != name {
		v.finding(VerifyFinding{Root: s.label, Class: FindingRootInvalid,
			Detail: fmt.Sprintf("snapshot document names %s/%s, found as %s/%s", ref.info.VolumeID, ref.info.Name, v.vol, name)})
	}
	if ref.info.geo != (Geometry{}) && ref.info.geo != v.geo {
		v.finding(VerifyFinding{Root: s.label, Class: FindingGeometry, Detail: fmt.Sprintf("snapshot geometry %+v, VOLUME %+v", ref.info.geo, v.geo)})
	}
	chunks, _, err := v.walk(ctx, s.h)
	if err != nil {
		return err
	}
	s.chunks = chunks
	v.geometry(s.label, s.h, chunks)
	v.discover(chunks)
	return nil
}

// forkRecord checks a fork's record against the parent snapshot it names:
// some attempt of that snapshot, at the recorded source generation, must
// have exactly the recorded HEAD digest (attempt entries are never
// removed, so a parent snapshot deleted or renamed since still answers).
func (v *verifier) forkRecord(ctx context.Context, vi VolumeInfo) error {
	fr := vi.Fork
	if vi.Origin != OriginFork || fr == nil {
		return nil
	}
	const label = "VOLUME fork record"
	mismatch := func(format string, a ...any) error {
		v.finding(VerifyFinding{Root: label, Owner: fr.Source, Class: FindingForkRecordMismatch, Detail: fmt.Sprintf(format, a...)})
		return nil
	}
	if CheckVolumeID(fr.Source) != nil || CheckSnapshotName(fr.Snapshot) != nil {
		return mismatch("fork record names source %q snapshot %q", fr.Source, fr.Snapshot)
	}
	dir := path.Join(v.c.base, fr.Source, snapsDir, fr.Snapshot)
	after := ""
	sawGen := false
	for {
		page, err := v.c.f.ListEntries(ctx, dir, "", after, listPage)
		if err != nil {
			return abortErr(ctx, err, "list attempts of %s/%s", fr.Source, fr.Snapshot)
		}
		for _, e := range page {
			if e.GetName() <= after {
				return remoteErr(nil, "non-advancing attempt listing of %s/%s", fr.Source, fr.Snapshot)
			}
			after = e.GetName()
			info, _, err := decodeSnap(path.Join(dir, e.GetName()), e)
			if err != nil || info.State == SnapAborted || info.HeadGen != fr.SourceGen {
				continue
			}
			sawGen = true
			if info.digest == fr.HeadSHA256 {
				return nil
			}
		}
		if len(page) < listPage {
			break
		}
	}
	if sawGen {
		return mismatch("no attempt of snapshot %s/%s at gen %d has HEAD digest %s", fr.Source, fr.Snapshot, fr.SourceGen, fr.HeadSHA256)
	}
	// The source volume itself may have been deleted since the fork: its
	// snapshot records went with it and there is nothing to compare with.
	// That is not damage to this volume, whose chunks are checked through
	// its own roots.
	if _, ok, err := v.c.ReadVolume(ctx, fr.Source); err != nil {
		return abortErr(ctx, err, "read fork source %s", fr.Source)
	} else if !ok {
		v.res.ForkSourceGone = true
		return nil
	}
	return mismatch("no attempt of snapshot %s/%s at gen %d found", fr.Source, fr.Snapshot, fr.SourceGen)
}

// ---- intents ----

// intents reports capture intents that look stale: below HEAD's epoch,
// older than IntentStaleAge, and naming no key HEAD references. It is a
// hint; whether GC may expire one is GC's decision, with blockmaster
// authority. An intent that does not decode is an invalid GC root.
func (v *verifier) intents(ctx context.Context, h storage.ChunkHead, chunks map[uint64]storage.ChunkRef) error {
	named := map[string]bool{}
	for _, ref := range chunks {
		named[chunkIdentity(v.vol, ref.Key)] = true
	}
	for _, ref := range h.Segments {
		named[chunkIdentity(v.vol, ref.Key)] = true
	}
	now := v.c.now()
	dir := path.Join(v.root, intentsDir)
	after := ""
	for {
		page, err := v.c.f.ListEntries(ctx, dir, "", after, listPage)
		if err != nil {
			return abortErr(ctx, err, "list intents of %s", v.vol)
		}
		for _, e := range page {
			if e.GetName() <= after {
				return remoteErr(nil, "non-advancing intent listing of %s", v.vol)
			}
			after = e.GetName()
			in, err := decodeIntent(path.Join(dir, e.GetName()), e)
			if err != nil {
				v.finding(VerifyFinding{Root: "intent " + e.GetName(), Class: FindingRootInvalid, Detail: err.Error()})
				continue
			}
			if strings.HasPrefix(in.Holder, "snapshot:") || strings.HasPrefix(in.Holder, "fork:") {
				continue // copy intents never expire by epoch
			}
			age := now.Sub(time.Unix(0, in.CreatedNs))
			if in.Epoch >= h.Epoch || age <= v.opts.IntentStaleAge {
				continue
			}
			bound := false
			for _, k := range in.Keys {
				if named[chunkIdentity(v.vol, k)] {
					bound = true
					break
				}
			}
			if bound {
				continue
			}
			v.finding(VerifyFinding{Root: "intent " + in.ID, Class: FindingIntentStale,
				Detail: fmt.Sprintf("capture intent of %q at epoch %d below HEAD epoch %d, %s old, naming %d keys HEAD does not reference",
					in.Holder, in.Epoch, h.Epoch, age.Round(time.Second), len(in.Keys))})
		}
		if len(page) < listPage {
			return nil
		}
	}
}

// ---- refresh ----

// maybeRefresh reads HEAD again (V.Refresh) once RefreshInterval has passed since the last read.
func (v *verifier) maybeRefresh(ctx context.Context) error {
	if v.head == nil || v.c.now().Sub(v.lastRefresh) < v.opts.RefreshInterval {
		return nil
	}
	_, err := v.readHead(ctx)
	return err
}

// readHead reads HEAD and applies it (V.Refresh); a HEAD that is gone, or does
// not decode (reported once), changes nothing. It reports whether HEAD named
// a chunk not seen.
func (v *verifier) readHead(ctx context.Context) (bool, error) {
	v.lastRefresh = v.c.now()
	e, err := v.c.f.LookupEntry(ctx, v.root, headName)
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, abortErr(ctx, err, "refresh HEAD of %s", v.vol)
	}
	h, err := decodeHead(path.Join(v.root, headName), e)
	if err != nil {
		if !v.badRefresh {
			v.badRefresh = true
			v.finding(VerifyFinding{Root: "HEAD", Class: FindingRootInvalid, Detail: "refreshed HEAD: " + err.Error()})
		}
		return false, nil
	}
	return v.refresh(ctx, h)
}

// V.Refresh: replace the HEAD view with h unless h is older (or the same); mark the keys it drops; discover its new keys.
func (v *verifier) refresh(ctx context.Context, h storage.ChunkHead) (bool, error) {
	if v.head != nil && (h.Gen < v.head.h.Gen || h.Gen == v.head.h.Gen && h.Epoch == v.head.h.Epoch) {
		return false, nil
	}
	chunks, unusable, err := v.walk(ctx, h)
	if err != nil {
		return false, err
	}
	if v.head != nil {
		// A key the old view named that h does not is dropped, unless its
		// old index lies in a segment of h that could not be read.
		perSeg := uint64(0)
		if h.ChunkBytes > 0 {
			perSeg = manifestSegmentBytes / h.ChunkBytes
		}
		now := map[string]bool{}
		for _, ref := range chunks {
			now[chunkIdentity(v.vol, ref.Key)] = true
		}
		for idx, ref := range v.head.chunks {
			if id := chunkIdentity(v.vol, ref.Key); !now[id] && (perSeg == 0 || !unusable[idx/perSeg]) {
				v.dropped[id] = true
			}
		}
	}
	v.head = &verifyView{label: headLabel(h.Gen), h: h, chunks: chunks}
	v.res.HeadGens = append(v.res.HeadGens, h.Gen)
	return v.discover(chunks), nil
}

// ---- bindings ----

// checkBindings checks the binding of every chunk in batch. The first batch
// lists each owner's keys/ once; chunks not found in a listing (bound after
// it, or queued by a refresh) are looked up one by one.
func (v *verifier) checkBindings(ctx context.Context, batch []*verifyChunk, list bool) error {
	found := map[*verifyChunk]*filerpb.Entry{}
	if list {
		byOwner := map[string]map[string]*verifyChunk{}
		var owners []string
		for _, w := range batch {
			name, err := keyName(w.key)
			if err != nil {
				continue
			}
			if byOwner[w.owner] == nil {
				byOwner[w.owner] = map[string]*verifyChunk{}
				owners = append(owners, w.owner)
			}
			byOwner[w.owner][name] = w
		}
		sort.Strings(owners)
		for _, owner := range owners {
			if err := v.maybeRefresh(ctx); err != nil {
				return err
			}
			if CheckVolumeID(owner) != nil {
				continue
			}
			want := byOwner[owner]
			dir := path.Join(v.c.base, owner, keysDir)
			after := ""
			for {
				page, err := v.c.f.ListEntries(ctx, dir, "", after, listPage)
				if err != nil {
					return abortErr(ctx, err, "list the chunk keys of %s", owner)
				}
				for _, e := range page {
					if e.GetName() <= after {
						return remoteErr(nil, "non-advancing chunk key listing of %s", owner)
					}
					after = e.GetName()
					if w := want[e.GetName()]; w != nil {
						found[w] = e
					}
				}
				if len(page) < listPage {
					break
				}
			}
		}
	}
	for _, w := range batch {
		e := found[w]
		if e == nil {
			var err error
			if e, err = v.lookupBinding(ctx, w); err != nil {
				return err
			}
		}
		v.evaluateBinding(w, e)
	}
	return nil
}

// lookupBinding reads w's binding directly; nil when it does not exist.
func (v *verifier) lookupBinding(ctx context.Context, w *verifyChunk) (*filerpb.Entry, error) {
	if CheckVolumeID(w.owner) != nil {
		return nil, nil
	}
	name, err := keyName(w.key)
	if err != nil {
		return nil, nil
	}
	dir := path.Join(v.c.base, w.owner, keysDir)
	e, err := v.c.f.LookupEntry(ctx, dir, name)
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, abortErr(ctx, err, "read chunk key binding %s/%s", dir, name)
	}
	return e, nil
}

// V.Settle (bindings): a missing, invalid or tombstoned binding is w's outcome; a sound one settles it at depth bindings.
func (v *verifier) evaluateBinding(w *verifyChunk, e *filerpb.Entry) {
	if e == nil {
		v.settle(w, FindingBindingMissing, fmt.Sprintf("no binding in %s/keys", w.owner))
		return
	}
	w.fid = string(e.Extended[extFid])
	switch bad := bindingProblem(e); {
	case bad != "":
		v.settle(w, FindingBindingMissing, "invalid binding: "+bad)
	case len(e.Extended[extGCDeleted]) != 0:
		v.settle(w, FindingBindingTombstoned, fmt.Sprintf("binding tombstoned by GC (%s=%s)", extGCDeleted, e.Extended[extGCDeleted]))
	default:
		w.bound = true
		if v.opts.Depth != VerifyData {
			v.settle(w, "", "")
		}
	}
}

// ---- data ----

// checkData fetches every bound chunk of batch once, at bounded concurrency
// and paced.
func (v *verifier) checkData(ctx context.Context, batch []*verifyChunk) error {
	if v.blobs == nil {
		return errors.New("chunkstore: verify needs a blob reader for depth data")
	}
	jobs := make(chan *verifyChunk)
	var (
		wg      sync.WaitGroup
		errMu   sync.Mutex
		firstEr error
	)
	setErr := func(err error) {
		errMu.Lock()
		if firstEr == nil {
			firstEr = err
		}
		errMu.Unlock()
	}
	getErr := func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return firstEr
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i := 0; i < v.opts.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range jobs {
				if err := v.readChunk(wctx, w); err != nil {
					setErr(err)
					cancel()
				}
			}
		}()
	}
	dispatch := func() error {
		for _, w := range batch {
			if !w.bound {
				continue
			}
			if err := getErr(); err != nil {
				return err
			}
			if err := v.maybeRefresh(ctx); err != nil {
				return err
			}
			if err := v.pacer.wait(wctx, int64(v.chunkBytes)); err != nil {
				if werr := getErr(); werr != nil {
					return werr
				}
				return abortErr(ctx, err, "pace chunk reads")
			}
			select {
			case jobs <- w:
			case <-wctx.Done():
				if err := getErr(); err != nil {
					return err
				}
				return abortErr(ctx, wctx.Err(), "read chunks of %s", v.vol)
			}
		}
		return nil
	}
	if err := dispatch(); err != nil {
		// Stop the reads in flight before waiting for them, so a known
		// failure is not held up by a read that would end only with the
		// caller's deadline. The failure that stopped the pass is the one
		// returned, not the cancellation it caused in the readers.
		setErr(err)
		cancel()
	}
	close(jobs)
	wg.Wait()
	return getErr()
}

// V.Settle (data): read w's needle once and record the outcome (missing, read error, wrong size, or the content's SHA-256).
func (v *verifier) readChunk(ctx context.Context, w *verifyChunk) error {
	data, err := v.fetch(ctx, w.fid)
	switch {
	case err == nil:
	case isContextErr(ctx, err):
		return abortErr(ctx, err, "read chunk %s", w.fid)
	case errors.Is(err, ErrBlobNotFound):
		v.settle(w, FindingNeedleMissing, err.Error())
		return nil
	default:
		v.settle(w, FindingReadError, readErrorDetail(err))
		return nil
	}
	sum := sha256.Sum256(data)
	v.mu.Lock()
	defer v.mu.Unlock()
	v.res.DistinctChunksRead++
	v.res.BytesRead += int64(len(data))
	w.settled, w.read, w.digest = true, true, sum
	if uint64(len(data)) != v.chunkBytes {
		w.class, w.detail = FindingSizeMismatch, fmt.Sprintf("needle holds %d bytes, chunk size %d", len(data), v.chunkBytes)
	}
	return nil
}

// ---- reconcile ----

// verifyRef is a final-view reference: the root (its position among the
// final roots), the index and the SHA-256.
type verifyRef struct {
	root int
	idx  uint64
	sha  [32]byte
}

// V.Reconcile: compute every chunk and segment finding from the outcomes and the final view alone; an unnamed bad chunk counts as root deleted if a gone selected snapshot names it, else as superseded.
func (v *verifier) reconcile() {
	// The final roots are the HEAD view, then each selected snapshot not
	// gone, in order. A chunk with a bad outcome: one finding under the
	// first final root that names its key, at that root's first index; if
	// none does, no finding, counted once (root deleted takes precedence
	// over superseded). A chunk read and verified: one CONTENT_MISMATCH,
	// under the first final reference that differs from the content, its
	// detail counting the others (and naming the next root carrying one).
	// At most one finding per key and class. Below depth data, final
	// references to one chunk with different SHA-256s are one
	// CONTENT_MISMATCH. A key HEAD
	// dropped earlier in the pass and names again is one ROOT_INVALID. A
	// segment problem follows the bad-chunk rule over the final roots that
	// name that segment. Chunks never settled (an incomplete pass) are not
	// judged.
	var roots []*verifyView
	if v.head != nil {
		roots = append(roots, v.head)
		v.geometry(v.head.label, v.head.h, v.head.chunks)
	}
	for _, s := range v.snaps {
		if s.gone {
			v.res.SnapshotsDeletedDuringVerify = append(v.res.SnapshotsDeletedDuringVerify, s.name+"/"+s.attempt)
		} else {
			roots = append(roots, s)
		}
	}
	named := map[string][]verifyRef{}
	for ri, r := range roots {
		for _, idx := range sortedIndexes(r.chunks) {
			ref := r.chunks[idx]
			id := chunkIdentity(v.vol, ref.Key)
			named[id] = append(named[id], verifyRef{root: ri, idx: idx, sha: ref.SHA256})
			v.res.ChunksChecked++
		}
	}
	ids := make([]string, 0, len(v.chunks))
	for id := range v.chunks {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		w, refs := v.chunks[id], named[id]
		report := func(at verifyRef, class, detail string) {
			idx := at.idx
			v.finding(VerifyFinding{Root: roots[at.root].label, Index: &idx, Owner: w.owner, Key: w.key, FID: w.fid, Class: class, Detail: detail})
		}
		switch {
		case !w.settled:
		case w.class != "" && len(refs) == 0:
			v.unnamed(func(s *verifyView) bool { return s.namesChunk(v.vol, id) })
		case w.class != "":
			report(refs[0], w.class, w.detail)
		case w.read:
			var differ []verifyRef
			for _, r := range refs {
				if r.sha != w.digest {
					differ = append(differ, r)
				}
			}
			if len(differ) == 0 {
				break
			}
			detail := fmt.Sprintf("needle SHA-256 %s, reference %s", hex.EncodeToString(w.digest[:]), hex.EncodeToString(differ[0].sha[:]))
			if more := differ[1:]; len(more) > 0 {
				detail += fmt.Sprintf("; %d more final reference(s) differ", len(more))
				for _, r := range more {
					if r.root != differ[0].root {
						detail += ", next under " + roots[r.root].label
						break
					}
				}
			}
			report(differ[0], FindingContentMismatch, detail)
		}
		if v.opts.Depth != VerifyData {
			for _, r := range refs[min(1, len(refs)):] {
				if r.sha != refs[0].sha {
					report(r, FindingContentMismatch, fmt.Sprintf("references to this chunk disagree: SHA-256 %s here, %s under %s at index %d (depth data reads the chunk)",
						hex.EncodeToString(r.sha[:]), hex.EncodeToString(refs[0].sha[:]), roots[refs[0].root].label, refs[0].idx))
					break
				}
			}
		}
	}
	if v.head != nil {
		reported := map[string]bool{}
		for _, idx := range sortedIndexes(v.head.chunks) {
			id := chunkIdentity(v.vol, v.head.chunks[idx].Key)
			if v.dropped[id] && !reported[id] {
				reported[id] = true
				i := idx
				owner, key := splitIdentity(id)
				fid := ""
				if w := v.chunks[id]; w != nil {
					fid = w.fid
				}
				v.finding(VerifyFinding{Root: v.head.label, Index: &i, Owner: owner, Key: key, FID: fid, Class: FindingRootInvalid,
					Detail: "HEAD names a key it had dropped earlier in this pass; a key that left every root must not be named again"})
			}
		}
	}
	for k, s := range v.segs {
		var issues []segIssue
		if s.tombstoned && v.opts.Depth != VerifyMeta {
			issues = append(issues, segIssue{FindingBindingTombstoned, s.fid, "segment binding tombstoned by GC"})
		}
		if s.issue != nil {
			issues = append(issues, *s.issue)
		}
		if len(issues) == 0 {
			continue
		}
		var at *verifyView
		for _, r := range roots {
			if r.namesSegment(k) {
				at = r
				break
			}
		}
		if at == nil {
			v.unnamed(func(s *verifyView) bool { return s.namesSegment(k) })
			continue
		}
		for _, is := range issues {
			g := k.group
			v.finding(VerifyFinding{Root: at.label, Segment: &g, Owner: k.ref.Owner, Key: k.ref.Key, FID: is.fid, Class: is.class, Detail: is.detail})
		}
	}
}

// unnamed counts a bad chunk or segment no final root names: root deleted
// when a selected snapshot that is gone names it, otherwise superseded.
func (v *verifier) unnamed(namedBy func(*verifyView) bool) {
	for _, s := range v.snaps {
		if s.gone && namedBy(s) {
			v.res.RootDeletedDuringVerify++
			return
		}
	}
	v.res.SupersededDuringVerify++
}

func (r *verifyView) namesChunk(vol, id string) bool {
	for _, ref := range r.chunks {
		if chunkIdentity(vol, ref.Key) == id {
			return true
		}
	}
	return false
}

func (r *verifyView) namesSegment(k verifySegKey) bool {
	ref, ok := r.h.Segments[k.group]
	return ok && ref == k.ref && r.h.ChunkBytes == k.chunkBytes
}

// verifyPacer spaces reads so they average at most rate bytes per second.
type verifyPacer struct {
	mu   sync.Mutex
	rate int64 // <= 0: unpaced
	next time.Time
}

func (p *verifyPacer) wait(ctx context.Context, n int64) error {
	if p == nil || p.rate <= 0 || n <= 0 {
		return ctx.Err()
	}
	p.mu.Lock()
	now := time.Now()
	if p.next.Before(now) {
		p.next = now
	}
	start := p.next
	p.next = p.next.Add(time.Duration(float64(n) / float64(p.rate) * float64(time.Second)))
	p.mu.Unlock()
	d := time.Until(start)
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func findingLess(a, b VerifyFinding) bool {
	if a.Root != b.Root {
		return a.Root < b.Root
	}
	if (a.Index == nil) != (b.Index == nil) {
		return a.Index == nil // a root's and its segments' findings first
	}
	if as, bs := ptrOr(a.Segment), ptrOr(b.Segment); as != bs {
		return as < bs
	}
	if ai, bi := ptrOr(a.Index), ptrOr(b.Index); ai != bi {
		return ai < bi
	}
	if a.Key != b.Key {
		return a.Key < b.Key
	}
	return a.Class < b.Class
}

func ptrOr(p *uint64) uint64 {
	if p == nil {
		return 0
	}
	return *p
}
