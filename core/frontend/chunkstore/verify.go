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
//   - V.Discover: an object (a chunk or a segment, by owner and key) a root
//     view names that the pass has not seen is queued for one check; every
//     reference met is recorded as seen, with nothing judged about it.
//   - V.Settle: the check records the object's one outcome (its content, or
//     binding missing or tombstoned, needle missing, read error) and reports
//     nothing; a settled object is never checked again.
//   - V.Refresh: HEAD is read again at least every RefreshInterval; a newer
//     generation replaces the HEAD view (an older one is ignored), the keys
//     the old view named and the new one does not are marked dropped, and
//     its new keys are discovered.
//   - V.Round: once every queued chunk is settled, one closing read of HEAD
//     and of each selected snapshot's state; if it names a chunk not seen,
//     the pass checks it and reads again, at most ClosingRounds times
//     (V.GiveUp: then the pass is incomplete).
//   - V.Close: a closing read that names no chunk not seen is the final view.
//     Only a decodable HEAD, not older than the view, can close a pass as
//     complete: a closing read that finds no HEAD, or one that does not
//     decode (ROOT_INVALID), leaves no HEAD root and the pass incomplete.
//   - V.Reconcile: the findings and counters are computed from the outcomes,
//     the references seen and the final view alone, nowhere else. Judge, a
//     pure function of an object's outcome and one reference, gives the
//     outcome when it is bad, else CONTENT_MISMATCH (another hash),
//     SIZE_MISMATCH (a chunk not a chunk long) or SEGMENT_UNREADABLE (a
//     segment that does not decode under that reference's fields), else
//     sound. Per object and class Judge gives a final reference: one finding,
//     under the first final root having one (HEAD, then the snapshots not
//     gone, in order) at its first such index. An object with a problem
//     under any reference seen, and no finding, is counted once. A key HEAD
//     dropped and names again is ROOT_INVALID, whatever its outcome.
//
// The pass therefore reports the state of each named chunk at its check,
// for the roots at the final read; a chunk damaged after its check is the
// next pass's finding. A chunk that left HEAD can be collected after GC's
// grace, and a deleted snapshot's chunks are kept only for the grace, so an
// object that no root names at the final read is not a finding.
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
	verifyHookClosed       = "closed"        // the quiet closing read was made: the final view is fixed
)

// VerifyIncomplete is why a pass did not complete.
type VerifyIncomplete string

const (
	VerifyIncompleteDeadline    VerifyIncomplete = "deadline"                 // the caller's deadline passed
	VerifyIncompleteCancelled   VerifyIncomplete = "cancelled"                // the caller cancelled
	VerifyIncompleteRounds      VerifyIncomplete = "closing_rounds_exhausted" // every closing read named new chunks
	VerifyIncompleteHeadAbsent  VerifyIncomplete = "head_absent"              // the closing read found no HEAD
	VerifyIncompleteHeadInvalid VerifyIncomplete = "head_invalid"             // the closing read's HEAD does not decode
	VerifyIncompleteError       VerifyIncomplete = "error"                    // a filer or blob store failure, or a refused request
)

// CodeVerifyIncomplete: the closing reads could not close the pass (each
// named chunks not seen before, or the last found no usable HEAD).
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
	// SupersededDuringVerify counts the chunks and segments that had a bad
	// outcome or a problem under a reference seen during the pass, yet yield
	// no finding (no final reference has a problem), once each;
	// RootDeletedDuringVerify counts instead those whose problem reference
	// was carried by a selected snapshot deleted during the pass.
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
		segs: map[string]*verifySegment{}, chunks: map[string]*verifyChunk{}, dropped: map[string]bool{},
		seenChunks: map[string]map[seenChunk]bool{}, seenSegs: map[string]map[seenSeg]bool{},
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
		case errors.Is(err, errVerifyHeadAbsent):
			res.Incomplete = VerifyIncompleteHeadAbsent
		case errors.Is(err, errVerifyHeadInvalid):
			res.Incomplete = VerifyIncompleteHeadInvalid
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

// Why a closing read leaves no HEAD root (wrapped in CodeVerifyIncomplete).
var (
	errVerifyHeadAbsent  = errors.New("chunkstore: verify: the closing read found no HEAD")
	errVerifyHeadInvalid = errors.New("chunkstore: verify: the closing read's HEAD does not decode")
)

// verifySegment is one segment object (owner, key): read once, like a chunk
// (keys are never rebound). Its one outcome is a bad one (class and detail:
// binding missing, needle missing, read error) or its content (sum, body:
// a segment keeps its bytes). A reference to it is judged at need
// (judgeSegment); nothing about a reference is kept but the decode memo.
type verifySegment struct {
	owner, key    string
	fid           string
	tombstoned    bool
	class, detail string
	sum           [32]byte
	body          []byte
	memo          map[segRef]segJudged // decodes by the full reference (every input of the decode)
}

// segRef is one root's reference to a segment, with every root field its
// decode depends on.
type segRef struct {
	group, chunkBytes, remoteLSN uint64
	ref                          storage.ChunkSegment
}

// segJudged is Judge's answer for a segment reference: a class (and detail)
// when it is not sound, else this root's chunk references.
type segJudged struct {
	class, detail string
	chunks        map[uint64]storage.ChunkRef
}

// segID is a segment's identity: its owner and key.
func segID(vol string, ref storage.ChunkSegment) string { return chunkIdentity(vol, ref.Key) }

// Seen references, per object: the root (HEAD, or a snapshot's label), the
// index (a chunk's) or the reference with its root fields (a segment's).
type seenChunk struct {
	root string
	idx  uint64
	sha  [32]byte
}

type seenSeg struct {
	root string
	ref  segRef
}

const headRoot = "HEAD" // the root of every HEAD generation's references

// verifyChunk is one distinct chunk (owner volume + the owner's own key):
// unseen while absent from the verifier's map, then queued, then settled.
type verifyChunk struct {
	id         string
	owner, key string
	fid        string
	bound      bool // a sound, untombstoned binding was found

	// The outcome, under the verifier's lock until the checks are joined:
	// class and detail of a bad outcome (binding missing or tombstoned,
	// needle missing, read error), or the content read (read: its SHA-256
	// and length).
	settled       bool
	class, detail string
	read          bool
	digest        [32]byte
	length        int
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
	segs    map[string]*verifySegment // the segments read, by identity
	chunks  map[string]*verifyChunk   // the chunks seen, by identity
	pending []*verifyChunk            // queued, not yet checked
	listed  bool                      // the owners' keys/ were listed once

	head        *verifyView     // the HEAD view; nil without a usable HEAD
	dropped     map[string]bool // keys a HEAD view named and a later one did not
	snaps       []*verifyView   // the selected snapshots, in order
	lastRefresh time.Time

	seenChunks map[string]map[seenChunk]bool // the references seen, per chunk
	seenSegs   map[string]map[seenSeg]bool   // the references seen, per segment
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
			v.hook(verifyHookClosed)
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
		if named, err = v.readHead(ctx, true); err != nil {
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

// V.Discover and V.Settle (segments): the segment ref names, read once per (owner, key) by this pass, its outcome kept. An error stops the pass.
func (v *verifier) segment(ctx context.Context, ref storage.ChunkSegment) (*verifySegment, error) {
	id := segID(v.vol, ref)
	if s := v.segs[id]; s != nil {
		return s, nil
	}
	s, err := v.readSegment(ctx, ref)
	if err != nil {
		return nil, err
	}
	v.segs[id] = s
	return s, nil
}

func (v *verifier) readSegment(ctx context.Context, ref storage.ChunkSegment) (*verifySegment, error) {
	if v.blobs == nil {
		return nil, errors.New("chunkstore: verify needs a blob reader for segmented roots")
	}
	r, err := NewChunkRemote(v.c.f, v.blobs, v.root)
	if err != nil {
		return nil, err
	}
	s := &verifySegment{owner: ref.Owner, key: ref.Key, memo: map[segRef]segJudged{}}
	dir, name, err := r.keyLocation(ref.Key)
	if err != nil {
		s.class, s.detail = FindingBindingMissing, "segment binding: "+err.Error()
		return s, nil
	}
	e, err := v.c.f.LookupEntry(ctx, dir, name)
	if errors.Is(err, ErrNotFound) {
		s.class, s.detail = FindingBindingMissing, "segment binding not found"
		return s, nil
	}
	if err != nil {
		return nil, abortErr(ctx, err, "read segment binding %s/%s", dir, name)
	}
	b, err := decodeBinding(e)
	if err != nil {
		s.class, s.detail = FindingBindingMissing, "invalid segment binding: "+err.Error()
		return s, nil
	}
	if b.done {
		// Reclaimed: the binding names no needle any more (the filer deletes
		// the dropped one); the tombstone is reported beside this outcome.
		s.tombstoned = true
		s.class, s.detail = FindingNeedleMissing, "segment binding reclaimed by GC; its needle was handed to the filer for deletion"
		return s, nil
	}
	s.fid = b.fid
	v.mu.Lock()
	v.res.SegmentReads++
	v.mu.Unlock()
	body, err := v.fetch(ctx, s.fid)
	switch {
	case err == nil:
	case isContextErr(ctx, err):
		return nil, abortErr(ctx, err, "read segment %s", ref.Key)
	case errors.Is(err, ErrBlobNotFound):
		s.class, s.detail = FindingNeedleMissing, "segment needle not found: "+err.Error()
		return s, nil
	default:
		s.class, s.detail = FindingReadError, readErrorDetail(err)
		return s, nil
	}
	s.sum, s.body = sha256.Sum256(body), body
	return s, nil
}

// V.Reconcile's Judge for a segment reference (also used to walk a root): the segment's bad outcome; a hash mismatch; bytes that do not decode under this reference's own fields (SEGMENT_UNREADABLE); else this root's chunk references.
func (v *verifier) judgeSegment(ctx context.Context, s *verifySegment, k segRef) segJudged {
	switch {
	case s.class != "":
		return segJudged{class: s.class, detail: s.detail}
	case k.ref.SHA256 != s.sum:
		return segJudged{class: FindingContentMismatch, detail: fmt.Sprintf("segment SHA-256 %s, reference %s", hex.EncodeToString(s.sum[:]), hex.EncodeToString(k.ref.SHA256[:]))}
	}
	// A memo of the decode, keyed by the full reference: the decode is a
	// function of the bytes and of k alone, so the memo cannot change an
	// answer; it only saves decoding a segment again at each walk.
	if j, ok := s.memo[k]; ok {
		return j
	}
	j := segJudged{}
	r, err := NewChunkRemote(v.c.f, verifyPrefetched{s.fid, s.body}, v.root)
	if err == nil {
		r.mu.Lock()
		r.fids[k.ref.Key] = s.fid
		r.mu.Unlock()
		one := storage.ChunkHead{Segments: map[uint64]storage.ChunkSegment{k.group: k.ref}, ChunkBytes: k.chunkBytes,
			ManifestChunkCount: k.ref.Count, RemoteLSN: k.remoteLSN, State: storage.ChunkHeadActive}
		var resolved storage.ChunkHead
		if resolved, err = r.resolveManifest(ctx, one); err == nil {
			j.chunks = resolved.Chunks
		}
	}
	if err != nil {
		j = segJudged{class: FindingSegmentUnreadable, detail: err.Error()}
	}
	s.memo[k] = j
	return j
}

// walk reads the segments root h (of root, HEAD or a snapshot's label) names
// and returns its chunk references by index, and the segment groups whose
// reference Judge does not find sound (their chunks are unknown). Every
// reference met is recorded as seen.
func (v *verifier) walk(ctx context.Context, root string, h storage.ChunkHead) (map[uint64]storage.ChunkRef, map[uint64]bool, error) {
	out := map[uint64]storage.ChunkRef{}
	unusable := map[uint64]bool{}
	if h.Segments == nil {
		for idx, ref := range h.Chunks {
			out[idx] = ref
		}
	}
	for _, g := range sortedGroups(h.Segments) {
		v.mu.Lock()
		v.res.SegmentsChecked++
		v.mu.Unlock()
		s, err := v.segment(ctx, h.Segments[g])
		if err != nil {
			return out, unusable, err
		}
		k := segRef{group: g, chunkBytes: h.ChunkBytes, remoteLSN: h.RemoteLSN, ref: h.Segments[g]}
		see(v.seenSegs, segID(v.vol, k.ref), seenSeg{root, k})
		j := v.judgeSegment(ctx, s, k)
		if j.class != "" {
			unusable[g] = true
			continue
		}
		for idx, cr := range j.chunks {
			out[idx] = cr
		}
	}
	for idx, ref := range out {
		see(v.seenChunks, chunkIdentity(v.vol, ref.Key), seenChunk{root, idx, ref.SHA256})
	}
	return out, unusable, nil
}

// see records a reference to object id as seen.
func see[R comparable](seen map[string]map[R]bool, id string, r R) {
	if seen[id] == nil {
		seen[id] = map[R]bool{}
	}
	seen[id][r] = true
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
	chunks, _, err := v.walk(ctx, s.label, s.h)
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
	_, err := v.readHead(ctx, false)
	return err
}

// readHead reads HEAD and applies it (V.Refresh); it reports whether HEAD
// named a chunk not seen. A periodic read that finds no HEAD, one that does
// not decode, or an older generation changes nothing: the closing read
// decides. At the closing read (closing set) every answer decides the final
// view: no HEAD leaves no HEAD root and the pass incomplete (HEAD absent);
// an undecodable one is ROOT_INVALID, no HEAD root, incomplete (HEAD
// invalid); an older generation than the view cannot be the final view and
// cannot keep the earlier one, so the pass is incomplete (an error).
func (v *verifier) readHead(ctx context.Context, closing bool) (bool, error) {
	v.lastRefresh = v.c.now()
	e, err := v.c.f.LookupEntry(ctx, v.root, headName)
	if errors.Is(err, ErrNotFound) {
		if !closing {
			return false, nil
		}
		v.head = nil
		return false, catErr(CodeVerifyIncomplete, true, errVerifyHeadAbsent, "verify of %s: the closing read found no HEAD; the pass has no HEAD to report against", v.vol)
	}
	if err != nil {
		return false, abortErr(ctx, err, "refresh HEAD of %s", v.vol)
	}
	h, err := decodeHead(path.Join(v.root, headName), e)
	if err != nil {
		if !closing {
			return false, nil
		}
		v.head = nil
		v.finding(VerifyFinding{Root: "HEAD", Class: FindingRootInvalid, Detail: "HEAD at the closing read: " + err.Error()})
		return false, catErr(CodeVerifyIncomplete, true, errVerifyHeadInvalid, "verify of %s: the HEAD of the closing read does not decode", v.vol)
	}
	if closing && v.head != nil && h.Gen < v.head.h.Gen {
		return false, remoteErr(nil, "verify of %s: the closing read returned HEAD gen %d, older than gen %d already read", v.vol, h.Gen, v.head.h.Gen)
	}
	return v.refresh(ctx, h)
}

// V.Refresh: replace the HEAD view with h unless h is older (or the same); mark the keys it drops; discover its new keys.
func (v *verifier) refresh(ctx context.Context, h storage.ChunkHead) (bool, error) {
	if v.head != nil && (h.Gen < v.head.h.Gen || h.Gen == v.head.h.Gen && h.Epoch == v.head.h.Epoch) {
		return false, nil
	}
	chunks, unusable, err := v.walk(ctx, headRoot, h)
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
	b, err := decodeBinding(e)
	w.fid = b.fid
	switch {
	case err != nil:
		v.settle(w, FindingBindingMissing, "invalid binding: "+err.Error())
	case b.done:
		v.settle(w, FindingBindingTombstoned, fmt.Sprintf("binding tombstoned by GC (%s=done)", extGCDeleted))
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
	w.settled, w.read, w.digest, w.length = true, true, sum, len(data)
	return nil
}

// ---- reconcile ----

// V.Reconcile's Judge for a chunk reference: the chunk's bad outcome; a hash mismatch; a wrong length; else sound ("").
func (v *verifier) judgeChunk(w *verifyChunk, sha [32]byte) (string, string) {
	switch {
	case w.class != "":
		return w.class, w.detail
	case !w.read: // checked to its binding (depth bindings), or not checked (depth meta)
		return "", ""
	case sha != w.digest:
		return FindingContentMismatch, fmt.Sprintf("needle SHA-256 %s, reference %s", hex.EncodeToString(w.digest[:]), hex.EncodeToString(sha[:]))
	case uint64(w.length) != v.chunkBytes:
		return FindingSizeMismatch, fmt.Sprintf("needle holds %d bytes, chunk size %d", w.length, v.chunkBytes)
	}
	return "", ""
}

// judged is Judge's answer for one final reference: its root (position
// among the final roots), its index (a chunk's) or group (a segment's), the
// SHA-256 it expects, and its class ("" when sound) and detail.
type judged struct {
	root          int
	at            uint64
	sha           [32]byte
	class, detail string
}

// V.Reconcile: every finding and counter, from the settled outcomes, the references seen and the final view alone (an incomplete pass: its last view); root deleted takes precedence over superseded.
func (v *verifier) reconcile() {
	// The final roots: the HEAD view, then each selected snapshot not gone,
	// in order. Per object and class Judge gives a final reference: one
	// finding (byClass). An object with a bad outcome or a problem under any
	// reference seen, and no finding, is counted once (count). Objects never
	// settled (an incomplete pass) are not judged.
	var roots []*verifyView
	gone := map[string]bool{}
	if v.head != nil {
		roots = append(roots, v.head)
		v.geometry(v.head.label, v.head.h, v.head.chunks)
	}
	for _, s := range v.snaps {
		if s.gone {
			gone[s.label] = true
			v.res.SnapshotsDeletedDuringVerify = append(v.res.SnapshotsDeletedDuringVerify, s.name+"/"+s.attempt)
		} else {
			roots = append(roots, s)
		}
	}
	finalChunks := map[string][]judged{}
	for ri, r := range roots {
		for _, idx := range sortedIndexes(r.chunks) {
			ref := r.chunks[idx]
			id := chunkIdentity(v.vol, ref.Key)
			finalChunks[id] = append(finalChunks[id], judged{root: ri, at: idx, sha: ref.SHA256})
			v.res.ChunksChecked++
		}
	}
	for _, id := range sortedKeys(v.chunks) {
		w := v.chunks[id]
		if !w.settled {
			continue
		}
		refs := finalChunks[id]
		for i := range refs {
			refs[i].class, refs[i].detail = v.judgeChunk(w, refs[i].sha)
		}
		mk := func(j judged) VerifyFinding {
			idx := j.at
			return VerifyFinding{Root: roots[j.root].label, Index: &idx, Owner: w.owner, Key: w.key, FID: w.fid, Class: j.class, Detail: j.detail}
		}
		reported := v.byClass(roots, refs, mk)
		if v.opts.Depth != VerifyData {
			// Below depth data no content was read: final references to one
			// chunk with different SHA-256s disagree with each other.
			for _, r := range refs[min(1, len(refs)):] {
				if r.sha != refs[0].sha {
					v.finding(mk(judged{root: r.root, at: r.at, class: FindingContentMismatch, detail: fmt.Sprintf(
						"references to this chunk disagree: SHA-256 %s here, %s under %s at index %d (depth data reads the chunk)",
						hex.EncodeToString(r.sha[:]), hex.EncodeToString(refs[0].sha[:]), roots[refs[0].root].label, refs[0].at)}))
					reported = true
					break
				}
			}
		}
		if !reported {
			var problem []string
			for r := range v.seenChunks[id] {
				if c, _ := v.judgeChunk(w, r.sha); c != "" {
					problem = append(problem, r.root)
				}
			}
			v.count(problem, w.class != "", gone)
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
	for _, id := range sortedKeys(v.segs) {
		s := v.segs[id]
		var refs []judged
		for ri, r := range roots {
			for _, g := range sortedGroups(r.h.Segments) {
				if ref := r.h.Segments[g]; segID(v.vol, ref) == id {
					j := v.judgeSegment(context.Background(), s, segRef{group: g, chunkBytes: r.h.ChunkBytes, remoteLSN: r.h.RemoteLSN, ref: ref})
					refs = append(refs, judged{root: ri, at: g, class: j.class, detail: j.detail})
				}
			}
		}
		mk := func(j judged) VerifyFinding {
			g := j.at
			return VerifyFinding{Root: roots[j.root].label, Segment: &g, Owner: s.owner, Key: s.key, FID: s.fid, Class: j.class, Detail: j.detail}
		}
		reported := v.byClass(roots, refs, mk)
		// A tombstoned segment binding is reported beside the segment's
		// outcome, at depths that check bindings.
		tomb := s.tombstoned && v.opts.Depth != VerifyMeta
		if tomb && len(refs) > 0 {
			v.finding(mk(judged{root: refs[0].root, at: refs[0].at, class: FindingBindingTombstoned, detail: "segment binding tombstoned by GC"}))
			reported = true
		}
		if !reported {
			var problem []string
			for r := range v.seenSegs[id] {
				if tomb || v.judgeSegment(context.Background(), s, r.ref).class != "" {
					problem = append(problem, r.root)
				}
			}
			v.count(problem, s.class != "" || tomb, gone)
		}
	}
}

// byClass reports one finding per class among refs (in final-root order):
// under the first reference of that class, its detail counting the others
// of that class and naming the next root carrying one. It reports whether it
// made one.
func (v *verifier) byClass(roots []*verifyView, refs []judged, mk func(judged) VerifyFinding) bool {
	var order []string
	first, more, next := map[string]int{}, map[string]int{}, map[string]string{}
	for i, j := range refs {
		if j.class == "" {
			continue
		}
		f, ok := first[j.class]
		if !ok {
			first[j.class] = i
			order = append(order, j.class)
			continue
		}
		more[j.class]++
		if next[j.class] == "" && j.root != refs[f].root {
			next[j.class] = roots[j.root].label
		}
	}
	for _, c := range order {
		j := refs[first[c]]
		if n := more[c]; n > 0 {
			j.detail += fmt.Sprintf("; %d more final reference(s) of this class", n)
			if next[c] != "" {
				j.detail += ", next under " + next[c]
			}
		}
		v.finding(mk(j))
	}
	return len(order) > 0
}

// count counts an object that yields no finding but had a bad outcome (bad)
// or a problem under a reference seen (problem: those references' roots),
// once: root deleted when a snapshot that is gone carried such a reference,
// otherwise superseded.
func (v *verifier) count(problem []string, bad bool, gone map[string]bool) {
	if len(problem) == 0 && !bad {
		return
	}
	for _, r := range problem {
		if gone[r] {
			v.res.RootDeletedDuringVerify++
			return
		}
	}
	v.res.SupersededDuringVerify++
}

func sortedKeys[T any](m map[string]T) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedGroups(m map[uint64]storage.ChunkSegment) []uint64 {
	out := make([]uint64, 0, len(m))
	for g := range m {
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
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
