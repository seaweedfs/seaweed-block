package chunkstore

// Verify: a read-only check of one volume's remote image against what its
// HEAD (and, on request, its snapshots) say. It writes nothing to the filer
// or the blob store: no intent, no mark, no tombstone, no binding.
//
// Depths, each including the one before:
//
//   - meta: the HEAD root decodes and is self-consistent; its geometry
//     matches VOLUME; every segment a selected root names is readable and
//     passes its SHA-256 and strict decode; a fork's record matches the
//     parent snapshot it was made from.
//   - bindings (the default): every chunk key a selected root references
//     has a sound binding in its owner's keys/ (one listing per owner),
//     not tombstoned by GC.
//   - data: every distinct chunk (owner + key, across all selected roots)
//     is fetched once; its SHA-256 must equal the reference and its length
//     the chunk size (the rule the chunk layer applies on a read miss).
//
// Running beside a live owner and beside GC: segments and chunks are
// immutable and written before the root that names them, so a root read
// once and the segments it names are a consistent view. A chunk that leaves
// HEAD can be collected after GC's grace, so a miss under a HEAD root is a
// finding only if the current HEAD still names that key at that index: on a
// miss, HEAD is read again (the root and the one segment), and a key it no
// longer names is counted as superseded during verify. A snapshot can be
// deleted during the pass, and its chunks are then kept only for the grace,
// so a miss under a snapshot is a finding only if that exact attempt still
// exists and is active (its name record and attempt entry are read again);
// otherwise it is counted as root deleted during verify, and the snapshot is
// listed as not fully covered. A chunk several selected roots name is one
// finding, reported under the first of them that still names it. The HEAD view is
// refreshed at least every RefreshInterval during the binding and data
// phases; only segments whose key changed are read again, and the result
// lists every HEAD generation the pass covered.
//
// A needle is NEEDLE_MISSING only when the store reports it absent
// (ErrBlobNotFound). Any other read failure is retried; if it persists it is
// READ_ERROR, which means "not verified", not "damaged". Both rechecks above
// apply to it.
//
// A finding never stops the pass. Only an inability to continue does (the
// context ended, the filer failed): Verify then returns the partial result,
// with Complete false, and the error.

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

	// hook, when set, runs at the named stages of a pass (a test seam).
	hook func(stage string)
}

// Verify stages passed to the hook.
const (
	verifyHookHeadWalked  = "head-walked"  // the first HEAD view has been read and walked
	verifyHookRootsWalked = "roots-walked" // the selected snapshots have been read and walked too
)

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
// pass could not finish; the counters then cover what it did.
type VerifyResult struct {
	VolumeID string      `json:"volumeID"`
	Depth    VerifyDepth `json:"depth"`
	Complete bool        `json:"complete"`
	// HeadGens are the HEAD generations the pass covered, in order.
	HeadGens []uint64 `json:"headGens"`
	// Snapshots are the snapshots checked, as name/attempt.
	Snapshots []string `json:"snapshots"`
	// SegmentsChecked counts segment references checked (per root);
	// SegmentReads the segment objects actually read.
	SegmentsChecked int `json:"segmentsChecked"`
	SegmentReads    int `json:"segmentReads"`
	// ChunksChecked counts chunk references checked (root, index);
	// DistinctChunks the distinct chunks (owner, key) among them.
	ChunksChecked  int `json:"chunksChecked"`
	DistinctChunks int `json:"distinctChunks"`
	// DistinctChunksRead and BytesRead count the chunk data read.
	DistinctChunksRead int   `json:"distinctChunksRead"`
	BytesRead          int64 `json:"bytesRead"`
	// SupersededDuringVerify counts misses under a HEAD root whose key the
	// current HEAD no longer names (not findings).
	SupersededDuringVerify int `json:"supersededDuringVerify"`
	// RootDeletedDuringVerify counts misses under a snapshot whose attempt
	// was deleted (or aborted, or is gone) by the time of the report (not
	// findings).
	RootDeletedDuringVerify int `json:"rootDeletedDuringVerify"`
	// SnapshotsDeletedDuringVerify lists, as name/attempt, the checked
	// snapshots deleted during the pass: they are not fully covered.
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
	if opts.Depth == "" {
		opts.Depth = VerifyBindings
	}
	res.Depth = opts.Depth
	if opts.Depth != VerifyMeta && opts.Depth != VerifyBindings && opts.Depth != VerifyData {
		return res, catErr(CodeInvalidName, false, nil, "verify depth %q: meta, bindings or data", opts.Depth)
	}
	if opts.Snapshot != "" && opts.AllSnapshots {
		return res, catErr(CodeInvalidName, false, nil, "verify: a named snapshot and all snapshots do not combine")
	}
	if opts.Snapshot != "" {
		if err := CheckSnapshotName(opts.Snapshot); err != nil {
			return res, err
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
	root, err := c.volRoot(vol)
	if err != nil {
		return res, err
	}
	v := &verifier{c: c, vol: vol, root: root, opts: opts, res: &res, blobs: c.blobs,
		segs: map[verifySegKey]verifiedSegment{}, chunks: map[string]*verifyChunk{}, deletedSnaps: map[verifyRoot]bool{},
		pacer: &verifyPacer{rate: opts.BytesPerSecond}}
	err = v.run(ctx)
	sort.SliceStable(res.Findings, func(i, j int) bool { return findingLess(res.Findings[i], res.Findings[j]) })
	for _, f := range res.Findings {
		res.FindingsByClass[f.Class]++
	}
	res.DistinctChunks = len(v.chunks)
	if err != nil {
		return res, err
	}
	res.Complete = true
	return res, nil
}

// verifySegKey names a segment reference: a segment object is immutable, so
// one verified under the same reference needs no second read.
type verifySegKey struct {
	group, chunkBytes uint64
	ref               storage.ChunkSegment
}

type verifiedSegment struct {
	chunks     map[uint64]storage.ChunkRef
	fid        string
	tombstoned bool
}

// verifyRoot names a root a pass checks: HEAD (head set) or one snapshot
// attempt (name and attempt).
type verifyRoot struct {
	label         string
	head          bool
	name, attempt string
}

// verifyUse is one root's reference to a chunk.
type verifyUse struct {
	root verifyRoot
	idx  uint64
	sha  [32]byte
}

// verifyChunk is one distinct chunk (owner volume + the owner's own key).
type verifyChunk struct {
	id         string
	owner, key string
	uses       []verifyUse
	fid        string
	bound      bool // a sound, untombstoned binding was found
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
	segs    map[verifySegKey]verifiedSegment // verified segments, by reference
	chunks  map[string]*verifyChunk          // by identity
	pending []*verifyChunk                   // not yet checked at depth bindings/data

	snapRoots    []verifyRoot        // the snapshots checked
	deletedSnaps map[verifyRoot]bool // snapshots found deleted during the pass (permanent)

	head        storage.ChunkHead // the current HEAD view (root only)
	hasHead     bool
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

func (v *verifier) superseded() {
	v.mu.Lock()
	v.res.SupersededDuringVerify++
	v.mu.Unlock()
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
		v.head, v.hasHead, v.lastRefresh = h, true, time.Now()
		hr := verifyRoot{label: headLabel(h.Gen), head: true}
		v.res.HeadGens = append(v.res.HeadGens, h.Gen)
		chunks, err := v.walk(ctx, hr, h)
		if err != nil {
			return err
		}
		v.addUses(hr, chunks, false)
		if err := v.intents(ctx, h, chunks); err != nil {
			return err
		}
	}
	v.hook(verifyHookHeadWalked)

	if err := v.snapshots(ctx); err != nil {
		return err
	}
	v.hook(verifyHookRootsWalked)
	if v.opts.Depth != VerifyMeta {
		for first := true; len(v.pending) > 0; first = false {
			batch := v.pending
			v.pending = nil
			if err := v.checkBindings(ctx, batch, first); err != nil {
				return err
			}
			if v.opts.Depth == VerifyData {
				if err := v.checkData(ctx, batch); err != nil {
					return err
				}
			}
			if err := v.maybeRefresh(ctx); err != nil {
				return err
			}
		}
	}
	// Name the snapshots deleted during the pass: not fully covered.
	for _, s := range v.snapRoots {
		if _, _, err := v.snapshotActive(ctx, s); err != nil {
			return err
		}
	}
	for _, s := range v.snapRoots {
		if v.deletedSnaps[s] {
			v.res.SnapshotsDeletedDuringVerify = append(v.res.SnapshotsDeletedDuringVerify, s.name+"/"+s.attempt)
		}
	}
	return nil
}

// ---- roots and segments ----

// segIssue is why a segment could not be used.
type segIssue struct {
	class, fid, detail string
}

// segment returns the verified chunks of h's segment group (keys
// owner-qualified), reading it unless this pass already verified the same
// reference. A non-nil issue is a problem with the segment; an error stops
// the pass. tombstoned reports a segment binding GC has tombstoned.
func (v *verifier) segment(ctx context.Context, h storage.ChunkHead, group uint64) (chunks map[uint64]storage.ChunkRef, fid string, tombstoned bool, issue *segIssue, err error) {
	ref := h.Segments[group]
	k := verifySegKey{group: group, chunkBytes: h.ChunkBytes, ref: ref}
	v.mu.Lock()
	cached, ok := v.segs[k]
	v.mu.Unlock()
	if ok {
		return cached.chunks, cached.fid, cached.tombstoned, nil, nil
	}
	if v.blobs == nil {
		return nil, "", false, nil, errors.New("chunkstore: verify needs a blob reader for segmented roots")
	}
	r, err := NewChunkRemote(v.c.f, v.blobs, v.root)
	if err != nil {
		return nil, "", false, nil, err
	}
	dir, name, err := r.keyLocation(ref.Key)
	if err != nil {
		return nil, "", false, &segIssue{FindingSegmentUnreadable, "", err.Error()}, nil
	}
	e, err := v.c.f.LookupEntry(ctx, dir, name)
	if errors.Is(err, ErrNotFound) {
		return nil, "", false, &segIssue{FindingSegmentUnreadable, "", "segment binding not found"}, nil
	}
	if err != nil {
		return nil, "", false, nil, abortErr(ctx, err, "read segment binding %s/%s", dir, name)
	}
	if bad := bindingProblem(e); bad != "" {
		return nil, "", false, &segIssue{FindingSegmentUnreadable, string(e.Extended[extFid]), "segment binding: " + bad}, nil
	}
	fid = string(e.Extended[extFid])
	tombstoned = len(e.Extended[extGCDeleted]) != 0
	v.mu.Lock()
	v.res.SegmentReads++
	v.mu.Unlock()
	body, err := v.fetch(ctx, fid)
	switch {
	case err == nil:
	case isContextErr(ctx, err):
		return nil, "", false, nil, abortErr(ctx, err, "read segment %s", ref.Key)
	case errors.Is(err, ErrBlobNotFound):
		return nil, fid, tombstoned, &segIssue{FindingSegmentUnreadable, fid, "segment needle not found: " + err.Error()}, nil
	default:
		return nil, fid, tombstoned, &segIssue{FindingReadError, fid, readErrorDetail(err)}, nil
	}
	// Verify the bytes read through the same decoder GC and the chunk
	// layer use (hash, size bound, strict decode, contents).
	r, err = NewChunkRemote(v.c.f, verifyPrefetched{fid, body}, v.root)
	if err != nil {
		return nil, "", false, nil, err
	}
	r.mu.Lock()
	r.fids[ref.Key] = fid
	r.mu.Unlock()
	one := storage.ChunkHead{Segments: map[uint64]storage.ChunkSegment{group: ref}, ChunkBytes: h.ChunkBytes,
		ManifestChunkCount: ref.Count, RemoteLSN: h.RemoteLSN, State: storage.ChunkHeadActive}
	resolved, err := r.resolveManifest(ctx, one)
	if err != nil {
		return nil, fid, tombstoned, &segIssue{FindingSegmentUnreadable, fid, err.Error()}, nil
	}
	v.mu.Lock()
	v.segs[k] = verifiedSegment{resolved.Chunks, fid, tombstoned}
	v.mu.Unlock()
	return resolved.Chunks, fid, tombstoned, nil, nil
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

// walk checks one root (meta depth) and returns its chunk map.
func (v *verifier) walk(ctx context.Context, r verifyRoot, h storage.ChunkHead) (map[uint64]storage.ChunkRef, error) {
	label := r.label
	out := map[uint64]storage.ChunkRef{}
	if h.Segments == nil {
		for idx, ref := range h.Chunks {
			out[idx] = ref
		}
	} else {
		if h.ChunkBytes != v.chunkBytes {
			v.finding(VerifyFinding{Root: label, Class: FindingGeometry,
				Detail: fmt.Sprintf("root chunk_bytes %d, VOLUME chunk size %d", h.ChunkBytes, v.chunkBytes)})
		}
		groups := make([]uint64, 0, len(h.Segments))
		for g := range h.Segments {
			groups = append(groups, g)
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i] < groups[j] })
		for _, g := range groups {
			ref := h.Segments[g]
			v.mu.Lock()
			v.res.SegmentsChecked++
			v.mu.Unlock()
			chunks, fid, tomb, issue, err := v.segment(ctx, h, g)
			if err != nil {
				return out, err
			}
			group := g
			if tomb && v.opts.Depth != VerifyMeta {
				if err := v.segmentMiss(ctx, r, group, ref, VerifyFinding{Class: FindingBindingTombstoned, FID: fid, Detail: "segment binding tombstoned by GC"}); err != nil {
					return out, err
				}
			}
			if issue != nil {
				if err := v.segmentMiss(ctx, r, group, ref, VerifyFinding{Class: issue.class, FID: issue.fid, Detail: issue.detail}); err != nil {
					return out, err
				}
				continue
			}
			for idx, cr := range chunks {
				out[idx] = cr
			}
		}
	}
	idxs := sortedIndexes(out)
	for _, idx := range idxs {
		if idx >= v.nChunks {
			i := idx
			owner, key := splitIdentity(chunkIdentity(v.vol, out[idx].Key))
			v.finding(VerifyFinding{Root: label, Index: &i, Owner: owner, Key: key, Class: FindingGeometry,
				Detail: fmt.Sprintf("chunk index %d outside the volume (%d chunks)", idx, v.nChunks)})
		}
	}
	return out, nil
}

// segmentMiss reports a segment problem, unless it is under a HEAD root
// that no longer names that segment, or under a snapshot deleted since.
func (v *verifier) segmentMiss(ctx context.Context, r verifyRoot, group uint64, ref storage.ChunkSegment, f VerifyFinding) error {
	if r.head {
		h, ok, err := v.readHeadRoot(ctx)
		switch {
		case errors.Is(err, errUndecodableHead):
			// cannot excuse the miss: report it
		case err != nil:
			return err
		case !ok || h.Segments[group] != ref:
			v.superseded()
			return nil
		}
	} else {
		active, _, err := v.snapshotActive(ctx, r)
		if err != nil {
			return err
		}
		if !active {
			v.rootDeleted()
			return nil
		}
	}
	f.Root, f.Segment, f.Owner, f.Key = r.label, &group, ref.Owner, ref.Key
	v.finding(f)
	return nil
}

func (v *verifier) rootDeleted() {
	v.mu.Lock()
	v.res.RootDeletedDuringVerify++
	v.mu.Unlock()
}

// snapshotActive reads snapshot r again: its name record must still name
// r's attempt, and the attempt entry must still exist, active. A name
// record that is gone or names another attempt, or an attempt entry that is
// gone, aborted or marked deleting, means r was deleted (permanently: the
// attempt path is never reused). An entry that does not decode cannot
// excuse a miss: it counts as active, with a nil head.
func (v *verifier) snapshotActive(ctx context.Context, r verifyRoot) (active bool, head *storage.ChunkHead, err error) {
	v.mu.Lock()
	gone := v.deletedSnaps[r]
	v.mu.Unlock()
	if gone {
		return false, nil, nil
	}
	deleted := func() (bool, *storage.ChunkHead, error) {
		v.mu.Lock()
		v.deletedSnaps[r] = true
		v.mu.Unlock()
		return false, nil, nil
	}
	ne, err := v.c.f.LookupEntry(ctx, path.Join(v.root, snapNamesDir), r.name)
	if errors.Is(err, ErrNotFound) {
		return deleted()
	}
	if err != nil {
		return false, nil, abortErr(ctx, err, "re-read snapshot name %s/%s", v.vol, r.name)
	}
	rec, err := decodeNameRecord(path.Join(v.root, snapNamesDir, r.name), ne)
	if err == nil && rec.Attempt != r.attempt {
		return deleted()
	}
	dir := path.Join(v.root, snapsDir, r.name)
	e, err := v.c.f.LookupEntry(ctx, dir, r.attempt)
	if errors.Is(err, ErrNotFound) {
		return deleted()
	}
	if err != nil {
		return false, nil, abortErr(ctx, err, "re-read snapshot %s/%s/%s", v.vol, r.name, r.attempt)
	}
	if st := string(e.Extended[extSnapState]); st == SnapAborted || st == SnapDeleting || len(e.Extended[extSnapDeletedNs]) != 0 {
		return deleted()
	}
	info, _, err := decodeSnap(path.Join(dir, r.attempt), e)
	if err != nil {
		return true, nil, nil
	}
	h := info.Head()
	return true, &h, nil
}

// readHeadRoot reads the current HEAD root. ok is false when there is none;
// a HEAD that does not decode is errUndecodableHead.
func (v *verifier) readHeadRoot(ctx context.Context) (storage.ChunkHead, bool, error) {
	e, err := v.c.f.LookupEntry(ctx, v.root, headName)
	if errors.Is(err, ErrNotFound) {
		return storage.ChunkHead{}, false, nil
	}
	if err != nil {
		return storage.ChunkHead{}, false, abortErr(ctx, err, "re-read HEAD of %s", v.vol)
	}
	h, err := decodeHead(path.Join(v.root, headName), e)
	if err != nil {
		return storage.ChunkHead{}, false, errUndecodableHead
	}
	return h, true, nil
}

// errUndecodableHead: a re-read HEAD that does not decode cannot excuse a
// miss; the caller reports it.
var errUndecodableHead = errors.New("chunkstore: verify: HEAD does not decode")

// stillNamed reports whether root r still names chunk identity id at index
// idx at report time, and counts why when it does not. For HEAD it reads
// the root again and, for a segmented root, the one segment covering idx
// (superseded during verify when the key is gone). For a snapshot it reads
// the name record and the attempt again (root deleted during verify when
// the attempt is no longer active). Anything it cannot read or decode
// counts as still naming the chunk: a miss is never excused on a guess.
func (v *verifier) stillNamed(ctx context.Context, r verifyRoot, idx uint64, id string) (bool, error) {
	var h storage.ChunkHead
	if r.head {
		cur, ok, err := v.readHeadRoot(ctx)
		if errors.Is(err, errUndecodableHead) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		if !ok {
			v.superseded()
			return false, nil
		}
		h = cur
	} else {
		active, head, err := v.snapshotActive(ctx, r)
		if err != nil {
			return false, err
		}
		if !active {
			v.rootDeleted()
			return false, nil
		}
		if head == nil {
			return true, nil
		}
		h = *head
	}
	named, err := v.names(ctx, h, idx, id)
	if err == nil && !named {
		v.superseded()
	}
	return named, err
}

// names reports whether root h names chunk identity id at index idx.
func (v *verifier) names(ctx context.Context, h storage.ChunkHead, idx uint64, id string) (bool, error) {
	if h.Segments == nil {
		ref, has := h.Chunks[idx]
		return has && chunkIdentity(v.vol, ref.Key) == id, nil
	}
	if h.ChunkBytes == 0 {
		return true, nil
	}
	group := idx / (manifestSegmentBytes / h.ChunkBytes)
	if _, has := h.Segments[group]; !has {
		return false, nil
	}
	chunks, _, _, issue, err := v.segment(ctx, h, group)
	if err != nil {
		return false, err
	}
	if issue != nil {
		return true, nil
	}
	ref, has := chunks[idx]
	return has && chunkIdentity(v.vol, ref.Key) == id, nil
}

// addUses records r's chunk references. onlyNew (a refreshed HEAD)
// adds only chunks this pass has not seen: an unchanged key was already
// checked, and its content cannot have changed.
func (v *verifier) addUses(r verifyRoot, chunks map[uint64]storage.ChunkRef, onlyNew bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, idx := range sortedIndexes(chunks) {
		ref := chunks[idx]
		id := chunkIdentity(v.vol, ref.Key)
		w := v.chunks[id]
		if w != nil && onlyNew {
			continue
		}
		if w == nil {
			owner, key := splitIdentity(id)
			w = &verifyChunk{id: id, owner: owner, key: key}
			v.chunks[id] = w
			v.pending = append(v.pending, w)
		}
		w.uses = append(w.uses, verifyUse{root: r, idx: idx, sha: ref.SHA256})
		v.res.ChunksChecked++
	}
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
		return v.snapshot(ctx, ref.info)
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
			if err := v.snapshot(ctx, ref.info); err != nil {
				return err
			}
		}
		if len(page) < listPage {
			return nil
		}
	}
}

func (v *verifier) snapshot(ctx context.Context, s SnapshotInfo) error {
	r := verifyRoot{label: "snapshot " + s.Name + "/" + s.attempt, name: s.Name, attempt: s.attempt}
	v.res.Snapshots = append(v.res.Snapshots, s.Name+"/"+s.attempt)
	v.snapRoots = append(v.snapRoots, r)
	if s.geo != (Geometry{}) && s.geo != v.geo {
		v.finding(VerifyFinding{Root: r.label, Class: FindingGeometry, Detail: fmt.Sprintf("snapshot geometry %+v, VOLUME %+v", s.geo, v.geo)})
	}
	chunks, err := v.walk(ctx, r, s.Head())
	if err != nil {
		return err
	}
	v.addUses(r, chunks, false)
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

// maybeRefresh reads HEAD again once RefreshInterval has passed since the
// last view. A new generation is walked (only segments whose reference
// changed are read) and its chunks this pass has not seen are queued.
func (v *verifier) maybeRefresh(ctx context.Context) error {
	if !v.hasHead || time.Since(v.lastRefresh) < v.opts.RefreshInterval {
		return nil
	}
	v.lastRefresh = time.Now()
	e, err := v.c.f.LookupEntry(ctx, v.root, headName)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return abortErr(ctx, err, "refresh HEAD of %s", v.vol)
	}
	h, err := decodeHead(path.Join(v.root, headName), e)
	if err != nil {
		if !v.badRefresh {
			v.badRefresh = true
			v.finding(VerifyFinding{Root: "HEAD", Class: FindingRootInvalid, Detail: "refreshed HEAD: " + err.Error()})
		}
		return nil
	}
	if h.Gen == v.head.Gen && h.Epoch == v.head.Epoch {
		return nil
	}
	hr := verifyRoot{label: headLabel(h.Gen), head: true}
	v.res.HeadGens = append(v.res.HeadGens, h.Gen)
	chunks, err := v.walk(ctx, hr, h)
	if err != nil {
		return err
	}
	v.addUses(hr, chunks, true)
	v.head = h
	return nil
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
		if err := v.evaluateBinding(ctx, w, e); err != nil {
			return err
		}
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

func (v *verifier) evaluateBinding(ctx context.Context, w *verifyChunk, e *filerpb.Entry) error {
	if e == nil {
		return v.miss(ctx, w, FindingBindingMissing, fmt.Sprintf("no binding in %s/keys", w.owner))
	}
	w.fid = string(e.Extended[extFid])
	if bad := bindingProblem(e); bad != "" {
		return v.miss(ctx, w, FindingBindingMissing, "invalid binding: "+bad)
	}
	if t := e.Extended[extGCDeleted]; len(t) != 0 {
		return v.miss(ctx, w, FindingBindingTombstoned, fmt.Sprintf("binding tombstoned by GC (%s=%s)", extGCDeleted, t))
	}
	w.bound = true
	return nil
}

// miss reports a miss of w under all of its roots.
func (v *verifier) miss(ctx context.Context, w *verifyChunk, class, detail string) error {
	return v.missUses(ctx, w, w.uses, class, func(verifyUse) string { return detail })
}

// missUses reports a miss of w as at most one finding: under the first of
// uses whose root still names w at that index at report time (HEAD first,
// then snapshots in the order checked). A root that no longer names it is
// counted (superseded, or root deleted, during verify) and not reported.
func (v *verifier) missUses(ctx context.Context, w *verifyChunk, uses []verifyUse, class string, detail func(verifyUse) string) error {
	for _, u := range uses {
		still, err := v.stillNamed(ctx, u.root, u.idx, w.id)
		if err != nil {
			return err
		}
		if !still {
			continue
		}
		idx := u.idx
		v.finding(VerifyFinding{Root: u.root.label, Index: &idx, Owner: w.owner, Key: w.key, FID: w.fid, Class: class, Detail: detail(u)})
		return nil
	}
	return nil
}

// ---- data ----

// checkData fetches every bound chunk of batch once, at bounded concurrency
// and paced, and checks it against each reference.
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
	derr := dispatch()
	close(jobs)
	wg.Wait()
	if err := getErr(); err != nil {
		return err
	}
	return derr
}

func (v *verifier) readChunk(ctx context.Context, w *verifyChunk) error {
	data, err := v.fetch(ctx, w.fid)
	switch {
	case err == nil:
	case isContextErr(ctx, err):
		return abortErr(ctx, err, "read chunk %s", w.fid)
	case errors.Is(err, ErrBlobNotFound):
		return v.miss(ctx, w, FindingNeedleMissing, err.Error())
	default:
		return v.miss(ctx, w, FindingReadError, readErrorDetail(err))
	}
	v.mu.Lock()
	v.res.DistinctChunksRead++
	v.res.BytesRead += int64(len(data))
	v.mu.Unlock()
	if uint64(len(data)) != v.chunkBytes {
		return v.miss(ctx, w, FindingSizeMismatch, fmt.Sprintf("needle holds %d bytes, chunk size %d", len(data), v.chunkBytes))
	}
	sum := sha256.Sum256(data)
	var mismatched []verifyUse
	for _, u := range w.uses {
		if sum != u.sha {
			mismatched = append(mismatched, u)
		}
	}
	return v.missUses(ctx, w, mismatched, FindingContentMismatch, func(u verifyUse) string {
		return fmt.Sprintf("needle SHA-256 %s, reference %s", hex.EncodeToString(sum[:]), hex.EncodeToString(u.sha[:]))
	})
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
