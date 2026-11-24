package chunkstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/protobuf/proto"
)

const extGCDeleted = "swb.gc.deleted"

// DefaultGCClockSkew is the allowed pairwise difference between timestamp
// writers and collectors. Deployments with a larger bound must raise ClockSkew;
// no finite time-based grace can be safe with unbounded clock differences.
const DefaultGCClockSkew = time.Minute

// BlobReader reads one immutable needle (a manifest segment, during the
// mark). GC never deletes a needle: it reclaims a key by dropping the
// binding's chunk, and the filer deletes the dropped needle.
type BlobReader interface {
	Get(context.Context, string) ([]byte, error)
}

// GCConfig binds a collector to the authoritative blockmaster epoch reader.
// IssuedEpoch must read the highest epoch issued for this volume, never a
// lease expiry or a cached guess. Unknown authority retains the intent.
type GCConfig struct {
	Grace, IntentTTL time.Duration
	// ClockSkew bounds pairwise wall-clock differences. Zero uses the default.
	// Binding creation timestamps additionally lose up to one second of precision.
	ClockSkew time.Duration
	// CASApplyBound is the margin for a publish applied after its fork's
	// deadline check. Zero uses the catalog operation deadline. A publish
	// known to have succeeded was applied before its context ended, and that
	// context ends at readAt + deadline, so it is inside the deadline; an
	// unknown outcome keeps the fork intent, which roots the keys until the
	// target is resolved, with no time bound. This bound covers apply delay
	// and clock-rate drift beyond those two. NewCollector requires
	// Grace >= operation deadline + ClockSkew + CASApplyBound, so a snapshot
	// deleted while a fork of it is in flight stays rooted until the fork can
	// no longer publish (INV-SNAP-DELETE-ANY-ORDER-001).
	CASApplyBound time.Duration
	IssuedEpoch   func(context.Context, string) (uint64, error)
	Now           func() time.Time
}

type GCRoot struct {
	Kind   string   `json:"kind"`
	Volume string   `json:"volume"`
	ID     string   `json:"id"`
	Keys   []string `json:"keys"`
	Epoch  uint64   `json:"epoch,omitempty"`
	Holder string   `json:"holder,omitempty"`
}
type GCObject struct {
	Volume string `json:"volume"`
	Key    string `json:"key"`
	FID    string `json:"fid"`
}

// GCResult reports one pass. Reclaimed counts the bindings this pass turned
// from live into the done tombstone: unbound, their needles handed to the
// filer's deletion queue (it does not prove a needle gone). A needle that was
// uploaded but never bound, or whose deletion the filer lost, is referenced
// by no entry: stock volume.fsck lists it as an orphan.
type GCResult struct {
	Roots                                                   []GCRoot   `json:"roots"`
	WouldDelete                                             []GCObject `json:"would_delete"`
	WouldExpireIntents                                      []string   `json:"would_expire_intents"`
	Volumes, Intents, ExpiredIntents, Candidates, Reclaimed int
}

type gcCandidate struct {
	since  time.Time
	digest [32]byte
}
type gcBinding struct {
	volume string
	entry  *filerpb.Entry
	digest [32]byte
}
type gcIntent struct {
	remote *ChunkRemote
	intent storage.ChunkIntent
}

// Collector traces all catalog roots. Calls are serialized per collector;
// restarting loses candidates, which adds delay rather than authorizing a
// deletion. No WAL, publication or shard lock is held during remote I/O.
// Two collectors cannot both reclaim a binding (the reclaim is conditional on
// it being live) and cannot shorten either one's two-mark grace requirement.
type Collector struct {
	scope      string // empty traces and reclaims the entire namespace
	cat        *Catalog
	blobs      BlobReader
	cfg        GCConfig
	mu         sync.Mutex
	candidates map[string]gcCandidate
	last       time.Time
}

func NewCollector(cat *Catalog, blobs BlobReader, cfg GCConfig) (*Collector, error) {
	if cat == nil || blobs == nil || cfg.IntentTTL <= 0 || cfg.IssuedEpoch == nil {
		return nil, errors.New("chunkstore: GC requires catalog, a blob reader, positive intent TTL and epoch authority")
	}
	if cfg.ClockSkew == 0 {
		cfg.ClockSkew = DefaultGCClockSkew
	}
	if cfg.ClockSkew < 0 || cfg.ClockSkew > time.Duration(1<<63-1)-time.Second {
		return nil, errors.New("chunkstore: invalid GC clock skew bound")
	}
	if cfg.CASApplyBound == 0 {
		cfg.CASApplyBound = cat.deadline()
	}
	const maxDur = time.Duration(1<<63 - 1)
	d := cat.deadline()
	if cfg.CASApplyBound < 0 || cfg.CASApplyBound > maxDur-d-cfg.ClockSkew {
		return nil, errors.New("chunkstore: invalid GC CAS apply bound")
	}
	if need := d + cfg.ClockSkew + cfg.CASApplyBound; cfg.Grace < need {
		return nil, fmt.Errorf("chunkstore: GC grace %v is below operation deadline %v + clock skew %v + CAS apply bound %v = %v", cfg.Grace, d, cfg.ClockSkew, cfg.CASApplyBound, need)
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Collector{cat: cat, blobs: blobs, cfg: cfg, candidates: map[string]gcCandidate{}}, nil
}

// Run completes a mark before any chunk deletion. Any failed mark discards
// previous candidates. Roots include active and deleting snapshots until
// the catalog has actually removed them; this collector never infers their
// absence from an age or a retention label.
func (g *Collector) Run(ctx context.Context) (GCResult, error) { return g.run(ctx, true) }

// Preview performs the same mark and admission as Run, with no metadata,
// blob, intent or candidate-state mutations.
func (g *Collector) Preview(ctx context.Context) (GCResult, error) { return g.run(ctx, false) }

func (g *Collector) run(ctx context.Context, apply bool) (result GCResult, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	result.Roots = []GCRoot{}
	result.WouldDelete = []GCObject{}
	result.WouldExpireIntents = []string{}
	defer func() {
		if err != nil && apply {
			g.candidates = map[string]gcCandidate{}
		}
	}()
	start := g.cfg.Now()
	if !g.last.IsZero() && start.Before(g.last) {
		// Discard coverage across a backward clock step; a fresh complete mark
		// can still make progress without borrowing time from the future.
		if apply {
			g.candidates = map[string]gcCandidate{}
		}
		// Preview must also ignore the old coverage without mutating it.
		// Candidate lookup below is guarded by this same condition.
	}
	roots := map[string]bool{}
	var bindings []gcBinding
	var expired []gcIntent
	volumes, err := g.cat.ListVolumes(ctx)
	if err != nil {
		return result, err
	}
	result.Volumes = len(volumes)
	for _, vol := range volumes {
		r, e := NewChunkRemote(g.cat.f, gcBlobReader{g.blobs}, path.Join(g.cat.base, vol))
		if e != nil {
			return result, e
		}
		// The format gate, and whether the volume is live: a live volume
		// whose HEAD is absent is checked below (INV-GC-HEAD-ABSENT-001).
		v, hasVolume, e := g.cat.ReadVolume(ctx, vol)
		if e != nil {
			return result, e
		}
		headPresent, e := g.cat.headExists(ctx, path.Join(g.cat.base, vol))
		if e != nil {
			return result, e
		}
		h, e := r.GetHead(ctx)
		if e != nil {
			return result, e
		}
		intentNamed := map[string]bool{}
		addHead := func(kind, id string, head storage.ChunkHead) error {
			var err error
			head, err = r.resolveManifest(ctx, head)
			if err != nil {
				return err
			}
			for _, ref := range head.Segments {
				dir, name, err := r.keyLocation(ref.Key)
				if err != nil {
					return err
				}
				roots[path.Join(dir, name)] = true
			}

			keys := make([]string, 0, len(head.Chunks)+len(head.Segments))
			for _, ref := range head.Segments {
				keys = append(keys, ref.Key)
			}
			for _, ref := range head.Chunks {
				keys = append(keys, ref.Key)
			}
			sort.Strings(keys)
			result.Roots = append(result.Roots, GCRoot{Kind: kind, Volume: vol, ID: id, Keys: keys, Epoch: head.Epoch, Holder: head.Holder})
			for _, ref := range head.Chunks {
				dir, name, e := r.keyLocation(ref.Key)
				if e != nil {
					return e
				}
				roots[path.Join(dir, name)] = true
			}
			return nil
		}
		if e = addHead("HEAD", headName, h); e != nil {
			return result, e
		}
		after := ""
		for {
			snaps, next, e := g.cat.ListSnapshots(ctx, vol, ListOptions{StartAfter: after, IncludeDeleting: true, Limit: maxListLimit})
			if e != nil {
				return result, e
			}
			for _, snap := range snaps {
				if e = addHead("snapshot", snap.Name, snap.Head()); e != nil {
					return result, e
				}
			}
			if next == "" {
				break
			}
			if next <= after {
				return result, errors.New("chunkstore: non-advancing snapshot listing")
			}
			after = next
		}
		attempts, e := g.snapshotAttemptHeads(ctx, vol, start)
		if e != nil {
			return result, e
		}
		for _, attempt := range attempts {
			if e = addHead("snapshot_attempt", attempt.Name, attempt.Head); e != nil {
				return result, e
			}
		}
		intents, e := r.ListIntents(ctx)
		if e != nil {
			return result, e
		}
		for _, in := range intents {
			for _, key := range in.Keys {
				if dir, name, e := r.keyLocation(key); e == nil {
					intentNamed[path.Join(dir, name)] = true
				}
			}
			result.Intents++
			if in.CreatedNs <= 0 {
				return result, errors.New("chunkstore: invalid intent creation time")
			}
			expire := false
			if !strings.HasPrefix(in.Holder, "snapshot:") && !strings.HasPrefix(in.Holder, "fork:") && g.oldEnough(start, time.Unix(0, in.CreatedNs), g.cfg.IntentTTL, 0) {
				issued, authorityErr := g.cfg.IssuedEpoch(ctx, vol)
				// Catalog copy intents are retained: their exclusive creates
				// are not epoch-fenced. For a capture, both the authority
				// and the actual CAS domain must have
				// advanced. Mere lease loss, or issuance without takeover,
				// cannot authorize expiry of a paused publisher's intent.
				expire = (g.scope == "" || g.scope == vol) && authorityErr == nil && issued > in.Epoch && h.Epoch > in.Epoch && h.Epoch <= issued
			}
			if expire {
				expired = append(expired, gcIntent{r, in})
				result.WouldExpireIntents = append(result.WouldExpireIntents, path.Join(vol, in.ID))
			} else {
				kind := "capture_intent"
				if strings.HasPrefix(in.Holder, "snapshot:") || strings.HasPrefix(in.Holder, "fork:") {
					kind = "retained_copy_intent"
				}
				if in.Segments != nil {
					h := storage.ChunkHead{Segments: in.Segments, ChunkBytes: in.ChunkBytes}
					// Intents protect keys without carrying a published frontier.
					// Validate segment maxima against the reference maxima here.
					for group, ref := range h.Segments {
						if ref.SegFormat == 0 {
							ref.SegFormat = 1
							h.Segments[group] = ref
						}
						h.RemoteLSN = max(h.RemoteLSN, ref.MaxBirthLSN)
					}
					for _, ref := range in.Segments {
						h.ManifestChunkCount += ref.Count
					}
					if err := addHead(kind, in.ID, h); err != nil {
						return result, err
					}
				}
				result.Roots = append(result.Roots, GCRoot{Kind: kind, Volume: vol, ID: in.ID, Keys: append([]string{}, in.Keys...), Epoch: in.Epoch, Holder: in.Holder})
				for _, key := range in.Keys {
					dir, name, e := r.keyLocation(key)
					if e != nil {
						return result, e
					}
					roots[path.Join(dir, name)] = true
				}
			}
		}
		// INV-MAINT-SCOPE: roots above remain catalog-wide; candidates are local.
		if g.scope != "" && g.scope != vol {
			continue
		}
		pageAfter := ""
		for {
			page, e := g.cat.f.ListEntries(ctx, r.keyDir(), "", pageAfter, listPage)
			if e != nil {
				return result, e
			}
			for _, entry := range page {
				if entry.GetName() <= pageAfter || entry.Attributes == nil || entry.Attributes.Crtime <= 0 {
					return result, fmt.Errorf("chunkstore: invalid GC binding %s/%s", r.keyDir(), entry.GetName())
				}
				bound, e := decodeBinding(entry)
				if e != nil {
					return result, fmt.Errorf("chunkstore: invalid GC binding %s/%s: %v", r.keyDir(), entry.GetName(), e)
				}
				if hasVolume && v.State == "" && !headPresent && !bound.done && !intentNamed[path.Join(r.keyDir(), entry.GetName())] {
					// HEAD was removed (a stock tool pruning an entry whose
					// needle is missing, or by hand): its keys would all look
					// unrooted. Refuse the pass rather than reclaim the volume.
					return result, fmt.Errorf("chunkstore: volume %s is live but has no HEAD while %s/%s is a live binding no intent names; GC refuses (restore HEAD, or delete the volume)", vol, r.keyDir(), entry.GetName())
				}
				key, e := url.PathUnescape(entry.GetName())
				if e != nil {
					return result, e
				}
				dir, name, e := r.keyLocation(key)
				if e != nil || dir != r.keyDir() || name != entry.GetName() {
					return result, errors.New("chunkstore: noncanonical GC binding")
				}
				raw, e := proto.MarshalOptions{Deterministic: true}.Marshal(entry)
				if e != nil {
					return result, e
				}
				bindings = append(bindings, gcBinding{vol, entry, sha256.Sum256(raw)})
				pageAfter = entry.GetName()
			}
			if len(page) < listPage {
				break
			}
		}
	}
	if g.cfg.Now().Sub(start)+g.cat.deadline() >= g.cfg.Grace {
		return result, errors.New("chunkstore: GC mark exceeded grace safety margin")
	}
	// An intent is retired only after the complete mark and confirmed fencing.
	for _, x := range expired {
		if !apply {
			continue
		}
		if err = x.remote.DeleteIntent(ctx, x.intent.ID); err != nil {
			return result, err
		}
		result.ExpiredIntents++
	}
	nextCandidates := map[string]gcCandidate{}
	for _, b := range bindings {
		p := path.Join(g.cat.base, b.volume, keysDir, b.entry.Name)
		if roots[p] {
			if len(b.entry.Extended[extGCDeleted]) != 0 {
				return result, fmt.Errorf("chunkstore: root references reclaimed binding %s", p)
			}
			continue
		}
		if string(b.entry.Extended[extGCDeleted]) == "done" {
			continue
		}
		created := time.Unix(b.entry.Attributes.Crtime, 0)
		if !g.oldEnough(start, created, g.cfg.Grace, time.Second) {
			continue
		}
		candidate, seen := g.candidates[p]
		if !g.last.IsZero() && start.Before(g.last) {
			seen = false
		}
		if !seen || candidate.digest != b.digest {
			candidate = gcCandidate{start, b.digest}
		}
		result.Candidates++
		if !g.oldEnough(start, candidate.since, g.cfg.Grace, 0) {
			nextCandidates[p] = candidate
			continue
		}
		live, _ := decodeBinding(b.entry) // validated when listed
		result.WouldDelete = append(result.WouldDelete, GCObject{Volume: b.volume, Key: mustUnescapeGCKey(b.entry.Name), FID: live.fid})
		if !apply {
			continue
		}
		if err = ctx.Err(); err != nil {
			return result, err
		}
		err = reclaimBinding(ctx, g.cat.f, path.Dir(p), b.entry)
		if err != nil && ctx.Err() != nil {
			err = ctx.Err()
		}
		if err != nil {
			return result, err
		}
		result.Reclaimed++
	}
	if apply {
		g.candidates = nextCandidates
		g.last = start
	}
	return result, nil
}

// nilBlobs is used only for read-only metadata traversal, never chunk I/O.
type nilBlobs struct{}

func (nilBlobs) Put(context.Context, []byte) (string, error) {
	return "", errors.New("chunkstore: metadata-only remote")
}
func (nilBlobs) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("chunkstore: metadata-only remote")
}

// The binding name was already validated before reclamation admission.
func mustUnescapeGCKey(name string) string { k, _ := url.PathUnescape(name); return k }

// gcBlobReader gives the mark its read-only remote: segments are read,
// nothing is uploaded.
type gcBlobReader struct{ BlobReader }

func (b gcBlobReader) Put(context.Context, []byte) (string, error) {
	return "", errors.New("chunkstore: GC is read-only during mark")
}

// reclaimBinding reclaims a key by one conditional UpdateEntry: the binding
// is rewritten with no chunk and swb.gc.deleted=done, accepted only while it
// is still live (kind chunkkey, swb.gc.deleted absent). The filer deletes the
// dropped chunk's needle after storing the update (its own deletion queue);
// GC deletes no needle. The binding stays for ever, so the key is never
// rebound. A second collector reclaiming the same binding loses the condition
// and its pass ends. The caller decides that nothing references the key.
func reclaimBinding(ctx context.Context, f Filer, dir string, entry *filerpb.Entry) error {
	e := proto.Clone(entry).(*filerpb.Entry)
	e.Chunks = nil
	if e.Attributes != nil {
		e.Attributes.FileSize = 0 // no chunk: a stock read of the file is empty, not zeros
	}
	e.Extended[extGCDeleted] = []byte("done")
	return f.UpdateEntry(ctx, dir, e, map[string][]byte{extKind: []byte(kindChunkKey), extGCDeleted: nil})
}

// INV-GC-CLOCK-SKEW-001: expiry uses a lower bound on elapsed time,
// never the raw difference of timestamps from different hosts. Subtracting
// separately avoids overflowing grace + skew for large configured durations.
func (g *Collector) oldEnough(now, stamp time.Time, grace, resolution time.Duration) bool {
	age := now.Sub(stamp)
	return age > grace && age-grace > g.cfg.ClockSkew+resolution
}
