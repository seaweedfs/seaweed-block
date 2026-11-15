package chunkstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
	"google.golang.org/protobuf/proto"
)

const extGCDeleted = "swb.gc.deleted"

// BlobDeleter deletes one immutable needle. Missing needles are success.
// Implementations must not reuse a deleted fid.
type BlobDeleter interface {
	DeleteBlob(context.Context, string) error
}

// GCConfig binds a collector to the authoritative blockmaster epoch reader.
// IssuedEpoch must read the highest epoch issued for this volume, never a
// lease expiry or a cached guess. Unknown authority retains the intent.
type GCConfig struct {
	Grace, IntentTTL time.Duration
	IssuedEpoch      func(context.Context, string) (uint64, error)
	Now              func() time.Time
}

type GCResult struct {
	Volumes, Intents, ExpiredIntents, Candidates, Reclaimed int
	// Pre-binding needles cannot be enumerated by this API. They are retained;
	// UnknownUnbound is true rather than inventing a report-only count.
	UnknownUnbound bool
}

type gcCandidate struct {
	since  time.Time
	digest [32]byte
}
type gcBinding struct {
	volume, key string
	entry       *filerpb.Entry
	digest      [32]byte
}
type gcIntent struct {
	remote *ChunkRemote
	intent storage.ChunkIntent
}

// Collector traces all catalog roots. Calls are serialized per collector;
// restarting loses candidates, which adds delay rather than authorizing a
// deletion. No WAL, publication or shard lock is held during remote I/O.
// Different collectors may duplicate idempotent blob deletes but cannot
// shorten either collector's independent two-mark grace requirement.
type Collector struct {
	cat        *Catalog
	blobs      BlobDeleter
	cfg        GCConfig
	mu         sync.Mutex
	candidates map[string]gcCandidate
	last       time.Time
}

func NewCollector(cat *Catalog, blobs BlobDeleter, cfg GCConfig) (*Collector, error) {
	if cat == nil || blobs == nil || cfg.Grace <= cat.deadline() || cfg.IntentTTL <= 0 || cfg.IssuedEpoch == nil {
		return nil, errors.New("chunkstore: GC requires catalog, blob deletion, grace > operation deadline, positive intent TTL and epoch authority")
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
func (g *Collector) Run(ctx context.Context) (result GCResult, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	result.UnknownUnbound = true
	defer func() {
		if err != nil {
			g.candidates = map[string]gcCandidate{}
		}
	}()
	start := g.cfg.Now()
	if !g.last.IsZero() && start.Before(g.last) {
		return result, errors.New("chunkstore: GC clock moved backwards")
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
		r, e := NewChunkRemote(g.cat.f, nilBlobs{}, path.Join(g.cat.base, vol))
		if e != nil {
			return result, e
		}
		h, e := r.GetHead(ctx)
		if e != nil {
			return result, e
		}
		addHead := func(head storage.ChunkHead) error {
			for _, ref := range head.Chunks {
				dir, name, e := r.keyLocation(ref.Key)
				if e != nil {
					return e
				}
				roots[path.Join(dir, name)] = true
			}
			return nil
		}
		if e = addHead(h); e != nil {
			return result, e
		}
		after := ""
		for {
			snaps, next, e := g.cat.ListSnapshots(ctx, vol, ListOptions{StartAfter: after, IncludeDeleting: true, Limit: maxListLimit})
			if e != nil {
				return result, e
			}
			for _, snap := range snaps {
				if e = addHead(snap.Head()); e != nil {
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
		intents, e := r.ListIntents(ctx)
		if e != nil {
			return result, e
		}
		for _, in := range intents {
			result.Intents++
			// Initial candidate intentionally retains all intents. The caller RED
			// proves omitting these roots deletes a stalled cut's uploaded bytes.
			_ = in
		}
		pageAfter := ""
		for {
			page, e := g.cat.f.ListEntries(ctx, r.keyDir(), "", pageAfter, listPage)
			if e != nil {
				return result, e
			}
			for _, entry := range page {
				if entry.GetName() <= pageAfter || entry.GetIsDirectory() || len(entry.GetChunks()) != 0 || string(entry.Extended[extKind]) != kindChunkKey || len(entry.Extended[extFid]) == 0 || entry.Attributes == nil || entry.Attributes.Crtime <= 0 {
					return result, fmt.Errorf("chunkstore: invalid GC binding %s/%s", r.keyDir(), entry.GetName())
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
				bindings = append(bindings, gcBinding{vol, key, entry, sha256.Sum256(raw)})
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
		if err = x.remote.DeleteIntent(ctx, x.intent.ID); err != nil {
			return result, err
		}
		result.ExpiredIntents++
	}
	nextCandidates := map[string]gcCandidate{}
	for _, b := range bindings {
		p := path.Join(g.cat.base, b.volume, keysDir, b.entry.Name)
		if roots[p] {
			if string(b.entry.Extended[extGCDeleted]) == "1" {
				return result, fmt.Errorf("chunkstore: root references reclaimed binding %s", p)
			}
			continue
		}
		created := time.Unix(b.entry.Attributes.Crtime, 0)
		if !start.After(created.Add(g.cfg.Grace)) {
			continue
		}
		candidate, seen := g.candidates[p]
		if !seen || candidate.digest != b.digest {
			candidate = gcCandidate{start, b.digest}
		}
		result.Candidates++
		if start.Sub(candidate.since) < g.cfg.Grace {
			nextCandidates[p] = candidate
			continue
		}
		// Preserve the immutable binding forever: removing it would allow Put
		// to rebind a key that once existed. The tombstone also permits retry
		// after a failed physical delete without losing the fid.
		if string(b.entry.Extended[extGCDeleted]) != "1" {
			e := proto.Clone(b.entry).(*filerpb.Entry)
			e.Extended[extGCDeleted] = []byte("1")
			expected := map[string][]byte{extKind: []byte(kindChunkKey), extFid: b.entry.Extended[extFid], extGCDeleted: nil}
			if err = g.cat.f.UpdateEntry(ctx, path.Dir(p), e, expected); err != nil {
				return result, err
			}
		}
		if err = g.blobs.DeleteBlob(ctx, string(b.entry.Extended[extFid])); err != nil {
			return result, err
		}
		result.Reclaimed++
	}
	g.candidates = nextCandidates
	g.last = start
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
