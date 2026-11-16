package chunkstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

const gcMarkName = "_GC_MARK"
const gcMarkKind = "chunkgcmark"
const gcToken = "swb.gc.token"

type savedGCCandidate struct {
	SinceNs int64  `json:"since_ns"`
	Digest  string `json:"digest"`
}
type savedGCMark struct {
	Format       int                         `json:"format"`
	Base         string                      `json:"base"`
	GraceNs      int64                       `json:"grace_ns"`
	TTLNs        int64                       `json:"ttl_ns"`
	OpDeadlineNs int64                       `json:"op_deadline_ns"`
	Ready        bool                        `json:"ready"`
	LastNs       int64                       `json:"last_ns"`
	Candidates   map[string]savedGCCandidate `json:"candidates"`
}

type GCExecution struct {
	// MarkSaved confirms this caller received the completed-write response;
	// false does not prove that no completed mark exists remotely.
	MarkOutcome     string   `json:"mark_outcome"`
	UnknownOutcomes []string `json:"unknown_outcomes"`
	DryRun          bool     `json:"dry_run"`
	SettledCopies   int      `json:"settled_copy_intents"`
	MarkSaved       bool     `json:"mark_saved"`
	GCResult
}

// ExecuteGC binds independent CLI calls to completed filer-side marks.
// Preview never writes even the watermark. Apply first invalidates stored
// coverage with a conditional empty/in-progress record before other effects.
// A write may commit despite a lost response: errors retain known effects
// and identify uncertain stages, rather than asserting remote absence.
// The catalog namespace is the scope; permanent tombstones are untouched.
func (c *Catalog) ExecuteGC(ctx context.Context, blobs BlobDeleter, cfg GCConfig, apply bool) (out GCExecution, err error) {
	// Pre-binding needles remain unenumerable even if admission/readback fails.
	out.UnknownUnbound = true
	out.DryRun = !apply
	out.MarkOutcome = "not_attempted"
	out.UnknownOutcomes = []string{}
	g, err := NewCollector(c, blobs, cfg)
	if err != nil {
		return out, err
	}
	mark, previous, err := g.readMark(ctx)
	if err != nil {
		return out, err
	}
	if mark != nil && mark.Ready && mark.GraceNs == int64(cfg.Grace) && mark.TTLNs == int64(cfg.IntentTTL) && mark.OpDeadlineNs == int64(c.deadline()) {
		for p, candidate := range mark.Candidates {
			raw, _ := hex.DecodeString(candidate.Digest)
			var digest [32]byte
			copy(digest[:], raw)
			g.candidates[p] = gcCandidate{time.Unix(0, candidate.SinceNs), digest}
		}
		g.last = time.Unix(0, mark.LastNs)
	}
	if !apply {
		out.GCResult, err = g.Preview(ctx)
		return out, err
	}
	busy := g.mark(false)
	token, err := g.writeMark(ctx, busy, previous)
	if err != nil {
		out.MarkOutcome = "unknown"
		out.UnknownOutcomes = append(out.UnknownOutcomes, "in_progress_mark_write")
		return out, err
	}
	out.MarkOutcome = "in_progress"
	out.SettledCopies, err = g.SweepCopies(ctx)
	if err != nil {
		out.UnknownOutcomes = append(out.UnknownOutcomes, "copy_intent_settlement")
		return out, err
	}
	out.GCResult, err = g.Run(ctx)
	if err != nil {
		out.UnknownOutcomes = append(out.UnknownOutcomes, "reclamation")
		return out, err
	}
	_, err = g.writeMark(ctx, g.mark(true), token)
	if err != nil {
		out.MarkOutcome = "unknown"
		out.UnknownOutcomes = append(out.UnknownOutcomes, "completed_mark_write")
		return out, err
	}
	out.MarkOutcome = "complete"
	out.MarkSaved = true
	return out, nil
}

func (g *Collector) mark(ready bool) savedGCMark {
	m := savedGCMark{Format: 1, Base: g.cat.base, GraceNs: int64(g.cfg.Grace), TTLNs: int64(g.cfg.IntentTTL), OpDeadlineNs: int64(g.cat.deadline()), Ready: ready, Candidates: map[string]savedGCCandidate{}}
	if ready {
		m.LastNs = g.last.UnixNano()
		for p, c := range g.candidates {
			m.Candidates[p] = savedGCCandidate{c.since.UnixNano(), hex.EncodeToString(c.digest[:])}
		}
	}
	return m
}

func (g *Collector) readMark(ctx context.Context) (*savedGCMark, string, error) {
	e, err := g.cat.f.LookupEntry(ctx, g.cat.base, gcMarkName)
	if errors.Is(err, ErrNotFound) {
		return nil, "", nil
	}
	if err != nil {
		return nil, "", err
	}
	token := string(e.Extended[gcToken])
	if e.GetIsDirectory() || len(e.GetChunks()) != 0 || string(e.Extended[extKind]) != gcMarkKind || !storage.ValidChunkIntentID(token) || len(e.Content) > 64<<20 {
		return nil, "", errors.New("chunkstore: invalid GC mark entry")
	}
	var m savedGCMark
	d := json.NewDecoder(bytes.NewReader(e.Content))
	d.DisallowUnknownFields()
	if err = d.Decode(&m); err != nil {
		return nil, "", err
	}
	var trailing any
	if err = d.Decode(&trailing); err != io.EOF {
		return nil, "", errors.New("chunkstore: trailing GC mark content")
	}
	if m.Format != 1 || m.Base != g.cat.base || m.GraceNs <= 0 || m.TTLNs <= 0 || m.OpDeadlineNs <= 0 {
		return nil, "", errors.New("chunkstore: incompatible GC mark")
	}
	if m.Ready {
		now := g.cfg.Now().UnixNano()
		if m.LastNs <= 0 || m.LastNs > now {
			return nil, "", errors.New("chunkstore: invalid/future GC mark time")
		}
		for p, c := range m.Candidates {
			digest, err := hex.DecodeString(c.Digest)
			if err != nil || len(digest) != 32 || c.SinceNs <= 0 || c.SinceNs > m.LastNs || path.Clean(p) != p || !strings.HasPrefix(p, g.cat.base+"/") || !strings.Contains(p, "/"+keysDir+"/") {
				return nil, "", errors.New("chunkstore: invalid GC candidate state")
			}
		}
	} else if m.LastNs != 0 || len(m.Candidates) != 0 {
		return nil, "", errors.New("chunkstore: incomplete GC mark carries coverage")
	}
	return &m, token, nil
}

func (g *Collector) writeMark(ctx context.Context, m savedGCMark, expected string) (string, error) {
	token, err := storage.NewChunkIntentID(0)
	if err != nil {
		return "", err
	}
	body, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	if len(body) > 64<<20 {
		return "", errors.New("chunkstore: GC mark exceeds 64 MiB")
	}
	now := g.cfg.Now().Unix()
	e := &filerpb.Entry{Name: gcMarkName, Attributes: &filerpb.FuseAttributes{FileSize: uint64(len(body)), Crtime: now, Mtime: now, FileMode: 0600}, Content: body, Extended: map[string][]byte{extKind: []byte(gcMarkKind), gcToken: []byte(token)}}
	if expected == "" {
		err = g.cat.f.CreateEntry(ctx, g.cat.base, e, true)
	} else {
		err = g.cat.f.UpdateEntry(ctx, g.cat.base, e, map[string][]byte{extKind: []byte(gcMarkKind), gcToken: []byte(expected)})
	}
	if err != nil {
		return "", fmt.Errorf("chunkstore: GC watermark changed or unavailable: %w", err)
	}
	return token, nil
}
