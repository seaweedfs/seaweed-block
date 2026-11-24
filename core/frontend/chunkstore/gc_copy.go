package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// SweepCopies settles expired copy attempts using permanent publication
// target claims; a deadline only admits a claim, never releases protection.
// It shares the collector pass mutex but acquires no storage locks.
func (g *Collector) SweepCopies(ctx context.Context) (settled int, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.cfg.Now()
	vols, err := g.cat.ListVolumes(ctx)
	if err != nil {
		return 0, err
	}
	type copyAttempt struct {
		vol    string
		remote *ChunkRemote
		in     storage.ChunkIntent
	}
	var pending []copyAttempt
	for _, vol := range vols {
		if g.scope != "" && g.scope != vol {
			continue
		}
		r, e := NewChunkRemote(g.cat.f, gcBlobReader{g.blobs}, path.Join(g.cat.base, vol))
		if e != nil {
			return 0, e
		}
		intents, e := r.ListIntents(ctx)
		if e != nil {
			return 0, e
		}
		for _, in := range intents {
			if !strings.HasPrefix(in.Holder, "snapshot:") && !strings.HasPrefix(in.Holder, "fork:") {
				continue
			}
			if in.CreatedNs <= 0 {
				return 0, errors.New("chunkstore: invalid copy intent creation time")
			}
			// A future creation stamp retains protection, without preventing
			// unrelated copy attempts from settling. Deadline alone never
			// releases roots: the permanent target claim below is still required.
			if in.CreatedNs > now.UnixNano() || in.DeadlineNs <= 0 || now.UnixNano() <= in.DeadlineNs {
				continue
			}
			pending = append(pending, copyAttempt{vol, r, in})
		}
	}
	for _, a := range pending {
		if err = ctx.Err(); err != nil {
			return settled, err
		}
		if strings.HasPrefix(a.in.Holder, "snapshot:") {
			parts := strings.Split(strings.TrimPrefix(a.in.Holder, "snapshot:"), "/")
			if len(parts) != 3 || parts[0] != a.vol || CheckSnapshotName(parts[1]) != nil || !storage.ValidChunkIntentID(parts[2]) {
				return settled, fmt.Errorf("chunkstore: malformed snapshot intent %s", a.in.ID)
			}
			// Claim the final attempt, never the reusable name record.
			if _, err = g.cat.AbortSnapshot(ctx, a.vol, parts[1], parts[2]); err != nil {
				return settled, err
			}
			dir := path.Join(g.cat.base, a.vol, snapsDir, parts[1])
			e, body, readErr := lookupRecord(ctx, g.cat.f, gcBlobReader{g.blobs}, dir, parts[2])
			if readErr != nil {
				return settled, readErr
			}
			info, _, readErr := decodeSnap(path.Join(dir, parts[2]), e, body)
			if readErr != nil {
				return settled, readErr
			}
			if info.VolumeID != a.vol || info.Name != parts[1] || (info.State != SnapAborted && string(e.Extended[extAttempt]) != parts[2]) {
				return settled, errors.New("chunkstore: copy claim identity mismatch")
			}
		} else {
			parts := strings.SplitN(strings.TrimPrefix(a.in.Holder, "fork:"), "/", 2)
			if len(parts) != 2 || parts[0] != a.vol || parts[1] == "" {
				return settled, fmt.Errorf("chunkstore: malformed fork intent %s", a.in.ID)
			}
			v, ok, readErr := g.cat.ReadVolume(ctx, a.vol)
			if readErr != nil {
				return settled, readErr
			}
			if !ok || !isForked(v.Origin) || v.Fork == nil || v.Fork.RequestID != parts[1] {
				return settled, errors.New("chunkstore: fork intent has no matching target VOLUME")
			}
			if _, err = g.cat.AbortFork(ctx, a.vol); err != nil {
				return settled, err
			}
			h, ok, readErr := g.cat.ReadHead(ctx, a.vol)
			if readErr != nil {
				return settled, readErr
			}
			if !ok || h.Gen == 0 {
				return settled, errors.New("chunkstore: fork claim did not settle the publishing HEAD")
			}
		}
		// Both a valid published immutable entry and a valid permanent aborted
		// entry settle this attempt. Unknown claim/read never drops its intent.
		if err = a.remote.DeleteIntent(ctx, a.in.ID); err != nil {
			return settled, err
		}
		settled++
	}
	return settled, nil
}

func (g *Collector) gcEntries(ctx context.Context, dir string) ([]*filerpb.Entry, error) {
	var out []*filerpb.Entry
	after := ""
	for {
		page, err := g.cat.f.ListEntries(ctx, dir, "", after, listPage)
		if err != nil {
			return nil, err
		}
		for _, e := range page {
			if e.GetName() <= after || path.Base(e.GetName()) != e.GetName() {
				return nil, errors.New("chunkstore: invalid/non-advancing GC directory listing")
			}
			after = e.Name
			out = append(out, e)
		}
		if len(page) < listPage {
			return out, nil
		}
	}
}

// snapshotAttemptHeads includes unnamed attempts left behind by DeleteSnapshot.
// Attempt entries are permanent. A deleting attempt stops rooting its keys
// only after its deleting timestamp ages past grace; ordinary two marks
// still apply afterward. An aborted attempt roots no keys.
type gcSnapshotHead struct {
	Head storage.ChunkHead
	Name string
}

func (g *Collector) snapshotAttemptHeads(ctx context.Context, vol string, now time.Time) ([]gcSnapshotHead, error) {
	root := path.Join(g.cat.base, vol, snapsDir)
	dirs, err := g.gcEntries(ctx, root)
	if err != nil {
		return nil, err
	}
	var heads []gcSnapshotHead
	for _, dir := range dirs {
		if !dir.GetIsDirectory() || CheckSnapshotName(dir.Name) != nil {
			return nil, errors.New("chunkstore: invalid snapshot attempt directory")
		}
		entries, err := g.gcEntries(ctx, path.Join(root, dir.Name))
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !storage.ValidChunkIntentID(e.Name) {
				return nil, errors.New("chunkstore: invalid snapshot attempt identity")
			}
			e, body, err := readRecord(ctx, g.cat.f, gcBlobReader{g.blobs}, path.Join(root, dir.Name), e)
			if err != nil {
				return nil, err
			}
			info, _, err := decodeSnap(path.Join(root, dir.Name, e.Name), e, body)
			if err != nil {
				return nil, err
			}
			if info.VolumeID != vol || info.Name != dir.Name || (info.State != SnapAborted && string(e.Extended[extAttempt]) != e.Name) {
				return nil, errors.New("chunkstore: snapshot attempt names another volume/name")
			}
			if info.State == SnapAborted {
				continue
			}
			if info.State == SnapDeleting {
				ns, err := strconv.ParseInt(string(e.Extended[extSnapDeletedNs]), 10, 64)
				if err != nil || ns <= 0 {
					return nil, errors.New("chunkstore: invalid deleted snapshot timestamp")
				}
				if g.oldEnough(now, time.Unix(0, ns), g.cfg.Grace, 0) {
					continue
				}
			}
			heads = append(heads, gcSnapshotHead{info.Head(), path.Join(dir.Name, e.Name)})
		}
	}
	return heads, nil
}
