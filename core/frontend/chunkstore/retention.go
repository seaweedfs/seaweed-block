package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/storage"
)

// Snapshot retention (claude05's rulings, 2026-11-15 22:53Z).
//
// A snapshot has a class, stored with it:
//   - "semantic": an automatic checkpoint (e.g. an agent's per-step
//     checkpoint). Governed by count, age and the log-spaced keeps.
//   - "pinned": an explicit keep. It is never expired by count or age,
//     only by its own ExpiresAtNs or an explicit delete. A snapshot with
//     ExpiresAtNs set is pinned whatever its class.
//   - "" (snapshots made before classes existed): treated as unpinned.
//
// The policy is stored on the VOLUME entry. Retain keeps
//
//	newest KeepNewest unpinned
//	+ every pinned snapshot not yet past its ExpiresAtNs
//	+ every snapshot younger than KeepYounger (a creation time in the
//	  future, from clock skew, counts as young)
//	+ the log-spaced keeps: the newest snapshot of each of the last
//	  KeepHourly hours, KeepDaily days and KeepWeekly weeks
//	+ every snapshot a fork still needs (named by a volume's fork record
//	  while that volume has no real published HEAD yet)
//
// and deletes every other snapshot through DeleteSnapshot (attempt-scoped
// tombstone rules: HEAD is never touched, the name is freed, the attempt
// path stays). A pinned snapshot past its ExpiresAtNs is treated as
// unpinned. GC reclaims the chunks.

const (
	SnapshotClassSemantic = "semantic"
	SnapshotClassPinned   = "pinned"

	CodeInvalidClass  = "invalid_class"
	CodeInvalidPolicy = "invalid_policy"

	DefaultKeepNewest  = 10
	DefaultKeepYounger = time.Hour

	extRetention    = "retention"
	extRetentionRev = "retention_rev"
)

func validSnapshotClass(class string) error {
	if red := true; red { // RED: no validation
		return nil
	}
	switch class {
	case "", SnapshotClassSemantic, SnapshotClassPinned:
		return nil
	}
	return catErr(CodeInvalidClass, false, nil, "snapshot class %q: want %q or %q", class, SnapshotClassSemantic, SnapshotClassPinned)
}

// Pinned reports whether the snapshot is an explicit keep.
func (s SnapshotInfo) Pinned() bool {
	return s.Class == SnapshotClassPinned || s.ExpiresAtNs > 0
}

// RetentionPolicy is a volume's snapshot retention policy.
type RetentionPolicy struct {
	KeepNewest    int   `json:"keepNewest"`
	KeepYoungerNs int64 `json:"keepYoungerNs"`
	KeepHourly    int   `json:"keepHourly"`
	KeepDaily     int   `json:"keepDaily"`
	KeepWeekly    int   `json:"keepWeekly"`
}

// DefaultRetentionPolicy is the policy of a volume that never set one.
func DefaultRetentionPolicy() RetentionPolicy {
	return RetentionPolicy{KeepNewest: DefaultKeepNewest, KeepYoungerNs: int64(DefaultKeepYounger)}
}

func (p RetentionPolicy) validate() error {
	if p.KeepNewest < 0 || p.KeepYoungerNs < 0 || p.KeepHourly < 0 || p.KeepDaily < 0 || p.KeepWeekly < 0 {
		return catErr(CodeInvalidPolicy, false, nil, "retention policy %+v: counts and durations must not be negative", p)
	}
	return nil
}

// ReadRetentionPolicy returns vol's policy; set is false (and the policy
// is DefaultRetentionPolicy) when none was ever set.
func (c *Catalog) ReadRetentionPolicy(ctx context.Context, vol string) (RetentionPolicy, bool, error) {
	e, _, err := c.volumeEntry(ctx, vol)
	if err != nil {
		return RetentionPolicy{}, false, err
	}
	return decodeRetention(e)
}

func decodeRetention(e *filerpb.Entry) (RetentionPolicy, bool, error) {
	raw := e.Extended[extRetention]
	if len(raw) == 0 {
		return DefaultRetentionPolicy(), false, nil
	}
	var p RetentionPolicy
	if err := json.Unmarshal(raw, &p); err != nil {
		return RetentionPolicy{}, false, catErr(CodeInvalidPolicy, false, err, "stored retention policy is unreadable")
	}
	return p, true, nil
}

// volumeEntry reads vol's VOLUME entry; a volume without one is
// not_chunk_volume.
func (c *Catalog) volumeEntry(ctx context.Context, vol string) (*filerpb.Entry, string, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return nil, "", err
	}
	e, err := c.f.LookupEntry(ctx, root, volumeName)
	if errors.Is(err, ErrNotFound) {
		return nil, "", catErr(CodeNotChunkVolume, false, nil, "volume %s has no VOLUME entry", vol)
	}
	if err != nil {
		return nil, "", remoteErr(err, "read VOLUME of %s", vol)
	}
	return e, root, nil
}

// SetRetentionPolicy stores p on vol's VOLUME entry. Only the policy's
// extended attributes change; the volume document is untouched. The update
// is conditional on the policy revision read, so of two concurrent setters
// one wins and the other gets a retryable remote error.
func (c *Catalog) SetRetentionPolicy(ctx context.Context, vol string, p RetentionPolicy) error {
	if red := true; red { // RED: stores nothing
		return nil
	}
	if err := p.validate(); err != nil {
		return err
	}
	e, root, err := c.volumeEntry(ctx, vol)
	if err != nil {
		return err
	}
	rev := string(e.Extended[extRetentionRev])
	n, _ := strconv.ParseUint(rev, 10, 64)
	body, err := json.Marshal(p)
	if err != nil {
		return err
	}
	next := proto.Clone(e).(*filerpb.Entry)
	if next.Extended == nil {
		next.Extended = map[string][]byte{}
	}
	next.Extended[extRetention] = body
	next.Extended[extRetentionRev] = []byte(strconv.FormatUint(n+1, 10))
	if err := c.f.UpdateEntry(ctx, root, next, map[string][]byte{extRetentionRev: []byte(rev)}); err != nil {
		return remoteErr(err, "set the retention policy of %s", vol)
	}
	return nil
}

// RetentionDecision is one snapshot's fate under a policy.
type RetentionDecision struct {
	Name      string   `json:"name"`
	CreatedNs int64    `json:"createdNs"`
	Class     string   `json:"class"`
	Pinned    bool     `json:"pinned"`
	Keep      bool     `json:"keep"`
	Reasons   []string `json:"reasons"` // why it is kept; "expired" when it goes
}

// The reasons a snapshot is kept.
const (
	keepNewest     = "newest"
	keepPinned     = "pinned"
	keepYoung      = "younger_than_policy"
	keepHourly     = "hourly"
	keepDaily      = "daily"
	keepWeekly     = "weekly"
	keepForkParent = "fork_parent"
	reasonExpired  = "expired"
)

// planRetention decides, from one listing, which snapshots policy keeps at
// now. needed names the snapshots a fork still needs. Unpinned snapshots
// are ranked newest first; equal creation times rank by name, so the plan
// is deterministic.
func planRetention(p RetentionPolicy, snaps []SnapshotInfo, needed map[string]bool, now time.Time) []RetentionDecision {
	if red := true; red { // RED: keeps everything
		out := make([]RetentionDecision, len(snaps))
		for i, s := range snaps {
			out[i] = RetentionDecision{Name: s.Name, Keep: true, Reasons: []string{"red"}}
		}
		return out
	}
	ordered := append([]SnapshotInfo(nil), snaps...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].CreatedNs != ordered[j].CreatedNs {
			return ordered[i].CreatedNs > ordered[j].CreatedNs
		}
		return ordered[i].Name < ordered[j].Name
	})
	nowNs := now.UnixNano()
	out := make([]RetentionDecision, len(ordered))
	unpinned := 0
	buckets := map[string]bool{}
	for i, s := range ordered {
		pinned := s.Pinned() && (s.ExpiresAtNs == 0 || nowNs < s.ExpiresAtNs)
		d := RetentionDecision{Name: s.Name, CreatedNs: s.CreatedNs, Class: s.Class, Pinned: pinned}
		if pinned {
			d.Reasons = append(d.Reasons, keepPinned)
		} else {
			if unpinned < p.KeepNewest {
				d.Reasons = append(d.Reasons, keepNewest)
			}
			unpinned++
		}
		age := time.Duration(nowNs - s.CreatedNs)
		if age < time.Duration(p.KeepYoungerNs) {
			d.Reasons = append(d.Reasons, keepYoung)
		}
		for _, lk := range []struct {
			reason string
			period time.Duration
			count  int
		}{
			{keepHourly, time.Hour, p.KeepHourly},
			{keepDaily, 24 * time.Hour, p.KeepDaily},
			{keepWeekly, 7 * 24 * time.Hour, p.KeepWeekly},
		} {
			if lk.count <= 0 || age < 0 {
				continue
			}
			ago := int64(age / lk.period) // 0 = the current period
			if ago >= int64(lk.count) {
				continue
			}
			key := fmt.Sprintf("%s/%d", lk.reason, s.CreatedNs/int64(lk.period))
			if !buckets[key] { // ordered newest first: the first seen is the bucket's newest
				buckets[key] = true
				d.Reasons = append(d.Reasons, lk.reason)
			}
		}
		if needed[s.Name] {
			d.Reasons = append(d.Reasons, keepForkParent)
		}
		d.Keep = len(d.Reasons) > 0
		if !d.Keep {
			d.Reasons = []string{reasonExpired}
		}
		out[i] = d
	}
	return out
}

// RetentionResult is the outcome of one Retain pass.
type RetentionResult struct {
	VolumeID  string              `json:"volumeID"`
	Policy    RetentionPolicy     `json:"policy"`
	PolicySet bool                `json:"policySet"`
	DryRun    bool                `json:"dryRun"`
	Decisions []RetentionDecision `json:"decisions"`
	Deleted   []string            `json:"deleted"` // names deleted (empty on a dry run)
	Kept      int                 `json:"kept"`
	Expired   int                 `json:"expired"`
}

// Retain applies vol's retention policy at the catalog clock's now. A dry
// run plans and writes nothing. Otherwise every expired snapshot is
// deleted through DeleteSnapshot; a snapshot that is already gone counts
// as deleted, and the first other failure stops the pass and is returned
// with what was deleted so far.
func (c *Catalog) Retain(ctx context.Context, vol string, dryRun bool) (RetentionResult, error) {
	e, _, err := c.volumeEntry(ctx, vol)
	if err != nil {
		return RetentionResult{}, err
	}
	p, set, err := decodeRetention(e)
	if err != nil {
		return RetentionResult{}, err
	}
	snaps, err := c.allSnapshots(ctx, vol)
	if err != nil {
		return RetentionResult{}, err
	}
	needed, err := c.forkParentsNeeded(ctx, vol)
	if err != nil {
		return RetentionResult{}, err
	}
	res := RetentionResult{VolumeID: vol, Policy: p, PolicySet: set, DryRun: dryRun, Deleted: []string{}}
	res.Decisions = planRetention(p, snaps, needed, c.now())
	for _, d := range res.Decisions {
		if d.Keep {
			res.Kept++
			continue
		}
		res.Expired++
		if dryRun {
			continue
		}
		if _, err := c.DeleteSnapshot(ctx, vol, d.Name); err != nil && CodeOf(err) != CodeSnapshotNotFound {
			return res, err
		}
		res.Deleted = append(res.Deleted, d.Name)
	}
	return res, nil
}

// allSnapshots lists every published, not-deleting snapshot of vol.
func (c *Catalog) allSnapshots(ctx context.Context, vol string) ([]SnapshotInfo, error) {
	var out []SnapshotInfo
	after := ""
	for {
		page, next, err := c.ListSnapshots(ctx, vol, ListOptions{StartAfter: after, Limit: maxListLimit})
		if err != nil {
			return nil, err
		}
		out = append(out, page...)
		if next == "" {
			return out, nil
		}
		if next <= after {
			return nil, errors.New("chunkstore: non-advancing snapshot listing")
		}
		after = next
	}
}

// forkParentsNeeded names the snapshots of vol a fork still needs: a
// volume whose fork record names vol and the snapshot, and which has no
// real published HEAD yet (absent or aborted). A published fork HEAD pins
// its chunks for GC itself, so it no longer needs the snapshot.
func (c *Catalog) forkParentsNeeded(ctx context.Context, vol string) (map[string]bool, error) {
	if red := true; red { // RED: no fork needs a parent
		return map[string]bool{}, nil
	}
	vols, err := c.ListVolumes(ctx)
	if err != nil {
		return nil, err
	}
	needed := map[string]bool{}
	for _, other := range vols {
		if other == vol {
			continue
		}
		v, ok, err := c.ReadVolume(ctx, other)
		if err != nil {
			return nil, err
		}
		if !ok || v.Fork == nil || v.Fork.Source != vol {
			continue
		}
		h, has, err := c.ReadHead(ctx, other)
		if err != nil {
			return nil, err
		}
		if !has || h.State == storage.ChunkHeadAborted {
			needed[v.Fork.Snapshot] = true
		}
	}
	return needed, nil
}
