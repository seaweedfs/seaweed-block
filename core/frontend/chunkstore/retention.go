package chunkstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"sort"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// Snapshot retention.
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
//	+ the log-spaced keeps (below)
//
// and deletes every other snapshot. A pinned snapshot past its ExpiresAtNs
// is treated as unpinned. GC reclaims the chunks.
//
// Log-spaced keeps use rolling windows anchored at now: window k of a
// period P holds the snapshots whose age is in [k*P, (k+1)*P). For
// KeepHourly = H, the newest snapshot of each of windows 0..H-1 is kept
// (likewise daily and weekly). So at 10:30 with KeepHourly = 1 only the
// newest snapshot of 09:30..10:30 is kept, not one per clock hour.
//
// Deletion is bound to the exact attempt planned (review of #142): the
// attempt's own entry is marked deleting conditionally, and the name
// record is removed only while it still names that attempt, so a
// same-name replacement made after planning is never touched.
//
// Forks and restores do not hold their parent (INV-SNAP-DELETE-ANY-ORDER-001).
// A snapshot is deleted by retention or DeleteSnapshot whatever forks read
// it: a fork reads the parent after taking its deadline start and publishes
// its HEAD only before readAt + deadline, and GC keeps a deleted attempt
// rooted for its grace, which exceeds deadline + clock skew + CAS apply
// bound (NewCollector). A fork that reads after the delete mark gets
// snapshot_deleting or snapshot_not_found, also on a retry. A claims
// xattr left by an older version is ignored.
//
// A pass stopped after a mark (e.g. a lost response) leaves the attempt
// deleting with its name still held; the next pass (and its dry run) lists
// it and frees the name.

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

// ValidateSnapshotClass refuses a class other than "", "semantic" and
// "pinned" (invalid_class).
func ValidateSnapshotClass(class string) error {
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
	Attempt   string   `json:"attempt"`
	CreatedNs int64    `json:"createdNs"`
	Class     string   `json:"class"`
	Pinned    bool     `json:"pinned"`
	Keep      bool     `json:"keep"`
	Reasons   []string `json:"reasons"` // why it is kept; "expired" when it goes
}

// The reasons a snapshot is kept.
const (
	keepNewest    = "newest"
	keepPinned    = "pinned"
	keepYoung     = "younger_than_policy"
	keepHourly    = "hourly"
	keepDaily     = "daily"
	keepWeekly    = "weekly"
	reasonExpired = "expired"
)

// planRetention decides, from one listing, which snapshots policy keeps at
// now. Unpinned snapshots
// are ranked newest first; equal creation times rank by name, so the plan
// is deterministic.
func planRetention(p RetentionPolicy, snaps []SnapshotInfo, now time.Time) []RetentionDecision {
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
	windows := map[string]bool{}
	for i, s := range ordered {
		pinned := s.Pinned() && (s.ExpiresAtNs == 0 || nowNs < s.ExpiresAtNs)
		d := RetentionDecision{Name: s.Name, Attempt: s.attempt, CreatedNs: s.CreatedNs, Class: s.Class, Pinned: pinned}
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
			k := int64(age / lk.period) // the rolling window holding this age
			if k >= int64(lk.count) {
				continue
			}
			key := fmt.Sprintf("%s/%d", lk.reason, k)
			if !windows[key] { // ordered newest first: the first seen is the window's newest
				windows[key] = true
				d.Reasons = append(d.Reasons, lk.reason)
			}
		}
		d.Keep = len(d.Reasons) > 0
		if !d.Keep {
			d.Reasons = []string{reasonExpired}
		}
		out[i] = d
	}
	return out
}

// RetentionFailure is the deletion that stopped a Retain pass.
type RetentionFailure struct {
	Name    string `json:"name"`
	Attempt string `json:"attempt"`
	Code    string `json:"code"`
	Message string `json:"message"`
	// Uncertain is set when the failing call was a mutation whose outcome
	// is unknown (e.g. the response was lost): the snapshot may or may
	// not be marked deleting. It is false when the pass failed before
	// changing anything for this snapshot.
	Uncertain bool `json:"uncertain"`
}

// RetentionResult is the outcome of one Retain pass.
type RetentionResult struct {
	VolumeID  string              `json:"volumeID"`
	Policy    RetentionPolicy     `json:"policy"`
	PolicySet bool                `json:"policySet"`
	DryRun    bool                `json:"dryRun"`
	Decisions []RetentionDecision `json:"decisions"`
	Deleted   []string            `json:"deleted"`  // confirmed deleted (empty on a dry run)
	Replaced  []string            `json:"replaced"` // planned, but the name now holds another attempt: left alone
	Failed    *RetentionFailure   `json:"failed,omitempty"`
	// NotAttempted lists the expired snapshots not reached because the
	// pass stopped at Failed.
	NotAttempted []string `json:"notAttempted"`
	Kept         int      `json:"kept"`
	Expired      int      `json:"expired"`
}

// Retain applies vol's retention policy at the catalog clock's now. A dry
// run plans and writes nothing. Otherwise each expired snapshot's planned
// attempt is deleted (deleteRetainedAttempt). The first failure stops the
// pass; the result then still reports every confirmed deletion, the
// failure (with whether its outcome is uncertain), and what was not
// attempted, together with the error.
func (c *Catalog) Retain(ctx context.Context, vol string, dryRun bool) (RetentionResult, error) {
	e, _, err := c.volumeEntry(ctx, vol)
	if err != nil {
		return RetentionResult{}, err
	}
	p, set, err := decodeRetention(e)
	if err != nil {
		return RetentionResult{}, err
	}
	listed, err := c.listSnapshots(ctx, vol, true)
	if err != nil {
		return RetentionResult{}, err
	}
	var snaps, unfinished []SnapshotInfo
	for _, s := range listed {
		if s.State == SnapDeleting {
			unfinished = append(unfinished, s) // a pass stopped after its mark
		} else {
			snaps = append(snaps, s)
		}
	}
	res := RetentionResult{VolumeID: vol, Policy: p, PolicySet: set, DryRun: dryRun,
		Deleted: []string{}, Replaced: []string{}, NotAttempted: []string{}}
	res.Decisions = planRetention(p, snaps, c.now())
	var expired []RetentionDecision
	for _, d := range res.Decisions {
		if d.Keep {
			res.Kept++
			continue
		}
		res.Expired++
		expired = append(expired, d)
	}
	for _, s := range unfinished {
		d := RetentionDecision{Name: s.Name, Attempt: s.attempt, CreatedNs: s.CreatedNs, Class: s.Class, Pinned: s.Pinned(), Reasons: []string{SnapDeleting}}
		res.Decisions = append(res.Decisions, d)
		res.Expired++
		expired = append(expired, d)
	}
	if dryRun {
		return res, nil
	}
	c.hook(hookPlanned, "")
	for i, d := range expired {
		outcome, uncertain, err := c.deleteRetainedAttempt(ctx, vol, d.Name, d.Attempt)
		if err != nil {
			res.Failed = &RetentionFailure{Name: d.Name, Attempt: d.Attempt, Code: codeOrRemote(err), Message: err.Error(), Uncertain: uncertain}
			for _, rest := range expired[i+1:] {
				res.NotAttempted = append(res.NotAttempted, rest.Name)
			}
			return res, err
		}
		switch outcome {
		case retainDeleted:
			res.Deleted = append(res.Deleted, d.Name)
		case retainReplaced:
			res.Replaced = append(res.Replaced, d.Name)
		}
	}
	return res, nil
}

func codeOrRemote(err error) string {
	if code := CodeOf(err); code != "" {
		return code
	}
	return CodeRemoteUnavailable
}

type retainOutcome int

const (
	retainDeleted  retainOutcome = iota // marked deleting (or already gone) and the name freed
	retainReplaced                      // the planned attempt is gone; another attempt holds the name
)

// deleteRetainedAttempt deletes the planned attempt of vol/name and only
// that attempt. uncertain reports, with an error, that the failing call
// was a mutation whose outcome is unknown.
func (c *Catalog) deleteRetainedAttempt(ctx context.Context, vol, name, attempt string) (retainOutcome, bool, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return 0, false, err
	}
	if _, uncertain, err := c.markDeleting(ctx, vol, name, attempt); err != nil {
		return 0, uncertain, err
	}
	return c.freeName(ctx, root, name, attempt)
}

type markResult int

const (
	markDone    markResult = iota // this call marked the attempt deleting
	markGone                      // the attempt's entry is gone
	markSettled                   // already deleting or aborted
)

// markDeleting marks attempt of vol/name deleting, the one path for
// retention and explicit delete: the mark is conditioned on {state active,
// gen}; a lost condition re-reads and decides again. No fork claim is read
// (INV-SNAP-DELETE-ANY-ORDER-001). uncertain reports, with an error, a
// mutation whose outcome is unknown.
func (c *Catalog) markDeleting(ctx context.Context, vol, name, attempt string) (markResult, bool, error) {
	root, err := c.volRoot(vol)
	if err != nil {
		return 0, false, err
	}
	dir := path.Join(root, snapsDir, name)
	for try := 0; try < 5; try++ {
		e, err := c.f.LookupEntry(ctx, dir, attempt)
		if errors.Is(err, ErrNotFound) {
			return markGone, false, nil
		}
		if err != nil {
			return 0, false, remoteErr(err, "read snapshot %s/%s attempt %s", vol, name, attempt)
		}
		if string(e.Extended[extSnapState]) != SnapActive {
			return markSettled, false, nil
		}
		c.hook(hookChecked, name)
		next := proto.Clone(e).(*filerpb.Entry)
		next.Extended[extSnapState] = []byte(SnapDeleting)
		next.Extended[extSnapDeletedNs] = []byte(strconv.FormatInt(c.now().UnixNano(), 10))
		err = c.f.UpdateEntry(ctx, dir, next, map[string][]byte{
			extSnapState: []byte(SnapActive),
			extSnapGen:   e.Extended[extSnapGen],
		})
		if errors.Is(err, ErrPrecondition) || errors.Is(err, ErrNotFound) {
			continue // a delete or a replacement landed: decide again
		}
		if err != nil {
			return 0, true, remoteErr(err, "mark snapshot %s/%s attempt %s deleting", vol, name, attempt)
		}
		c.hook(hookMarked, name)
		return markDone, false, nil
	}
	return 0, false, remoteErr(nil, "snapshot %s/%s attempt %s kept changing", vol, name, attempt)
}

// Test seams: hook stages in a Retain pass.
const (
	hookPlanned = "planned" // after planning, before any deletion
	hookChecked = "checked" // an attempt was read active, before its mark
	hookMarked  = "marked"  // an attempt is marked deleting, before its name is freed
)

func (c *Catalog) hook(stage, name string) {
	if c.retainHook != nil {
		c.retainHook(stage, name)
	}
}

// freeName removes the name record of name only while it names attempt.
func (c *Catalog) freeName(ctx context.Context, root, name, attempt string) (retainOutcome, bool, error) {
	err := c.f.DeleteEntry(ctx, path.Join(root, snapNamesDir), name, map[string][]byte{extAttempt: []byte(attempt)})
	switch {
	case err == nil || errors.Is(err, ErrNotFound):
		return retainDeleted, false, nil
	case errors.Is(err, ErrPrecondition):
		return retainReplaced, false, nil
	}
	return 0, true, remoteErr(err, "remove the name record %s", name)
}

// allSnapshots lists every published, not-deleting snapshot of vol, with
// the attempt each name resolved to.
func (c *Catalog) allSnapshots(ctx context.Context, vol string) ([]SnapshotInfo, error) {
	return c.listSnapshots(ctx, vol, false)
}

func (c *Catalog) listSnapshots(ctx context.Context, vol string, deleting bool) ([]SnapshotInfo, error) {
	var out []SnapshotInfo
	after := ""
	for {
		page, next, err := c.ListSnapshots(ctx, vol, ListOptions{StartAfter: after, Limit: maxListLimit, IncludeDeleting: deleting})
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
