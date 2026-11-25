package chunkstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/retain"
)

// retentionName is the per-volume RETENTION entry beside VOLUME: the
// operator's retention policy, changed by one conditional update, read
// only by retain. Absent is the default policy.
const retentionName = "RETENTION"

// ReadRetention returns the volume's policy and the entry's generation (0
// and the default policy when it was never set).
func (s *LayoutStore) ReadRetention(ctx context.Context) (retain.Policy, uint64, error) {
	e, err := s.f.LookupEntry(ctx, s.root, retentionName)
	if errors.Is(err, ErrNotFound) {
		return retain.DefaultPolicy(), 0, nil
	}
	if err != nil {
		return retain.Policy{}, 0, err
	}
	gen, err := strconv.ParseUint(string(e.Extended[layoutGen]), 10, 64)
	if err != nil || gen == 0 || string(e.Extended[layoutVolume]) != s.id.String() {
		return retain.Policy{}, 0, fmt.Errorf("%w: invalid RETENTION entry", layout.ErrCorrupt)
	}
	var p retain.Policy
	dec := json.NewDecoder(bytes.NewReader(e.Content))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&p); err != nil {
		return retain.Policy{}, 0, fmt.Errorf("%w: RETENTION: %v", layout.ErrCorrupt, err)
	}
	return p, gen, p.Validate()
}

// WriteRetention replaces the policy if the entry is still at
// expectedGen (0: not yet set); layout.ErrPrecondition otherwise.
func (s *LayoutStore) WriteRetention(ctx context.Context, expectedGen uint64, p retain.Policy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	e := layoutEntry(retentionName, raw)
	e.Extended = map[string][]byte{layoutKind: []byte("retention"), layoutGen: []byte(strconv.FormatUint(expectedGen+1, 10)), layoutVolume: []byte(s.id.String())}
	if expectedGen == 0 {
		err = s.f.CreateEntry(ctx, s.root, e, true)
		if errors.Is(err, ErrExists) {
			return layout.ErrPrecondition
		}
		return err
	}
	err = s.f.UpdateEntry(ctx, s.root, e, map[string][]byte{layoutGen: []byte(strconv.FormatUint(expectedGen, 10)), layoutVolume: []byte(s.id.String())})
	if errors.Is(err, ErrPrecondition) || errors.Is(err, ErrNotFound) {
		return layout.ErrPrecondition
	}
	return err
}

// SetRetention stores the volume's policy (replacing whatever is there,
// conditional on the generation it read).
func (c *BCatalog) SetRetention(ctx context.Context, v VolumeRef, p retain.Policy) error {
	s, err := c.Store(v)
	if err != nil {
		return err
	}
	if _, found, err := s.ReadHead(ctx); err != nil {
		return err
	} else if !found {
		return layout.ErrHeadAbsent
	}
	_, gen, err := s.ReadRetention(ctx)
	if err != nil && !errors.Is(err, retain.ErrInvalidPolicy) {
		return err
	}
	return s.WriteRetention(ctx, gen, p)
}

// ReadRetention returns the volume's policy (the default when never set).
func (c *BCatalog) ReadRetention(ctx context.Context, v VolumeRef) (retain.Policy, error) {
	s, err := c.Store(v)
	if err != nil {
		return retain.Policy{}, err
	}
	p, _, err := s.ReadRetention(ctx)
	return p, err
}

// RetainResult is a retention pass: every decision, and what it deleted.
type RetainResult struct {
	Policy    retain.Policy     `json:"policy"`
	Decisions []retain.Decision `json:"decisions"`
	Deleted   []string          `json:"deleted"`
	// Refused are names the plan would delete that the snapshot delete
	// refused: pinned on HEAD, or a restore target depends on them.
	Refused []string `json:"refused,omitempty"`
	DryRun  bool     `json:"dryRun,omitempty"`
}

// Retain applies the volume's policy at now: it plans over HEAD's
// snapshots and deletes each expired one by the automatic snapshot delete,
// which refuses a pinned snapshot or one with dependents. The policy is
// advisory: a change made during a pass applies from the next one.
func (c *BCatalog) Retain(ctx context.Context, v VolumeRef, now time.Time, dryRun bool) (RetainResult, error) {
	s, err := c.Store(v)
	if err != nil {
		return RetainResult{}, err
	}
	h, found, err := s.ReadHead(ctx)
	if err != nil {
		return RetainResult{}, err
	}
	if !found {
		return RetainResult{}, layout.ErrHeadAbsent
	}
	p, _, err := s.ReadRetention(ctx)
	if err != nil {
		return RetainResult{}, err
	}
	snaps := make([]retain.Snapshot, 0, len(h.Snapshots))
	for n, sn := range h.Snapshots {
		snaps = append(snaps, retain.Snapshot{Name: n, CreatedNs: sn.Meta.CreatedNs, Class: sn.Meta.Class, ExpiresNs: sn.Meta.ExpiresNs, Labels: sn.Meta.Labels})
	}
	res := RetainResult{Policy: p, Decisions: retain.Plan(p, snaps, now), Deleted: []string{}, DryRun: dryRun}
	for _, d := range res.Decisions {
		if d.Keep {
			continue
		}
		if dryRun {
			res.Deleted = append(res.Deleted, d.Name)
			continue
		}
		if _, err := layout.RunRetireSnapshot(ctx, s, d.Name, now.UnixNano()); err != nil {
			if errors.Is(err, layout.ErrPinned) || errors.Is(err, layout.ErrHasDependents) {
				res.Refused = append(res.Refused, d.Name)
				continue
			}
			return res, err
		}
		res.Deleted = append(res.Deleted, d.Name)
	}
	return res, nil
}
