package layout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"unicode/utf8"
)

// HEAD is the one mutable entry of a volume: who holds it (epoch, holder),
// the open transaction (activity), the current image and the named
// snapshots. Every change to it is one conditional update on its
// generation; nothing else is atomic.

// Activity kinds: no transaction, a snapshot session, a cleaning session.
const (
	ActIdle  = "idle"
	ActSnap  = "snap"
	ActClean = "clean"
)

// MaxSnapshots bounds the named snapshots one HEAD holds.
const MaxSnapshots = 10

// Activity is HEAD's open transaction. Session names it; a session id is
// never reused.
type Activity struct {
	Kind    string `json:"kind"`
	Session string `json:"session,omitempty"`
}

// Dependent is a restore target that still names a snapshot's chunks: the
// target volume's name and id, and the root it was made from. A volume
// created again under the target's name, or a snapshot name reused for
// another root, matches no dependent.
type Dependent struct {
	Target   string
	TargetID VolumeID
	Root     RootName
}

// Snapshot is one named snapshot: its root and the restore targets that
// still name its chunks, ordered by target id.
type Snapshot struct {
	Root       RootName
	Meta       SnapshotMeta
	Dependents []Dependent
}

// The retention classes of a snapshot.
const (
	ClassSemantic  = "semantic"
	ClassPinned    = "pinned"
	ClassScheduled = "scheduled"
)

// The bounds that keep HEAD inline at its snapshot cap: a snapshot's
// name, its labels, and its dependents.
const (
	MaxSnapshotNameBytes = 64
	MaxSnapshotLabels    = 4
	MaxLabelKeyBytes     = 24
	MaxLabelValueBytes   = 32
	MaxDependents        = 4
	// MaxDependentsPerHead bounds the dependents of all snapshots together.
	MaxDependentsPerHead = 16
	// MaxHeadBytes is the inline limit: Encode refuses a larger HEAD, so a
	// bound missed elsewhere fails the write, never stores it.
	MaxHeadBytes = 64 << 10
)

// ErrHeadTooLarge: an encoded HEAD above MaxHeadBytes.
var ErrHeadTooLarge = errors.New("layout: HEAD above its inline limit")

// SnapshotMeta is what retention decides from: written once by the commit
// that creates the name. The times are wall clock and decide policy only;
// reachability, dependents and removal never read them.
type SnapshotMeta struct {
	CreatedNs int64
	Class     string // "", ClassSemantic, ClassPinned or ClassScheduled
	ExpiresNs int64  // pinned until then, when set
	Labels    map[string]string
}

// PinnedAt reports whether the snapshot is an explicit keep at nowNs: class
// pinned, or an expiry not yet passed. An automatic delete refuses it.
func (m SnapshotMeta) PinnedAt(nowNs int64) bool {
	return m.Class == ClassPinned || m.ExpiresNs > 0 && nowNs < m.ExpiresNs
}

func (m SnapshotMeta) check(name string) error {
	switch m.Class {
	case "", ClassSemantic, ClassPinned, ClassScheduled:
	default:
		return fmt.Errorf("%w: snapshot %q class %q", ErrMalformed, name, m.Class)
	}
	if m.CreatedNs < 0 || m.ExpiresNs < 0 {
		return fmt.Errorf("%w: snapshot %q has a negative time", ErrMalformed, name)
	}
	if len(m.Labels) > MaxSnapshotLabels {
		return fmt.Errorf("%w: snapshot %q has %d labels, at most %d", ErrMalformed, name, len(m.Labels), MaxSnapshotLabels)
	}
	for k, v := range m.Labels {
		if k == "" || len(k) > MaxLabelKeyBytes || len(v) > MaxLabelValueBytes {
			return fmt.Errorf("%w: snapshot %q label %q: keys 1-%d bytes, values at most %d", ErrMalformed, name, k, MaxLabelKeyBytes, MaxLabelValueBytes)
		}
		if err := validName("label", k); err != nil {
			return err
		}
		if err := validName("label", v); err != nil {
			return err
		}
	}
	return nil
}

// Head is the decoded HEAD. Gen is the generation the store holds it at;
// it is not part of the encoding. VolumeID is the volume's id (its VOLUME
// record's); ClaimID is the id the current holder's claim wrote, which
// with the epoch identifies the holder's local data (the holder's name
// repeats across restarts and same-host moves).
type Head struct {
	Gen       uint64
	VolumeID  VolumeID
	Epoch     uint64
	Holder    string
	ClaimID   string
	Activity  Activity
	Current   *RootName // nil: no image committed yet
	Position  uint64
	Snapshots map[string]Snapshot
	State     string // StateLive, StateDetached, StateDeleting or StateDeleted
}

type headDoc struct {
	VolumeID  string             `json:"volume_id"`
	Epoch     uint64             `json:"epoch"`
	Holder    string             `json:"holder"`
	ClaimID   string             `json:"claim_id,omitempty"`
	Activity  Activity           `json:"activity"`
	Current   string             `json:"current,omitempty"`
	Position  uint64             `json:"position"`
	Snapshots map[string]snapDoc `json:"snapshots,omitempty"`
	State     string             `json:"state,omitempty"`
}

type snapDoc struct {
	Root       string            `json:"root"`
	CreatedNs  int64             `json:"created_ns,omitempty"`
	Class      string            `json:"class,omitempty"`
	ExpiresNs  int64             `json:"expires_ns,omitempty"`
	Labels     map[string]string `json:"labels,omitempty"`
	Dependents []depDoc          `json:"dependents,omitempty"`
}

type depDoc struct {
	Target   string `json:"target"`
	TargetID string `json:"target_id"`
	Root     string `json:"root"`
}

// validName refuses a string the JSON encoding would not keep byte for
// byte: invalid UTF-8 is written as an escaped U+FFFD, which DecodeHead's
// canonical check then refuses.
func validName(field, v string) error {
	if !utf8.ValidString(v) {
		return fmt.Errorf("%w: %s %q is not valid UTF-8", ErrMalformed, field, v)
	}
	return nil
}

func (h Head) check() error {
	if h.VolumeID.IsZero() {
		return fmt.Errorf("%w: HEAD without a volume id", ErrMalformed)
	}
	switch h.State {
	case StateLive, StateDetached, StateDeleting, StateDeleted:
	default:
		return fmt.Errorf("%w: HEAD state %q", ErrMalformed, h.State)
	}
	for field, v := range map[string]string{"holder": h.Holder, "session": h.Activity.Session, "claim id": h.ClaimID} {
		if err := validName(field, v); err != nil {
			return err
		}
	}
	switch h.Activity.Kind {
	case ActIdle:
		if h.Activity.Session != "" {
			return fmt.Errorf("%w: idle activity names session %q", ErrMalformed, h.Activity.Session)
		}
	case ActSnap, ActClean:
		if h.Activity.Session == "" {
			return fmt.Errorf("%w: %s activity without a session", ErrMalformed, h.Activity.Kind)
		}
	default:
		return fmt.Errorf("%w: activity %q", ErrMalformed, h.Activity.Kind)
	}
	if h.State == StateDetached && (h.Activity.Kind != ActIdle || h.Current == nil) {
		return fmt.Errorf("%w: detached HEAD needs an idle, committed image", ErrMalformed)
	}
	// A HEAD no one has claimed yet (a new volume or restore target, or a
	// tombstone) has epoch 0, no holder, no claim id and no activity; a
	// claimed HEAD has an epoch, a holder and the claim id of its claim.
	if h.Holder == "" {
		if h.Epoch != 0 || h.ClaimID != "" || h.Activity.Kind != ActIdle {
			return fmt.Errorf("%w: HEAD without a holder has epoch %d, claim id %q, activity %s", ErrMalformed, h.Epoch, h.ClaimID, h.Activity.Kind)
		}
	} else {
		if h.Epoch == 0 {
			return fmt.Errorf("%w: HEAD holder %q without an epoch", ErrMalformed, h.Holder)
		}
		if h.ClaimID == "" {
			return fmt.Errorf("%w: HEAD holder %q without a claim id", ErrMalformed, h.Holder)
		}
	}
	if len(h.Snapshots) > MaxSnapshots {
		return fmt.Errorf("%w: %d snapshots, at most %d", ErrMalformed, len(h.Snapshots), MaxSnapshots)
	}
	if n := h.dependents(); n > MaxDependentsPerHead {
		return fmt.Errorf("%w: %d dependents in HEAD, at most %d", ErrMalformed, n, MaxDependentsPerHead)
	}
	for n, s := range h.Snapshots {
		if n == "" {
			return fmt.Errorf("%w: unnamed snapshot", ErrMalformed)
		}
		if err := validName("snapshot", n); err != nil {
			return err
		}
		if len(n) > MaxSnapshotNameBytes {
			return fmt.Errorf("%w: snapshot name %q is %d bytes, at most %d", ErrMalformed, n, len(n), MaxSnapshotNameBytes)
		}
		if len(s.Dependents) > MaxDependents {
			return fmt.Errorf("%w: snapshot %q has %d dependents, at most %d", ErrMalformed, n, len(s.Dependents), MaxDependents)
		}
		if err := s.Meta.check(n); err != nil {
			return err
		}
		for i, d := range s.Dependents {
			if err := validName("dependent", d.Target); err != nil {
				return err
			}
			if err := checkOwner(d.Target); err != nil {
				return err
			}
			if d.TargetID.IsZero() {
				return fmt.Errorf("%w: snapshot %q dependent %q without its volume id", ErrMalformed, n, d.Target)
			}
			if d.Root != s.Root {
				return fmt.Errorf("%w: snapshot %q dependent %q records root %s, not the snapshot's %s", ErrMalformed, n, d.Target, d.Root, s.Root)
			}
			if i > 0 && bytes.Compare(s.Dependents[i-1].TargetID[:], d.TargetID[:]) >= 0 {
				return fmt.Errorf("%w: snapshot %q dependents not ordered and unique by target id", ErrMalformed, n)
			}
		}
	}
	return nil
}

// Encode returns HEAD's canonical bytes (JSON, keys in a fixed order).
func (h Head) Encode() ([]byte, error) {
	if err := h.check(); err != nil {
		return nil, err
	}
	d := headDoc{VolumeID: h.VolumeID.String(), Epoch: h.Epoch, Holder: h.Holder, ClaimID: h.ClaimID, Activity: h.Activity, Position: h.Position, State: h.State}
	if h.Current != nil {
		d.Current = h.Current.String()
	}
	if len(h.Snapshots) > 0 {
		d.Snapshots = map[string]snapDoc{}
		for n, s := range h.Snapshots {
			sd := snapDoc{Root: s.Root.String(), CreatedNs: s.Meta.CreatedNs, Class: s.Meta.Class, ExpiresNs: s.Meta.ExpiresNs}
			if len(s.Meta.Labels) > 0 {
				sd.Labels = s.Meta.Labels
			}
			for _, dep := range s.Dependents {
				sd.Dependents = append(sd.Dependents, depDoc{Target: dep.Target, TargetID: dep.TargetID.String(), Root: dep.Root.String()})
			}
			d.Snapshots[n] = sd
		}
	}
	b, err := json.Marshal(d)
	if err == nil && len(b) > MaxHeadBytes {
		return nil, fmt.Errorf("%w: %d bytes, at most %d", ErrHeadTooLarge, len(b), MaxHeadBytes)
	}
	return b, err
}

// DecodeHead parses canonical HEAD bytes for the volume whose VOLUME
// record carries volume; unknown fields and non-canonical bytes are
// refused, and so is a HEAD of another volume id (ErrIdentity): one left
// by an interrupted delete, or created late by an old restore, under a
// name since given to another volume. Every reader of HEAD decodes here.
func DecodeHead(b []byte, gen uint64, volume VolumeID) (Head, error) {
	if volume.IsZero() {
		return Head{}, errors.New("layout: DecodeHead needs the volume's id from its VOLUME")
	}
	var d headDoc
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return Head{}, fmt.Errorf("%w: HEAD: %v", ErrMalformed, err)
	}
	vid, err := ParseVolumeID(d.VolumeID)
	if err != nil {
		return Head{}, err
	}
	h := Head{Gen: gen, VolumeID: vid, Epoch: d.Epoch, Holder: d.Holder, ClaimID: d.ClaimID, Activity: d.Activity, Position: d.Position, State: d.State}
	if d.Current != "" {
		rn, err := ParseRootName(d.Current)
		if err != nil {
			return Head{}, err
		}
		h.Current = &rn
	}
	if len(d.Snapshots) > 0 {
		h.Snapshots = map[string]Snapshot{}
		for n, s := range d.Snapshots {
			rn, err := ParseRootName(s.Root)
			if err != nil {
				return Head{}, err
			}
			snap := Snapshot{Root: rn, Meta: SnapshotMeta{CreatedNs: s.CreatedNs, Class: s.Class, ExpiresNs: s.ExpiresNs, Labels: s.Labels}}
			for _, dep := range s.Dependents {
				tid, err := ParseVolumeID(dep.TargetID)
				if err != nil {
					return Head{}, err
				}
				drn, err := ParseRootName(dep.Root)
				if err != nil {
					return Head{}, err
				}
				snap.Dependents = append(snap.Dependents, Dependent{Target: dep.Target, TargetID: tid, Root: drn})
			}
			h.Snapshots[n] = snap
		}
	}
	again, err := h.Encode()
	if err != nil {
		return Head{}, err
	}
	if !bytes.Equal(again, b) {
		return Head{}, fmt.Errorf("%w: HEAD bytes are not canonical", ErrMalformed)
	}
	if h.VolumeID != volume {
		return Head{}, fmt.Errorf("%w: HEAD carries volume id %s, VOLUME has %s", ErrIdentity, h.VolumeID, volume)
	}
	return h, nil
}

// Refusals of a transition's guard. A refusal changes nothing.
var (
	ErrHeadAbsent = errors.New("layout: HEAD absent")
	ErrNotHolder  = errors.New("layout: not the holder at HEAD's epoch")
	ErrBusy       = errors.New("layout: another transaction is open")
	ErrNoSession  = errors.New("layout: the session is not open")
	ErrDetached   = errors.New("layout: detached; a new grant is needed")
)

// HolderID is the identity a transition acts as: the epoch of the line it
// was granted, its server, and the claim id its claim wrote into HEAD. The
// server's name repeats (a restart, a same-host move); a decision never
// rests on it alone.
type HolderID struct {
	Epoch   uint64
	Holder  string
	ClaimID string
}

func (h Head) heldBy(me HolderID) bool {
	return h.Holder != "" && h.Epoch == me.Epoch && h.Holder == me.Holder && h.ClaimID == me.ClaimID
}

// holderErr is the refusal of a holder transition: the volume is being
// deleted or is deleted (ErrVolumeDeleting), or the caller is not the
// holder at HEAD's epoch (ErrNotHolder); nil when it may act.
func (h Head) holderErr(me HolderID) error {
	if h.State == StateDetached {
		return ErrDetached
	}
	if h.State != StateLive {
		return fmt.Errorf("%w: HEAD is %s", ErrVolumeDeleting, h.State)
	}
	if !h.heldBy(me) {
		return fmt.Errorf("%w: HEAD epoch %d holder %q claim %q, caller epoch %d holder %q claim %q", ErrNotHolder, h.Epoch, h.Holder, h.ClaimID, me.Epoch, me.Holder, me.ClaimID)
	}
	return nil
}

// Begin is transition 2: activity idle -> snap(session), by the holder at
// HEAD's epoch. Begin of the session already open is a replay: it returns
// HEAD unchanged and changed=false. Any other open activity is ErrBusy.
func (h Head) Begin(me HolderID, session string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: Begin needs a session id")
	}
	if err := h.holderErr(me); err != nil {
		return Head{}, false, err
	}
	switch {
	case h.Activity == Activity{Kind: ActSnap, Session: session}:
		return h, false, nil
	case h.Activity.Kind != ActIdle:
		return Head{}, false, fmt.Errorf("%w: %s(%s)", ErrBusy, h.Activity.Kind, h.Activity.Session)
	}
	next = h.clone()
	next.Activity = Activity{Kind: ActSnap, Session: session}
	return next, true, nil
}

func (h Head) clone() Head {
	c := h
	if h.Current != nil {
		rn := *h.Current
		c.Current = &rn
	}
	if h.Snapshots != nil {
		c.Snapshots = make(map[string]Snapshot, len(h.Snapshots))
		for n, s := range h.Snapshots {
			c.Snapshots[n] = Snapshot{Root: s.Root, Meta: s.Meta.clone(), Dependents: append([]Dependent(nil), s.Dependents...)}
		}
	}
	return c
}

// ErrPrecondition is returned by HeadStore.Update when HEAD is not at the
// expected generation.
var ErrPrecondition = errors.New("layout: HEAD generation moved")

// HeadStore is the one entry a volume's transitions write: a read, and a
// conditional update on the generation (the store's only all-or-nothing
// unit). An absent HEAD reads as found=false.
type HeadStore interface {
	ReadHead(ctx context.Context) (h Head, found bool, err error)
	UpdateHead(ctx context.Context, expectedGen uint64, next Head) error
}

// RunBegin reads HEAD, applies Begin and commits it by one conditional
// update. A lost reply is resolved by a re-run with the same session: it
// finds snap(session) and returns without writing. An absent HEAD refuses
// (no path creates a HEAD here).
func RunBegin(ctx context.Context, st HeadStore, me HolderID, session string) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.Begin(me, session) })
}

// Abort is transition 5: activity snap(session) -> idle, by the holder at
// HEAD's epoch. Nothing it prepared is reachable, so nothing else changes.
// After its own crash a holder rolls back the session HEAD names by
// aborting it. Abort when HEAD is already idle succeeds and writes
// nothing: it means only that no activity remains (the session may have
// been aborted, committed, or never begun); whether its work was committed
// is read from HEAD's current root, never from Abort's result. Another
// open session or a cleaning is ErrNoSession.
func (h Head) Abort(me HolderID, session string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: Abort needs a session id")
	}
	if err := h.holderErr(me); err != nil {
		return Head{}, false, err
	}
	switch {
	case h.Activity.Kind == ActIdle:
		return h, false, nil
	case h.Activity != Activity{Kind: ActSnap, Session: session}:
		return Head{}, false, fmt.Errorf("%w: HEAD has %s(%s), not snap(%s)", ErrNoSession, h.Activity.Kind, h.Activity.Session, session)
	}
	next = h.clone()
	next.Activity = Activity{Kind: ActIdle}
	return next, true, nil
}

// RunAbort reads HEAD, applies Abort and commits it by one conditional
// update. An absent HEAD refuses.
func RunAbort(ctx context.Context, st HeadStore, me HolderID, session string) (Head, error) {
	return runTransition(ctx, st, func(h Head) (Head, bool, error) { return h.Abort(me, session) })
}

// runTransition is read HEAD, apply t, and one conditional update when t
// changed it. An absent HEAD is refused: no transition creates one.
func runTransition(ctx context.Context, st HeadStore, t func(Head) (Head, bool, error)) (Head, error) {
	h, found, err := st.ReadHead(ctx)
	if err != nil {
		return Head{}, err
	}
	if !found {
		return Head{}, ErrHeadAbsent
	}
	next, changed, err := t(h)
	if err != nil || !changed {
		return next, err
	}
	if err := next.check(); err != nil {
		return Head{}, err
	}
	if err := st.UpdateHead(ctx, h.Gen, next); err != nil {
		return Head{}, err
	}
	next.Gen = h.Gen + 1
	return next, nil
}

// dependents counts the dependents of every snapshot.
func (h Head) dependents() int {
	n := 0
	for _, s := range h.Snapshots {
		n += len(s.Dependents)
	}
	return n
}
