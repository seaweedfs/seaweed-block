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
const MaxSnapshots = 100

// Activity is HEAD's open transaction. Session names it; a session id is
// never reused.
type Activity struct {
	Kind    string `json:"kind"`
	Session string `json:"session,omitempty"`
}

// Snapshot is one named snapshot: its root and the restore targets that
// still name its chunks.
type Snapshot struct {
	Root       RootName
	Dependents []string
}

// Head is the decoded HEAD. Gen is the generation the store holds it at;
// it is not part of the encoding.
type Head struct {
	Gen       uint64
	Epoch     uint64
	Holder    string
	Activity  Activity
	Current   *RootName // nil: no image committed yet
	Position  uint64
	Snapshots map[string]Snapshot
}

type headDoc struct {
	Epoch     uint64             `json:"epoch"`
	Holder    string             `json:"holder"`
	Activity  Activity           `json:"activity"`
	Current   string             `json:"current,omitempty"`
	Position  uint64             `json:"position"`
	Snapshots map[string]snapDoc `json:"snapshots,omitempty"`
}

type snapDoc struct {
	Root       string   `json:"root"`
	Dependents []string `json:"dependents,omitempty"`
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
	for field, v := range map[string]string{"holder": h.Holder, "session": h.Activity.Session} {
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
	if h.Epoch == 0 || h.Holder == "" {
		return fmt.Errorf("%w: HEAD without epoch or holder", ErrMalformed)
	}
	if len(h.Snapshots) > MaxSnapshots {
		return fmt.Errorf("%w: %d snapshots, at most %d", ErrMalformed, len(h.Snapshots), MaxSnapshots)
	}
	for n, s := range h.Snapshots {
		if n == "" {
			return fmt.Errorf("%w: unnamed snapshot", ErrMalformed)
		}
		if err := validName("snapshot", n); err != nil {
			return err
		}
		for i, d := range s.Dependents {
			if err := validName("dependent", d); err != nil {
				return err
			}
			if err := checkOwner(d); err != nil {
				return err
			}
			if i > 0 && s.Dependents[i-1] >= d {
				return fmt.Errorf("%w: snapshot %q dependents not sorted and unique", ErrMalformed, n)
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
	d := headDoc{Epoch: h.Epoch, Holder: h.Holder, Activity: h.Activity, Position: h.Position}
	if h.Current != nil {
		d.Current = h.Current.String()
	}
	if len(h.Snapshots) > 0 {
		d.Snapshots = map[string]snapDoc{}
		for n, s := range h.Snapshots {
			d.Snapshots[n] = snapDoc{Root: s.Root.String(), Dependents: s.Dependents}
		}
	}
	return json.Marshal(d)
}

// DecodeHead parses canonical HEAD bytes; unknown fields and non-canonical
// bytes are refused.
func DecodeHead(b []byte, gen uint64) (Head, error) {
	var d headDoc
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return Head{}, fmt.Errorf("%w: HEAD: %v", ErrMalformed, err)
	}
	h := Head{Gen: gen, Epoch: d.Epoch, Holder: d.Holder, Activity: d.Activity, Position: d.Position}
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
			h.Snapshots[n] = Snapshot{Root: rn, Dependents: s.Dependents}
		}
	}
	again, err := h.Encode()
	if err != nil {
		return Head{}, err
	}
	if !bytes.Equal(again, b) {
		return Head{}, fmt.Errorf("%w: HEAD bytes are not canonical", ErrMalformed)
	}
	return h, nil
}

// Refusals of a transition's guard. A refusal changes nothing.
var (
	ErrHeadAbsent = errors.New("layout: HEAD absent")
	ErrNotHolder  = errors.New("layout: not the holder at HEAD's epoch")
	ErrBusy       = errors.New("layout: another transaction is open")
	ErrNoSession  = errors.New("layout: the session is not open")
)

// HolderID is the identity a transition acts as: the epoch of the line it was
// granted and its server.
type HolderID struct {
	Epoch  uint64
	Holder string
}

func (h Head) heldBy(me HolderID) bool { return h.Epoch == me.Epoch && h.Holder == me.Holder }

// Begin is transition 2: activity idle -> snap(session), by the holder at
// HEAD's epoch. Begin of the session already open is a replay: it returns
// HEAD unchanged and changed=false. Any other open activity is ErrBusy.
func (h Head) Begin(me HolderID, session string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: Begin needs a session id")
	}
	if !h.heldBy(me) {
		return Head{}, false, fmt.Errorf("%w: HEAD epoch %d holder %q, caller epoch %d holder %q", ErrNotHolder, h.Epoch, h.Holder, me.Epoch, me.Holder)
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
			c.Snapshots[n] = Snapshot{Root: s.Root, Dependents: append([]string(nil), s.Dependents...)}
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
// aborting it. Abort when HEAD is already idle is a replay (the lost reply
// of an earlier Abort: no one else can close this holder's session at its
// epoch) and writes nothing. Another open session or a cleaning is
// ErrNoSession.
func (h Head) Abort(me HolderID, session string) (next Head, changed bool, err error) {
	if session == "" {
		return Head{}, false, errors.New("layout: Abort needs a session id")
	}
	if !h.heldBy(me) {
		return Head{}, false, fmt.Errorf("%w: HEAD epoch %d holder %q, caller epoch %d holder %q", ErrNotHolder, h.Epoch, h.Holder, me.Epoch, me.Holder)
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
