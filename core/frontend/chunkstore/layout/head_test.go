package layout

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

var me = HolderID{Epoch: 3, Holder: "srv-a"}

func idleHead() Head {
	return Head{Gen: 7, Epoch: 3, Holder: "srv-a", Activity: Activity{Kind: ActIdle}}
}

func TestHead_GoldenEncodingRoundTrip(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := Head{Epoch: 3, Holder: "srv-a", Activity: Activity{Kind: ActSnap, Session: "s1"}, Current: &rn, Position: 42,
		Snapshots: map[string]Snapshot{"daily": {Root: rn, Dependents: []string{"t1", "t2"}}}}
	b, err := h.Encode()
	if err != nil {
		t.Fatal(err)
	}
	want := `{"epoch":3,"holder":"srv-a","activity":{"kind":"snap","session":"s1"},"current":"` + rn.String() +
		`","position":42,"snapshots":{"daily":{"root":"` + rn.String() + `","dependents":["t1","t2"]}}}`
	if string(b) != want {
		t.Fatalf("encoding\n got %s\nwant %s", b, want)
	}
	back, err := DecodeHead(b, 9)
	if err != nil {
		t.Fatal(err)
	}
	if back.Gen != 9 || back.Current == nil || *back.Current != rn || back.Snapshots["daily"].Root != rn {
		t.Fatalf("decoded %+v", back)
	}
	if b2, _ := back.Encode(); string(b2) != want {
		t.Fatal("re-encoding differs")
	}
}

func TestHead_Refusals(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	many := map[string]Snapshot{}
	for i := 0; i <= MaxSnapshots; i++ {
		many[fmt.Sprintf("s%03d", i)] = Snapshot{Root: rn}
	}
	for name, h := range map[string]Head{
		"idle with a session":    {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle, Session: "s"}},
		"snap without a session": {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActSnap}},
		"unknown activity":       {Epoch: 1, Holder: "a", Activity: Activity{Kind: "open"}},
		"epoch zero":             {Holder: "a", Activity: Activity{Kind: ActIdle}},
		"no holder":              {Epoch: 1, Activity: Activity{Kind: ActIdle}},
		"dependents unsorted":    {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle}, Snapshots: map[string]Snapshot{"n": {Root: rn, Dependents: []string{"b", "a"}}}},
		"dependent twice":        {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle}, Snapshots: map[string]Snapshot{"n": {Root: rn, Dependents: []string{"a", "a"}}}},
		"bad dependent":          {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle}, Snapshots: map[string]Snapshot{"n": {Root: rn, Dependents: []string{"a/b"}}}},
		"too many snapshots":     {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle}, Snapshots: many},
	} {
		if _, err := h.Encode(); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: %v; want ErrMalformed", name, err)
		}
	}
	good, _ := idleHead().Encode()
	for name, b := range map[string]string{
		"unknown field": strings.Replace(string(good), `"position"`, `"extra":1,"position"`, 1),
		"whitespace":    strings.Replace(string(good), `,`, `, `, 1),
		"bad root":      strings.Replace(string(good), `"position"`, `"current":"xyz","position"`, 1),
		"not json":      "HEAD",
	} {
		if _, err := DecodeHead([]byte(b), 1); !errors.Is(err, ErrMalformed) {
			t.Errorf("decode %s: %v; want ErrMalformed", name, err)
		}
	}
}

// Begin's closure row: idle -> snap(s); snap(s) again is a replay; any
// other open activity, another epoch or another holder is refused and
// changes nothing.
func TestBegin_Closure(t *testing.T) {
	h := idleHead()
	next, changed, err := h.Begin(me, "s1")
	if err != nil || !changed || next.Activity != (Activity{Kind: ActSnap, Session: "s1"}) {
		t.Fatalf("Begin from idle: %+v %t %v", next, changed, err)
	}
	if h.Activity.Kind != ActIdle {
		t.Fatal("Begin changed its receiver")
	}
	if again, changed, err := next.Begin(me, "s1"); err != nil || changed || again.Activity != next.Activity {
		t.Fatalf("Begin of the open session: %+v %t %v; want a replay", again, changed, err)
	}
	for name, tc := range map[string]struct {
		h    Head
		me   HolderID
		want error
	}{
		"other session open": {next, me, ErrBusy},
		"cleaning":           {Head{Epoch: 3, Holder: "srv-a", Activity: Activity{Kind: ActClean, Session: "c1"}}, me, ErrBusy},
		"older caller epoch": {h, HolderID{Epoch: 2, Holder: "srv-a"}, ErrNotHolder},
		"newer caller epoch": {h, HolderID{Epoch: 4, Holder: "srv-a"}, ErrNotHolder},
		"other holder":       {h, HolderID{Epoch: 3, Holder: "srv-b"}, ErrNotHolder},
	} {
		session := "s2"
		if _, _, err := tc.h.Begin(tc.me, session); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v; want %v", name, err, tc.want)
		}
	}
	if _, _, err := h.Begin(me, ""); err == nil {
		t.Error("Begin without a session id accepted")
	}
}

// memHead is a HEAD entry with a generation, and optional kill points: the
// n-th update dies before it applies, or applies and loses its reply.
type memHead struct {
	mu      sync.Mutex
	h       *Head
	updates int
	killAt  int
	after   bool
	dead    bool
	hook    func() // runs between a read and the next update
}

var errKilled = errors.New("process killed")

func (m *memHead) ReadHead(context.Context) (Head, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		return Head{}, false, errKilled
	}
	if m.h == nil {
		return Head{}, false, nil
	}
	b, _ := m.h.Encode()
	h, err := DecodeHead(b, m.h.Gen)
	return h, true, err
}

func (m *memHead) UpdateHead(_ context.Context, gen uint64, next Head) error {
	if m.hook != nil {
		hk := m.hook
		m.hook = nil
		hk()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dead {
		return errKilled
	}
	m.updates++
	kill := m.updates == m.killAt
	if kill && !m.after {
		m.dead = true
		return errKilled
	}
	if m.h == nil || m.h.Gen != gen {
		return ErrPrecondition
	}
	if _, err := next.Encode(); err != nil {
		return err
	}
	n := next.clone()
	n.Gen = gen + 1
	m.h = &n
	if kill {
		m.dead = true
		return errKilled
	}
	return nil
}

func (m *memHead) restart() { m.mu.Lock(); m.dead = false; m.killAt = 0; m.mu.Unlock() }

func TestRunBegin(t *testing.T) {
	ctx := context.Background()
	h := idleHead()
	st := &memHead{h: &h}
	got, err := RunBegin(ctx, st, me, "s1")
	if err != nil || got.Activity.Session != "s1" || got.Gen != 8 || st.h.Gen != 8 {
		t.Fatalf("RunBegin: %+v %v (store gen %d)", got, err, st.h.Gen)
	}
	if _, err := RunBegin(ctx, &memHead{}, me, "s1"); !errors.Is(err, ErrHeadAbsent) {
		t.Fatalf("RunBegin over an absent HEAD: %v; want ErrHeadAbsent", err)
	}
	// Another writer moves HEAD between the read and the update: refused,
	// nothing written by this Begin.
	h2 := idleHead()
	st2 := &memHead{h: &h2}
	st2.hook = func() { st2.h.Gen++ }
	if _, err := RunBegin(ctx, st2, me, "s1"); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("RunBegin racing another writer: %v; want ErrPrecondition", err)
	}
	if st2.h.Activity.Kind != ActIdle {
		t.Fatalf("HEAD after the refused Begin: %+v", st2.h.Activity)
	}
}

// Interrupt at every step: RunBegin killed at each of its HEAD writes and
// re-run with the same session ends in snap(session).
func TestRunBegin_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, idleHead(), func(st HeadStore) error {
		_, err := RunBegin(context.Background(), st, me, "s1")
		return err
	}, func(t *testing.T, h Head) {
		if h.Activity != (Activity{Kind: ActSnap, Session: "s1"}) {
			t.Fatalf("HEAD after Begin: %+v", h.Activity)
		}
	})
}

// interruptHead kills run at each of its HEAD writes (before it applies,
// and applied with its reply lost), re-runs it, and checks the end state:
// want at exactly start.Gen+steps, and a further re-run writes nothing.
func interruptHead(t *testing.T, start Head, run func(HeadStore) error, want func(*testing.T, Head)) {
	t.Helper()
	fresh := func() *Head { h := start.clone(); return &h }
	count := &memHead{h: fresh()}
	if err := run(count); err != nil {
		t.Fatalf("uninterrupted: %v", err)
	}
	want(t, *count.h)
	steps := count.updates
	if steps == 0 {
		t.Fatal("the run made no HEAD write")
	}
	for k := 1; k <= steps; k++ {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("write%d/after=%t", k, after), func(t *testing.T) {
				st := &memHead{h: fresh(), killAt: k, after: after}
				if err := run(st); !errors.Is(err, errKilled) {
					t.Fatalf("killed run: %v", err)
				}
				st.restart()
				if err := run(st); err != nil {
					t.Fatalf("re-run: %v", err)
				}
				want(t, *st.h)
				if st.h.Gen != start.Gen+uint64(steps) {
					t.Fatalf("HEAD gen %d after the re-run; want %d", st.h.Gen, start.Gen+uint64(steps))
				}
				before := st.updates
				if err := run(st); err != nil || st.updates != before {
					t.Fatalf("second re-run: %v, wrote %d times", err, st.updates-before)
				}
			})
		}
	}
}

func snapHead(session string) Head {
	rn := RootName{Hash: Sum([]byte("r"))}
	h := idleHead()
	h.Activity = Activity{Kind: ActSnap, Session: session}
	h.Current = &rn
	h.Position = 40
	h.Snapshots = map[string]Snapshot{"daily": {Root: rn, Dependents: []string{"t1"}}}
	return h
}

func TestAbort_Closure(t *testing.T) {
	h := snapHead("s1")
	next, changed, err := h.Abort(me, "s1")
	if err != nil || !changed || next.Activity != (Activity{Kind: ActIdle}) {
		t.Fatalf("Abort of the open session: %+v %t %v", next, changed, err)
	}
	if *next.Current != *h.Current || next.Position != h.Position || len(next.Snapshots) != 1 || next.Snapshots["daily"].Dependents[0] != "t1" {
		t.Fatalf("Abort changed more than activity: %+v", next)
	}
	if h.Activity.Kind != ActSnap {
		t.Fatal("Abort changed its receiver")
	}
	if again, changed, err := next.Abort(me, "s1"); err != nil || changed || again.Activity.Kind != ActIdle {
		t.Fatalf("Abort over idle: %+v %t %v; want a replay", again, changed, err)
	}
	for name, tc := range map[string]struct {
		h    Head
		me   HolderID
		want error
	}{
		"other session open": {snapHead("s2"), me, ErrNoSession},
		"cleaning":           {Head{Epoch: 3, Holder: "srv-a", Activity: Activity{Kind: ActClean, Session: "s1"}}, me, ErrNoSession},
		"other epoch":        {h, HolderID{Epoch: 4, Holder: "srv-a"}, ErrNotHolder},
		"other holder":       {h, HolderID{Epoch: 3, Holder: "srv-b"}, ErrNotHolder},
	} {
		if _, _, err := tc.h.Abort(tc.me, "s1"); !errors.Is(err, tc.want) {
			t.Errorf("%s: %v; want %v", name, err, tc.want)
		}
	}
	if _, _, err := h.Abort(me, ""); err == nil {
		t.Error("Abort without a session id accepted")
	}
}

// After its own crash a holder finds snap(s) at its epoch with nothing in
// memory: a new session beside s is refused; rolling s back by Abort, then
// Begin of a new session, succeeds.
func TestAbort_RollBackAfterOwnCrash(t *testing.T) {
	ctx := context.Background()
	h := snapHead("s1")
	st := &memHead{h: &h}
	if _, err := RunBegin(ctx, st, me, "s2"); !errors.Is(err, ErrBusy) {
		t.Fatalf("a new session beside the open one: %v; want ErrBusy", err)
	}
	cur, _, err := st.ReadHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RunAbort(ctx, st, me, cur.Activity.Session); err != nil {
		t.Fatal(err)
	}
	got, err := RunBegin(ctx, st, me, "s2")
	if err != nil || got.Activity != (Activity{Kind: ActSnap, Session: "s2"}) || st.h.Gen != h.Gen+2 {
		t.Fatalf("Begin after the roll back: %+v %v gen %d", got, err, st.h.Gen)
	}
	if _, err := RunAbort(ctx, &memHead{}, me, "s1"); !errors.Is(err, ErrHeadAbsent) {
		t.Fatalf("Abort over an absent HEAD: %v", err)
	}
}

func TestRunAbort_InterruptAtEveryStep(t *testing.T) {
	interruptHead(t, snapHead("s1"), func(st HeadStore) error {
		_, err := RunAbort(context.Background(), st, me, "s1")
		return err
	}, func(t *testing.T, h Head) {
		if h.Activity != (Activity{Kind: ActIdle}) || h.Position != 40 {
			t.Fatalf("HEAD after Abort: %+v", h)
		}
	})
}

// A name that is not valid UTF-8 would be stored as U+FFFD by the JSON
// encoding (and still re-encode the same), so HEAD would hold another name
// than the caller gave: refused on every string field.
func TestHead_RefusesInvalidUTF8(t *testing.T) {
	rn := RootName{Hash: Sum([]byte("r"))}
	bad := "a\xff"
	for name, h := range map[string]Head{
		"holder":    {Epoch: 1, Holder: bad, Activity: Activity{Kind: ActIdle}},
		"session":   {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActSnap, Session: bad}},
		"snapshot":  {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle}, Snapshots: map[string]Snapshot{bad: {Root: rn}}},
		"dependent": {Epoch: 1, Holder: "a", Activity: Activity{Kind: ActIdle}, Snapshots: map[string]Snapshot{"n": {Root: rn, Dependents: []string{bad}}}},
	} {
		if _, err := h.Encode(); !errors.Is(err, ErrMalformed) {
			t.Errorf("%s %q: %v; want ErrMalformed", name, bad, err)
		}
	}
	h := idleHead()
	st := &memHead{h: &h}
	if _, err := RunBegin(context.Background(), st, me, bad); err == nil || st.h.Activity.Kind != ActIdle {
		t.Fatalf("Begin with an invalid UTF-8 session: %v, HEAD %+v; want refused, nothing written", err, st.h.Activity)
	}
}
