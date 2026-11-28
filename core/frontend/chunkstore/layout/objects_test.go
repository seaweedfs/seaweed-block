package layout

import (
	"context"
	"errors"
	"path"
	"sync"
	"testing"
)

// memObject is one entry: its bytes and metadata.
type memObject struct {
	data []byte
	meta ObjectMeta
}

// memObjects is an ObjectStore of one volume, keyed by full path under
// root, with an optional failing path, a record of every Stat, and a kill
// point counted over its writes (Create, Reuse, Mark, DeleteMarked).
type memObjects struct {
	statMu sync.Mutex // RunCommit looks objects up concurrently
	root   string
	volume VolumeID // stamped on every entry the store creates
	objs   map[string]*memObject
	fail   string
	asked  []string
	writes []string
	killAt int
	after  bool
	dead   bool
	hook   func(op, rel string) // runs before a write applies
	// afterStat runs after a Stat of rel returned (a writer acting between
	// a read and the conditional write that follows it).
	afterStat func(rel string)
}

func newMemObjects(root string) *memObjects {
	return &memObjects{root: root, objs: map[string]*memObject{}}
}

func (m *memObjects) full(rel string) string { return path.Join(m.root, rel) }

func (m *memObjects) Stat(_ context.Context, rel string) (ObjectMeta, bool, error) {
	meta, found, err := m.statLocked(rel)
	if err == nil && m.afterStat != nil {
		m.afterStat(rel) // outside the lock: a hook may write to the store
	}
	return meta, found, err
}

// statLocked is Stat's lookup, under statMu: RunCommit looks objects up
// concurrently.
func (m *memObjects) statLocked(rel string) (ObjectMeta, bool, error) {
	m.statMu.Lock()
	defer m.statMu.Unlock()
	if m.dead {
		return ObjectMeta{}, false, errKilled
	}
	p := m.full(rel)
	m.asked = append(m.asked, p)
	if p == m.fail {
		return ObjectMeta{}, false, errors.New("filer unavailable")
	}
	o, ok := m.objs[p]
	if !ok {
		return ObjectMeta{}, false, nil
	}
	return o.meta, true, nil
}

// write runs op's effect under the kill point: before it applies, or
// applied with its reply lost.
func (m *memObjects) write(op, rel string, apply func() error) error {
	if m.dead {
		return errKilled
	}
	if m.hook != nil {
		m.hook(op, rel)
	}
	m.writes = append(m.writes, op+" "+rel)
	kill := len(m.writes) == m.killAt
	if kill && !m.after {
		m.dead = true
		return errKilled
	}
	err := apply()
	if kill {
		m.dead = true
		return errKilled
	}
	return err
}

func (m *memObjects) Create(_ context.Context, rel string, data []byte, session string) error {
	return m.write("create", rel, func() error {
		if _, ok := m.objs[m.full(rel)]; ok {
			return ErrExists
		}
		m.objs[m.full(rel)] = &memObject{data: append([]byte(nil), data...), meta: ObjectMeta{Session: session, Volume: m.volume}}
		return nil
	})
}

func (m *memObjects) Reuse(_ context.Context, rel string, expect ObjectMeta, session string) error {
	return m.write("reuse", rel, func() error {
		o, ok := m.objs[m.full(rel)]
		if !ok || o.meta != expect {
			return ErrPrecondition
		}
		o.meta = ObjectMeta{Session: session, Volume: o.meta.Volume}
		return nil
	})
}

func (m *memObjects) Mark(_ context.Context, rel string, expect ObjectMeta, mark string) error {
	return m.write("mark", rel, func() error {
		o, ok := m.objs[m.full(rel)]
		if !ok || o.meta != expect {
			return ErrPrecondition
		}
		o.meta.Mark = mark
		return nil
	})
}

func (m *memObjects) DeleteMarked(_ context.Context, rel string, mark string) error {
	return m.write("delete", rel, func() error {
		o, ok := m.objs[m.full(rel)]
		if !ok {
			return nil
		}
		if o.meta.Mark != mark {
			return ErrPrecondition
		}
		delete(m.objs, m.full(rel))
		return nil
	})
}

func (m *memObjects) DeleteOfVolume(_ context.Context, rel string, volume VolumeID) error {
	return m.write("delete-of-volume", rel, func() error {
		o, ok := m.objs[m.full(rel)]
		if !ok {
			return nil
		}
		if o.meta.Volume != volume {
			return ErrPrecondition
		}
		delete(m.objs, m.full(rel))
		return nil
	})
}

// put stores an object directly (fixture setup, no kill point).
func (m *memObjects) put(rel string, data []byte, session string) {
	m.objs[m.full(rel)] = &memObject{data: append([]byte(nil), data...), meta: ObjectMeta{Session: session, Volume: m.volume}}
}

func (m *memObjects) has(rel string) bool { _, ok := m.objs[m.full(rel)]; return ok }

// data returns every object's bytes by full path (for the reachability
// walk and the reader).
func (m *memObjects) data() map[string][]byte {
	out := map[string][]byte{}
	for p, o := range m.objs {
		out[p] = o.data
	}
	return out
}

func (m *memObjects) fetch(_ context.Context, p string) ([]byte, bool, error) {
	if m.dead {
		return nil, false, errKilled
	}
	o, ok := m.objs[p]
	if !ok {
		return nil, false, nil
	}
	return o.data, true, nil
}

func (m *memObjects) restart() { m.dead, m.killAt = false, 0 }

// EnsureObject: an absent object is created by the session; an existing
// one, written by another session or marked, is reused (session set, mark
// cleared); one already the session's and unmarked is left; a racing
// writer between the read and the conditional write is retried.
func TestEnsureObject(t *testing.T) {
	ctx := context.Background()
	o := newMemObjects("/v")
	if err := EnsureObject(ctx, o, "pool/0/a", []byte("x"), "s1"); err != nil || o.objs["/v/pool/0/a"].meta != (ObjectMeta{Session: "s1"}) {
		t.Fatalf("create: %v %+v", err, o.objs["/v/pool/0/a"])
	}
	writes := len(o.writes)
	if err := EnsureObject(ctx, o, "pool/0/a", []byte("x"), "s1"); err != nil || len(o.writes) != writes {
		t.Fatalf("own unmarked object rewritten: %v (%d writes)", err, len(o.writes)-writes)
	}
	o.objs["/v/pool/0/a"].meta = ObjectMeta{Session: "c9", Mark: "c9"}
	if err := EnsureObject(ctx, o, "pool/0/a", []byte("x"), "s2"); err != nil || o.objs["/v/pool/0/a"].meta != (ObjectMeta{Session: "s2"}) {
		t.Fatalf("reuse of a marked object: %v %+v; want session s2, mark cleared", err, o.objs["/v/pool/0/a"].meta)
	}
	raced := false
	o.hook = func(op, rel string) {
		if op == "reuse" && !raced {
			raced = true
			o.objs["/v/pool/0/a"].meta = ObjectMeta{Session: "other"}
		}
	}
	if err := EnsureObject(ctx, o, "pool/0/a", []byte("x"), "s3"); err != nil || o.objs["/v/pool/0/a"].meta.Session != "s3" {
		t.Fatalf("reuse racing another writer: %v %+v; want retried and s3", err, o.objs["/v/pool/0/a"].meta)
	}
	if err := EnsureObject(ctx, o, "pool/0/b", nil, ""); err == nil {
		t.Fatal("EnsureObject without a session accepted")
	}
}
