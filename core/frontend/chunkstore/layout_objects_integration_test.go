//go:build chunkstore_integration

package chunkstore

import (
	"context"
	"errors"
	"path"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

// filerObjects is a layout.ObjectStore on a real filer: an object is an
// entry under root whose extended attributes carry the writer's session
// and the removal mark ("-" when unmarked); every change of an existing
// entry is a conditional update or delete on those attributes.
type filerObjects struct {
	cl   *SWFS
	root string
}

const (
	objSessKey = "swb-sess"
	objMarkKey = "swb-mark"
	unmarked   = "-"
)

func (f filerObjects) loc(rel string) (string, string) {
	full := path.Join(f.root, rel)
	return path.Dir(full), path.Base(full)
}

func metaOf(e *filerpb.Entry) layout.ObjectMeta {
	m := layout.ObjectMeta{Session: string(e.GetExtended()[objSessKey]), Mark: string(e.GetExtended()[objMarkKey])}
	if m.Mark == unmarked {
		m.Mark = ""
	}
	return m
}

func markValue(m string) []byte {
	if m == "" {
		return []byte(unmarked)
	}
	return []byte(m)
}

func expected(m layout.ObjectMeta) map[string][]byte {
	return map[string][]byte{objSessKey: []byte(m.Session), objMarkKey: markValue(m.Mark)}
}

func layoutErr(err error) error {
	if errors.Is(err, ErrPrecondition) {
		return layout.ErrPrecondition
	}
	return err
}

func (f filerObjects) Stat(ctx context.Context, rel string) (layout.ObjectMeta, bool, error) {
	dir, name := f.loc(rel)
	e, err := f.cl.LookupEntry(ctx, dir, name)
	if errors.Is(err, ErrNotFound) {
		return layout.ObjectMeta{}, false, nil
	}
	if err != nil {
		return layout.ObjectMeta{}, false, err
	}
	return metaOf(e), true, nil
}

func (f filerObjects) Create(ctx context.Context, rel string, data []byte, session string) error {
	dir, name := f.loc(rel)
	e := &filerpb.Entry{Name: name, Content: data, Attributes: &filerpb.FuseAttributes{FileMode: 0o644, Mtime: time.Now().Unix()},
		Extended: map[string][]byte{objSessKey: []byte(session), objMarkKey: []byte(unmarked)}}
	err := f.cl.CreateEntry(ctx, dir, e, true)
	if errors.Is(err, ErrExists) {
		return layout.ErrExists
	}
	return layoutErr(err)
}

// rewrite updates the entry's attributes to next, conditional on expect.
func (f filerObjects) rewrite(ctx context.Context, rel string, expect, next layout.ObjectMeta) error {
	dir, name := f.loc(rel)
	e, err := f.cl.LookupEntry(ctx, dir, name)
	if errors.Is(err, ErrNotFound) {
		return layout.ErrPrecondition
	}
	if err != nil {
		return err
	}
	e.Extended[objSessKey] = []byte(next.Session)
	e.Extended[objMarkKey] = markValue(next.Mark)
	return layoutErr(f.cl.UpdateEntry(ctx, dir, e, expected(expect)))
}

func (f filerObjects) Reuse(ctx context.Context, rel string, expect layout.ObjectMeta, session string) error {
	return f.rewrite(ctx, rel, expect, layout.ObjectMeta{Session: session})
}

func (f filerObjects) Mark(ctx context.Context, rel string, expect layout.ObjectMeta, mark string) error {
	return f.rewrite(ctx, rel, expect, layout.ObjectMeta{Session: expect.Session, Mark: mark})
}

func (f filerObjects) DeleteMarked(ctx context.Context, rel string, mark string) error {
	dir, name := f.loc(rel)
	err := f.cl.DeleteEntry(ctx, dir, name, map[string][]byte{objMarkKey: markValue(mark)})
	if errors.Is(err, ErrPrecondition) {
		if _, found, serr := f.Stat(ctx, rel); serr == nil && !found {
			return nil // a missing entry succeeds
		}
		return layout.ErrPrecondition
	}
	return err
}

// On a real filer (stock weed): the removal's two conditional writes and
// the upload's reuse write behave as the layout needs. A delete whose
// mark does not match is refused; a reuse (another session, mark cleared)
// makes the remover's late mark and late delete both fail their
// conditions and the object stays; a mark on the current metadata and its
// own delete remove it.
func TestIntegration_ObjectMarkAndConditionalDelete(t *testing.T) {
	c := startWeed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const root = "/swblock/objs/v1"
	f := filerObjects{cl: c.client(t, root+"/"), root: root}
	rel := layout.NameChunk(5, []byte("orphan")).Path()

	if err := layout.EnsureObject(ctx, f, rel, []byte("orphan"), "s0"); err != nil {
		t.Fatal(err)
	}
	plan, found, err := f.Stat(ctx, rel)
	if err != nil || !found || plan != (layout.ObjectMeta{Session: "s0"}) {
		t.Fatalf("created object: %+v %t %v", plan, found, err)
	}
	if err := f.Mark(ctx, rel, plan, "c1"); err != nil {
		t.Fatalf("mark on the plan's metadata: %v", err)
	}
	if err := f.DeleteMarked(ctx, rel, "c9"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("delete with another mark: %v; want refused by the filer", err)
	}
	if _, found, _ := f.Stat(ctx, rel); !found {
		t.Fatal("a refused conditional delete removed the entry")
	}
	// A successor reuses the object: session set, mark cleared.
	if err := layout.EnsureObject(ctx, f, rel, []byte("orphan"), "successor"); err != nil {
		t.Fatal(err)
	}
	if m, _, _ := f.Stat(ctx, rel); m != (layout.ObjectMeta{Session: "successor"}) {
		t.Fatalf("after the reuse: %+v; want session successor, unmarked", m)
	}
	if err := f.DeleteMarked(ctx, rel, "c1"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("the old remover's late delete: %v; want refused by the filer", err)
	}
	if err := f.Mark(ctx, rel, plan, "c1"); !errors.Is(err, layout.ErrPrecondition) {
		t.Fatalf("the old remover's late mark on its plan's metadata: %v; want refused by the filer", err)
	}
	if _, found, _ := f.Stat(ctx, rel); !found {
		t.Fatal("the reused object was removed")
	}
	// A remover planning on the current metadata removes it.
	now, _, _ := f.Stat(ctx, rel)
	if err := f.Mark(ctx, rel, now, "c2"); err != nil {
		t.Fatal(err)
	}
	if err := f.DeleteMarked(ctx, rel, "c2"); err != nil {
		t.Fatalf("delete with its own mark: %v", err)
	}
	if _, found, _ := f.Stat(ctx, rel); found {
		t.Fatal("the marked object was not deleted")
	}
	if err := f.DeleteMarked(ctx, rel, "c2"); err != nil {
		t.Fatalf("delete of a missing entry: %v; want success", err)
	}
}
