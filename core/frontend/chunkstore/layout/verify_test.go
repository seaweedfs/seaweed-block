package layout

import (
	"context"
	"errors"
	"path"
	"sort"
	"strings"
	"testing"
)

// memStore is a store of whole objects by path, with directory listing.
type memStore struct {
	obj     map[string][]byte
	readErr string // a path whose read fails
}

func (m *memStore) fetch(_ context.Context, p string) ([]byte, bool, error) {
	if p == m.readErr {
		return nil, false, errors.New("injected read failure")
	}
	b, ok := m.obj[p]
	return b, ok, nil
}

func (m *memStore) list(_ context.Context, dir string) ([]string, error) {
	seen := map[string]bool{}
	for p := range m.obj {
		if rest, ok := strings.CutPrefix(p, dir+"/"); ok {
			seen[strings.SplitN(rest, "/", 2)[0]] = true
		}
	}
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

const vbase = "/chunks"

// putImage stores the chunks of data (index -> bytes, owner vol) and their root; returns the root name.
func putImage(t *testing.T, m *memStore, vol string, data map[uint64]string, borrowed map[uint64]Ref) RootName {
	t.Helper()
	manifest := map[uint64]Ref{}
	for i, d := range data {
		c := NameChunk(i, []byte(d))
		m.obj[path.Join(vbase, vol, c.Path())] = []byte(d)
		manifest[i] = Ref{Owner: vol, OwnerID: vid(vol, 0), Chunk: c}
	}
	for i, r := range borrowed {
		manifest[i] = r
	}
	root, segs, err := Build(manifest)
	if err != nil {
		t.Fatal(err)
	}
	for n, b := range segs {
		m.obj[path.Join(vbase, vol, n.Path())] = b
	}
	rn, rb, err := NameRoot(root)
	if err != nil {
		t.Fatal(err)
	}
	m.obj[path.Join(vbase, vol, rn.Path())] = rb
	return rn
}

// vid is a fixed id per volume name (n: a generation, to model a volume created again).
func vid(vol string, n byte) VolumeID {
	var id VolumeID
	copy(id[:], vol)
	id[15] = n + 1
	return id
}

func vputVolume(t *testing.T, m *memStore, vol string, id VolumeID) {
	t.Helper()
	b, err := Volume{ID: id, Name: vol, Format: VolumeFormat}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	m.obj[path.Join(vbase, vol, VolumeName)] = b
}

func putHead(t *testing.T, m *memStore, vol string, cur *RootName, snaps map[string]Snapshot) {
	t.Helper()
	vputVolume(t, m, vol, vid(vol, 0))
	b, err := Head{VolumeID: vid(vol, 0), Epoch: 1, Holder: "h1", ClaimID: "c1", Activity: Activity{Kind: ActIdle},
		Current: cur, Snapshots: snaps}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	m.obj[path.Join(vbase, vol, HeadName)] = b
}

func newVol(t *testing.T) (*memStore, RootName) {
	m := &memStore{obj: map[string][]byte{}}
	rn := putImage(t, m, "v1", map[uint64]string{0: "aaaa", 7: "bbbb", ShardSize + 3: "cccc"}, nil)
	putHead(t, m, "v1", &rn, nil)
	return m, rn
}

func run(t *testing.T, m *memStore, opts VerifyOptions) VerifyResult {
	t.Helper()
	opts.List = m.list
	r, err := Verify(context.Background(), vbase, "v1", m.fetch, opts)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func classes(r VerifyResult) string {
	var s []string
	for _, f := range r.Findings {
		s = append(s, f.Class+" "+f.Path)
	}
	return strings.Join(s, "; ")
}

func TestVerifyCleanVolume(t *testing.T) {
	m, _ := newVol(t)
	r := run(t, m, VerifyOptions{Depth: DepthFull, Orphans: true})
	if !r.Complete || len(r.Findings) != 0 || r.Roots != 1 || r.Segments != 2 || r.Chunks != 3 || !r.OrphansChecked {
		t.Fatalf("clean volume: %+v", r)
	}
}

func chunkPath(i uint64, d string) string {
	return path.Join(vbase, "v1", NameChunk(i, []byte(d)).Path())
}

func TestVerifyFindsMissingAndCorrupt(t *testing.T) {
	m, rn := newVol(t)
	delete(m.obj, chunkPath(7, "bbbb"))
	m.obj[chunkPath(0, "aaaa")] = []byte("XXXX")
	r := run(t, m, VerifyOptions{Depth: DepthFull})
	if c := classes(r); !strings.Contains(c, "MISSING v1/pool/0/7-") || !strings.Contains(c, "CORRUPT v1/pool/0/0-") || len(r.Findings) != 2 {
		t.Fatalf("full depth: %s", c)
	}
	// names depth: presence only, by design (the bytes are not read)
	r = run(t, m, VerifyOptions{Depth: DepthNames})
	if c := classes(r); !strings.Contains(c, "MISSING v1/pool/0/7-") || strings.Contains(c, "CORRUPT") {
		t.Fatalf("names depth: %s", c)
	}
	// a corrupt root and a missing segment
	m2, rn2 := newVol(t)
	m2.obj[path.Join(vbase, "v1", rn2.Path())] = []byte("not a root")
	if c := classes(run(t, m2, VerifyOptions{})); !strings.Contains(c, "CORRUPT v1/root/") {
		t.Fatalf("corrupt root: %s", c)
	}
	m3, _ := newVol(t)
	for p := range m3.obj {
		if strings.Contains(p, "/seg/1-") {
			delete(m3.obj, p)
		}
	}
	if c := classes(run(t, m3, VerifyOptions{})); !strings.Contains(c, "MISSING v1/seg/1-") {
		t.Fatalf("missing segment: %s", c)
	}
	_ = rn
}

func TestVerifyHeadMissingOrMalformedIsIncomplete(t *testing.T) {
	m, _ := newVol(t)
	delete(m.obj, path.Join(vbase, "v1", HeadName))
	r := run(t, m, VerifyOptions{Orphans: true})
	if r.Complete || classes(r) != "MISSING v1/HEAD" || r.OrphansChecked {
		t.Fatalf("no HEAD: %+v", r)
	}
	m.obj[path.Join(vbase, "v1", HeadName)] = []byte("{")
	if r = run(t, m, VerifyOptions{}); r.Complete || !strings.HasPrefix(classes(r), "MALFORMED v1/HEAD") {
		t.Fatalf("bad HEAD: %+v", r)
	}
}

func TestVerifyWalksSnapshotRoots(t *testing.T) {
	m, cur := newVol(t)
	snap := putImage(t, m, "v1", map[uint64]string{2: "ssss"}, nil)
	putHead(t, m, "v1", &cur, map[string]Snapshot{"s1": {Root: snap}})
	if r := run(t, m, VerifyOptions{Depth: DepthFull, Orphans: true}); !r.Complete || len(r.Findings) != 0 || r.Roots != 2 {
		t.Fatalf("with snapshot: %+v", r)
	}
	delete(m.obj, chunkPath(2, "ssss"))
	if c := classes(run(t, m, VerifyOptions{})); c != "MISSING v1/pool/0/"+NameChunk(2, []byte("ssss")).String() {
		t.Fatalf("snapshot-only chunk: %s", c)
	}
}

func TestVerifyOrphans(t *testing.T) {
	m, _ := newVol(t)
	extra := NameChunk(9, []byte("zzzz"))
	m.obj[path.Join(vbase, "v1", extra.Path())] = []byte("zzzz")
	m.obj[path.Join(vbase, "v1", SegDir, "garbage")] = []byte("x")
	r := run(t, m, VerifyOptions{Orphans: true})
	if c := classes(r); c != "ORPHAN v1/"+extra.Path()+"; MALFORMED v1/seg/garbage" && c != "MALFORMED v1/seg/garbage; ORPHAN v1/"+extra.Path() {
		t.Fatalf("orphans: %s", c)
	}
	if r = run(t, m, VerifyOptions{}); len(r.Findings) != 0 || r.OrphansChecked {
		t.Fatalf("orphans are reported only on request: %+v", r)
	}
}

func TestVerifyDependentReachIsNotOrphanAndUnreadableDependentSuppresses(t *testing.T) {
	m, cur := newVol(t)
	// v1 snapshot s1 names chunk 4; restore target v2's current root borrows it
	snap := putImage(t, m, "v1", map[uint64]string{4: "dddd"}, nil)
	putHead(t, m, "v1", &cur, map[string]Snapshot{"s1": {Root: snap, Dependents: []Dependent{{Target: "v2", TargetID: vid("v2", 0), Root: snap}}}})
	borrowed := NameChunk(5, []byte("eeee"))
	m.obj[path.Join(vbase, "v1", borrowed.Path())] = []byte("eeee") // only v2 names it
	v2 := putImage(t, m, "v2", nil, map[uint64]Ref{5: {Owner: "v1", OwnerID: vid("v1", 0), Chunk: borrowed}})
	putHead(t, m, "v2", &v2, nil)
	if r := run(t, m, VerifyOptions{Orphans: true}); !r.Complete || len(r.Findings) != 0 || !r.OrphansChecked {
		t.Fatalf("a chunk only the dependent names is not an orphan: %+v", r)
	}
	head2 := m.obj[path.Join(vbase, "v2", HeadName)]
	delete(m.obj, path.Join(vbase, "v2", HeadName))
	if r := run(t, m, VerifyOptions{Orphans: true}); r.Complete || r.OrphansChecked || len(r.Findings) != 0 {
		t.Fatalf("an unreadable dependent must suppress orphans: %+v", r)
	}
	// v2 created again under the same name (new id): no longer a dependent, so the borrowed chunk is an orphan
	m.obj[path.Join(vbase, "v2", HeadName)] = head2
	vputVolume(t, m, "v2", vid("v2", 1))
	if r := run(t, m, VerifyOptions{Orphans: true}); !r.Complete || classes(r) != "ORPHAN v1/"+borrowed.Path() {
		t.Fatalf("a re-created target matches no dependent: %+v", r)
	}
}

func TestVerifyVolumeRecordAndHeadID(t *testing.T) {
	m, _ := newVol(t)
	delete(m.obj, path.Join(vbase, "v1", VolumeName))
	if r := run(t, m, VerifyOptions{}); r.Complete || classes(r) != "MISSING v1/VOLUME" {
		t.Fatalf("no VOLUME: %+v", r)
	}
	vputVolume(t, m, "v1", vid("v1", 9)) // a VOLUME of another id: HEAD names v1's first id
	if r := run(t, m, VerifyOptions{}); r.Complete || !strings.HasPrefix(classes(r), "MALFORMED v1/HEAD") {
		t.Fatalf("HEAD of another volume id: %+v", r)
	}
}

func TestVerifyChunkOwnerIdentity(t *testing.T) {
	m, _ := newVol(t)
	vputVolume(t, m, "v1", vid("v1", 9)) // VOLUME re-created: chunk refs name the old id (HEAD too)
	m2, _ := newVol(t)
	snap := putImage(t, m2, "v1", nil, map[uint64]Ref{4: {Owner: "v3", OwnerID: vid("v3", 0), Chunk: NameChunk(4, []byte("gggg"))}})
	cur := putImage(t, m2, "v1", map[uint64]string{0: "aaaa"}, nil)
	putHead(t, m2, "v1", &cur, map[string]Snapshot{"s1": {Root: snap}})
	m2.obj[path.Join(vbase, "v3", NameChunk(4, []byte("gggg")).Path())] = []byte("gggg") // v3 has no VOLUME
	if c := classes(run(t, m2, VerifyOptions{Depth: DepthFull})); !strings.HasPrefix(c, "IDENTITY v3/pool/0/4-") {
		t.Fatalf("a chunk of an owner without VOLUME: %s", c)
	}
	vputVolume(t, m2, "v3", vid("v3", 1)) // an owner VOLUME of another id
	if c := classes(run(t, m2, VerifyOptions{Depth: DepthFull})); !strings.HasPrefix(c, "IDENTITY v3/pool/0/4-") {
		t.Fatalf("a chunk of an owner with another id: %s", c)
	}
	vputVolume(t, m2, "v3", vid("v3", 0))
	if r := run(t, m2, VerifyOptions{Depth: DepthFull}); len(r.Findings) != 0 {
		t.Fatalf("matching owner id: %s", classes(r))
	}
	_ = m
}

func TestVerifyReadErrorIsReturned(t *testing.T) {
	m, _ := newVol(t)
	m.readErr = chunkPath(7, "bbbb")
	if _, err := Verify(context.Background(), vbase, "v1", m.fetch, VerifyOptions{}); err == nil {
		t.Fatal("a read failure must be returned, not judged")
	}
}
