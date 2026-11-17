package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func mapDescriptorV3(t *testing.T) descriptorV3 {
	t.Helper()
	d, e := decodeSlotV3(readSlotVector(t, "slot-ref"), nil)
	if e != nil {
		t.Fatal(e)
	}
	for i := range d.UUID {
		d.UUID[i] = byte(i)
		d.MapLineage[i] = byte(i + 16)
	}
	d.VolumeSize = 4096
	d.BlockSize = 4096
	d.MapGeneration = 1
	d.MapCoveredCheckpoint = 7
	d.W = 7
	d.CheckpointLSN = 7
	return d
}
func TestChunkLSNMapV3_CanonicalVectors(t *testing.T) {
	d := mapDescriptorV3(t)
	p, e := encodeMapPageV3(d, 1, 0, []chunkLSNEntryV3{{LSN: 7}})
	if e != nil {
		t.Fatal(e)
	}
	want, e := os.ReadFile("testdata/format_v3/reference-page.bin")
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(p, want) {
		t.Fatal("page differs from independent vector")
	}
	root, e := encodeMapRootV3(d, 1, 1, 7, []mapPageRefV3{{Offset: 4096, Hash: mapDigestV3(mapPageDomainV3, p[:4064])}})
	if e != nil {
		t.Fatal(e)
	}
	want, e = os.ReadFile("testdata/format_v3/reference-root.bin")
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(root, want) {
		t.Fatal("root differs from independent vector")
	}
	d.MapRootHash = mapDigestV3(mapRootDomainV3, root[:128])
	d.MapRootLocator = 0
	f, e := os.Create(filepath.Join(t.TempDir(), "map"))
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if _, e = f.WriteAt(root, 0); e != nil {
		t.Fatal(e)
	}
	if _, e = f.WriteAt(p, 4096); e != nil {
		t.Fatal(e)
	}
	entries, _, e := readChunkLSNMapV3(f, d, 1)
	if e != nil || len(entries) != 1 || entries[0].LSN != 7 {
		t.Fatalf("read=%v %v", entries, e)
	}
}
func TestChunkLSNMapV3_PersistReloadAndReuse(t *testing.T) {
	d := mapDescriptorV3(t)
	d.VolumeSize = 251 * 4096
	path := filepath.Join(t.TempDir(), "map")
	m, e := createChunkLSNMapV3(path, d, 1)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	entries := make([]chunkLSNEntryV3, 251)
	entries[0].LSN = 7
	entries[250].Unknown = true
	a, e := m.Prepare(1, 7, entries, nil)
	if e != nil {
		t.Fatal(e)
	}
	entries[250] = chunkLSNEntryV3{LSN: 8}
	b, e := m.Prepare(2, 8, entries, a)
	if e != nil {
		t.Fatal(e)
	}
	if a.refs[0] != b.refs[0] || a.refs[1] == b.refs[1] {
		t.Fatal("COW did not retain precisely the unchanged page")
	}
	next := b.descriptor(d)
	next.W = 8
	got, _, e := readChunkLSNMapV3(m.fd, next, 1)
	if e != nil || len(got) != 251 || got[250].LSN != 8 {
		t.Fatalf("reload=%v %v", got, e)
	}
	old, _, e := readChunkLSNMapV3(m.fd, a.descriptor(d), 1)
	if e != nil || !old[250].Unknown {
		t.Fatalf("old generation lost: %v", e)
	}
	foreign := next
	foreign.UUID[0] ^= 1
	if _, _, e := readChunkLSNMapV3(m.fd, foreign, 1); e == nil {
		t.Fatal("foreign identity accepted")
	}
}
func TestChunkLSNMapV3_CorruptionAndBounds(t *testing.T) {
	for _, kind := range []string{"page", "coverage", "padding", "overlap", "count", "reserved"} {
		t.Run(kind, func(t *testing.T) {
			d := mapDescriptorV3(t)
			m, e := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 1)
			if e != nil {
				t.Fatal(e)
			}
			defer m.Close()
			c, e := m.Prepare(1, 7, []chunkLSNEntryV3{{LSN: 7}}, nil)
			if e != nil {
				t.Fatal(e)
			}
			d = c.descriptor(d)
			switch kind {
			case "page":
				_, e = m.fd.WriteAt([]byte{9}, int64(c.refs[0].Offset+64))
			case "coverage":
				d.MapCoveredCheckpoint = 6
			case "padding":
				_, e = m.fd.WriteAt([]byte{1}, int64(c.rootOffset+200))
			case "overlap":
				d.MapRootLocator = c.refs[0].Offset
			case "count":
				d.VolumeSize = 8192
			case "reserved":
				_, e = m.fd.WriteAt([]byte{1}, int64(c.refs[0].Offset+60))
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, _, e := readChunkLSNMapV3(m.fd, d, 1); e == nil {
				t.Fatalf("%s accepted", kind)
			}
		})
	}
}
func TestChunkLSNMapV3_FailureDoesNotPublishOrReuse(t *testing.T) {
	for _, stage := range []string{"page_sync", "root_sync", "directory"} {
		t.Run(stage, func(t *testing.T) {
			d := mapDescriptorV3(t)
			m, e := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 1)
			if e != nil {
				t.Fatal(e)
			}
			defer m.Close()
			boom := errors.New("injected durability failure")
			calls := 0
			m.syncFile = func() error {
				calls++
				if (stage == "page_sync" && calls == 1) || (stage == "root_sync" && calls == 2) {
					return boom
				}
				return m.fd.Sync()
			}
			m.syncDirectory = func() error {
				if stage == "directory" {
					return boom
				}
				return nil
			}
			if c, e := m.Prepare(1, 7, []chunkLSNEntryV3{{Unknown: true}}, nil); !errors.Is(e, boom) || c != nil {
				t.Fatalf("candidate escaped failed persistence: %v %v", c, e)
			}
			before, _ := m.fd.Stat()
			if _, e := m.Prepare(2, 7, []chunkLSNEntryV3{{Unknown: true}}, nil); e == nil {
				t.Fatal("failed sidecar reused")
			}
			after, _ := m.fd.Stat()
			if before.Size() != after.Size() {
				t.Fatal("retry wrote after failure")
			}
		})
	}
}

func TestChunkLSNMapV3_OrderOwnedOpenAndClippedPage(t *testing.T) {
	d := mapDescriptorV3(t)
	d.VolumeSize = 501 * 4096
	path := filepath.Join(t.TempDir(), "map")
	m, e := createChunkLSNMapV3(path, d, 2)
	if e != nil {
		t.Fatal(e)
	}
	if other, e := createChunkLSNMapV3(path, d, 2); e == nil {
		other.Close()
		t.Fatal("overwrote owned sidecar")
	}
	var stages []string
	m.syncFile = func() error {
		info, err := m.fd.Stat()
		if err != nil {
			return err
		}
		want := int64(8192)
		if len(stages) == 1 {
			want = 12288
		}
		if info.Size() != want {
			t.Errorf("sync boundary file size=%d want %d", info.Size(), want)
		}
		stages = append(stages, "file")
		return m.fd.Sync()
	}
	m.syncDirectory = func() error { stages = append(stages, "directory"); return nil }
	es := make([]chunkLSNEntryV3, 251)
	es[250] = chunkLSNEntryV3{LSN: 7, Unknown: true}
	c, e := m.Prepare(1, 7, es, nil)
	if e != nil {
		t.Fatal(e)
	}
	if fmt.Sprint(stages) != "[file file directory]" {
		t.Fatalf("durability order=%v", stages)
	}
	next := c.descriptor(d)
	if e = m.Close(); e != nil {
		t.Fatal(e)
	}
	reopened, loaded, e := openChunkLSNMapV3(path, next, 2)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	if len(loaded.entries) != 251 || !loaded.entries[250].Unknown {
		t.Fatal("clipped final chunk lost")
	}
	// The last page has precisely one entry even though the last chunk is clipped.
	if loaded.refs[1].Offset == loaded.refs[0].Offset {
		t.Fatal("pages aliased")
	}
	if _, e := reopened.Prepare(1, 7, es, loaded); e == nil {
		t.Fatal("generation reuse accepted")
	}
	foreign := next
	foreign.MapLineage[0] ^= 1
	if f, _, e := openChunkLSNMapV3(path, foreign, 2); e == nil {
		f.Close()
		t.Fatal("foreign lineage opened")
	}
	if f, _, e := openChunkLSNMapV3(path+"missing", next, 2); e == nil {
		f.Close()
		t.Fatal("missing metadata became clean")
	}
}
func TestChunkLSNMapV3_RejectsRehashedMalformedRecords(t *testing.T) {
	for _, kind := range []string{"page_generation", "state", "reserved", "unused", "page_domain", "root_domain", "root_padding", "overlap", "duplicate", "huge_count"} {
		t.Run(kind, func(t *testing.T) {
			d := mapDescriptorV3(t)
			d.VolumeSize = 251 * 4096
			m, e := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 1)
			if e != nil {
				t.Fatal(e)
			}
			defer m.Close()
			c, e := m.Prepare(1, 7, make([]chunkLSNEntryV3, 251), nil)
			if e != nil {
				t.Fatal(e)
			}
			d = c.descriptor(d)
			shape, _ := mapShapeForV3(d, 1)
			root := make([]byte, shape.aligned)
			if _, e = m.fd.ReadAt(root, int64(c.rootOffset)); e != nil {
				t.Fatal(e)
			}
			le := binary.LittleEndian
			page := make([]byte, 4096)
			if _, e = m.fd.ReadAt(page, int64(c.refs[1].Offset)); e != nil {
				t.Fatal(e)
			}
			switch kind {
			case "page_generation":
				le.PutUint64(page[40:], 2)
			case "state":
				page[72] = 2
			case "reserved":
				page[60] = 1
			case "unused":
				page[80] = 1
			case "page_domain":
			case "root_domain":
			case "root_padding":
				root[shape.raw] = 1
			case "overlap":
				le.PutUint64(root[88:], c.rootOffset)
			case "duplicate":
				copy(root[128:168], root[88:128])
			case "huge_count":
				le.PutUint64(root[72:], ^uint64(0))
			}
			ph := mapDigestV3(mapPageDomainV3, page[:4064])
			if kind == "page_domain" {
				ph = mapDigestV3(mapRootDomainV3, page[:4064])
			}
			copy(page[4064:], ph[:])
			copy(root[136:168], ph[:])
			if _, e = m.fd.WriteAt(page, int64(c.refs[1].Offset)); e != nil {
				t.Fatal(e)
			}
			rh := mapDigestV3(mapRootDomainV3, root[:shape.raw-32])
			if kind == "root_domain" {
				rh = mapDigestV3(mapPageDomainV3, root[:shape.raw-32])
			}
			copy(root[shape.raw-32:], rh[:])
			d.MapRootHash = rh
			if _, e = m.fd.WriteAt(root, int64(c.rootOffset)); e != nil {
				t.Fatal(e)
			}
			if _, _, e := readChunkLSNMapV3(m.fd, d, 1); e == nil {
				t.Fatalf("rehashed %s accepted", kind)
			}
		})
	}
}

func TestChunkLSNMapV3_CounterAndGeometryRefusal(t *testing.T) {
	d := mapDescriptorV3(t)
	if _, e := encodeMapPageV3(d, ^uint64(0), 0, []chunkLSNEntryV3{{}}); !errors.Is(e, ErrCounterExhausted) {
		t.Fatalf("page counter=%v", e)
	}
	if _, e := encodeMapRootV3(d, 1, 1, ^uint64(0), []mapPageRefV3{{}}); !errors.Is(e, ErrCounterExhausted) {
		t.Fatalf("root counter=%v", e)
	}
	m, e := createChunkLSNMapV3(filepath.Join(t.TempDir(), "map"), d, 1)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	if _, e = m.Prepare(^uint64(0), 7, []chunkLSNEntryV3{{}}, nil); !errors.Is(e, ErrCounterExhausted) {
		t.Fatalf("prepare counter=%v", e)
	}
	stat, _ := m.fd.Stat()
	if stat.Size() != 0 {
		t.Fatal("invalid counter wrote bytes")
	}
	if _, e = mapShapeForV3(d, 0); e == nil {
		t.Fatal("zero chunk geometry accepted")
	}
	d.VolumeSize++
	if _, e = mapShapeForV3(d, 1); e == nil {
		t.Fatal("nonintegral block geometry accepted")
	}
}
