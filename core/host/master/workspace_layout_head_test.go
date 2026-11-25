package master

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
)

type workspaceHeadBlobs struct{}

func (workspaceHeadBlobs) Put(context.Context, []byte) (string, error) {
	return "", errors.New("HEAD reader must not upload")
}
func (workspaceHeadBlobs) Get(context.Context, string) ([]byte, error) {
	return nil, errors.New("HEAD reader must not hydrate")
}

// headFiler counts HEAD lookups and can fail them (a filer that is down).
type headFiler struct {
	*chunkstore.MemFiler
	lookups atomic.Int32
	fail    error
}

func (f *headFiler) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	f.lookups.Add(1)
	if f.fail != nil {
		return nil, f.fail
	}
	return f.MemFiler.LookupEntry(ctx, dir, name)
}

func workspaceHeadCatalog(t *testing.T) (*chunkstore.BCatalog, *headFiler, chunkstore.VolumeRef) {
	t.Helper()
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	f := &headFiler{MemFiler: chunkstore.NewMemFiler()}
	cat, err := chunkstore.NewBCatalog(f, workspaceHeadBlobs{}, "/chunks")
	if err != nil {
		t.Fatal(err)
	}
	return cat, f, chunkstore.VolumeRef{Name: "v", ID: id}
}

// The master's one HEAD reader reads through the catalog: identity from the
// master's own record, the proof and the head time from that one entry.
func TestWorkspaceLayoutHEADUsesTheMasterIdentity(t *testing.T) {
	ctx := context.Background()
	cat, f, v := workspaceHeadCatalog(t)
	s, err := cat.Store(v)
	if err != nil {
		t.Fatal(err)
	}
	rn := layout.RootName{Hash: layout.Sum([]byte("committed root"))}
	h := layout.Head{VolumeID: v.ID, Epoch: 3, Holder: "node/r", ClaimID: "claim", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn, Position: 42, State: layout.StateDetached}
	if err := s.CreateHead(ctx, layout.Unclaimed(v.ID, nil)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateHead(ctx, 1, h); err != nil {
		t.Fatal(err)
	}
	e, err := f.MemFiler.LookupEntry(ctx, "/chunks/"+v.Dir(), "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	e.Attributes = &filerpb.FuseAttributes{Mtime: 100}
	if err := f.MemFiler.UpdateEntry(ctx, "/chunks/"+v.Dir(), e, nil); err != nil {
		t.Fatal(err)
	}
	read, err := NewLayoutWorkspaceHEADReader(cat, func(_ context.Context, name string) (chunkstore.VolumeRef, error) {
		if name != "v" {
			return chunkstore.VolumeRef{}, errors.New("not registered")
		}
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	f.lookups.Store(0)
	got, err := read(ctx, "v")
	if err != nil || !got.Detached || got.Gen != 2 || got.Epoch != 3 || got.Holder != "node/r" || got.RemoteLSN != 42 || got.FilerMtimeNS != 100e9 {
		t.Fatalf("proof: %+v %v", got, err)
	}
	if _, err := read(ctx, "other"); err == nil || f.lookups.Load() != 1 {
		t.Fatalf("resolver failure contacted the filer: %d lookups, %v", f.lookups.Load(), err)
	}
	if _, err := NewLayoutWorkspaceHEADReader(cat, nil); err == nil {
		t.Fatal("nil identity resolver accepted")
	}
	if _, err := NewLayoutWorkspaceHEADReader(nil, func(context.Context, string) (chunkstore.VolumeRef, error) { return v, nil }); err == nil {
		t.Fatal("nil catalog accepted")
	}
	renamed, _ := NewLayoutWorkspaceHEADReader(cat, func(context.Context, string) (chunkstore.VolumeRef, error) {
		return chunkstore.VolumeRef{Name: "w", ID: v.ID}, nil
	})
	if _, err := renamed(ctx, "v"); !errors.Is(err, layout.ErrIdentity) {
		t.Fatalf("a resolver naming another volume: %v; want ErrIdentity", err)
	}
}

// Every HEAD entry the shared decoder refuses is refused by the master too;
// the states map onto the proof (deleting, unclaimed, detached or live) and
// an entry without a modification time reports the head time unknown.
func TestWorkspaceLayoutHEADRefusalsAndStates(t *testing.T) {
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	rn := layout.RootName{Hash: layout.Sum([]byte("root"))}
	base := layout.Head{VolumeID: id, Epoch: 3, Holder: "node/r", ClaimID: "claim", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn, Position: 42, State: layout.StateDetached}
	for _, tc := range []struct {
		name     string
		state    string
		alter    func(*filerpb.Entry)
		absent   bool
		down     bool
		valid    bool
		sentinel error
	}{
		{name: "detached", valid: true},
		{name: "live", state: "live", valid: true},
		{name: "unknown mtime", valid: true, alter: func(e *filerpb.Entry) { e.Attributes = nil }},
		{name: "unclaimed", state: "unclaimed", sentinel: authority.ErrWorkspaceHeadUnclaimed},
		{name: "deleted", state: layout.StateDeleted, sentinel: authority.ErrWorkspaceHeadDeleting},
		{name: "missing generation", alter: func(e *filerpb.Entry) { delete(e.Extended, "swb.layout.gen") }},
		{name: "zero generation", alter: func(e *filerpb.Entry) { e.Extended["swb.layout.gen"] = []byte("0") }},
		{name: "noncanonical generation", alter: func(e *filerpb.Entry) { e.Extended["swb.layout.gen"] = []byte("09") }},
		{name: "wrong volume attribute", alter: func(e *filerpb.Entry) { e.Extended["swb.layout.volume"] = []byte("ffffffffffffffffffffffffffffffff") }},
		{name: "wrong body volume", alter: func(e *filerpb.Entry) {
			e.Content = bytes.ReplaceAll(e.Content, []byte(id.String()), []byte("ffffffffffffffffffffffffffffffff"))
		}},
		{name: "missing kind", alter: func(e *filerpb.Entry) { delete(e.Extended, "swb.layout.kind") }},
		{name: "missing claim", alter: func(e *filerpb.Entry) { e.Content = bytes.ReplaceAll(e.Content, []byte(`,"claim_id":"claim"`), nil) }},
		{name: "detached activity", alter: func(e *filerpb.Entry) {
			e.Content = bytes.ReplaceAll(e.Content, []byte(`"kind":"idle"`), []byte(`"kind":"snap","session":"s"`))
		}},
		{name: "chunked HEAD", alter: func(e *filerpb.Entry) { e.Chunks = []*filerpb.FileChunk{{FileId: "x"}} }},
		{name: "directory HEAD", alter: func(e *filerpb.Entry) { e.IsDirectory = true }},
		{name: "legacy content", alter: func(e *filerpb.Entry) {
			e.Content = []byte(`{"format":1,"gen":9,"epoch":3,"holder":"node/r","remote_lsn":42}`)
		}},
		{name: "oversized inline HEAD", alter: func(e *filerpb.Entry) { e.Content = bytes.Repeat([]byte("x"), layout.MaxHeadBytes+1) }, sentinel: layout.ErrHeadTooLarge},
		{name: "not found", absent: true, sentinel: layout.ErrHeadAbsent},
		{name: "unavailable", down: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cat, f, v := workspaceHeadCatalog(t)
			h := base
			switch tc.state {
			case "live":
				h.State = layout.StateLive
			case "unclaimed":
				h = layout.Unclaimed(id, nil)
			case layout.StateDeleted:
				h = layout.Unclaimed(id, nil)
				h.State = layout.StateDeleted
			}
			raw, err := h.Encode()
			if err != nil {
				t.Fatal(err)
			}
			e := &filerpb.Entry{Name: "HEAD", Content: raw, Attributes: &filerpb.FuseAttributes{Mtime: 100},
				Extended: map[string][]byte{"swb.layout.gen": []byte("9"), "swb.layout.kind": []byte("head"), "swb.layout.volume": []byte(id.String())}}
			if tc.alter != nil {
				tc.alter(e)
			}
			if !tc.absent {
				if err := f.MemFiler.CreateEntry(ctx, "/chunks/"+v.Dir(), e, true); err != nil {
					t.Fatal(err)
				}
			}
			if tc.down {
				f.fail = errors.New("filer unavailable")
			}
			read, err := NewLayoutWorkspaceHEADReader(cat, func(context.Context, string) (chunkstore.VolumeRef, error) { return v, nil })
			if err != nil {
				t.Fatal(err)
			}
			got, err := read(ctx, "v")
			wantTime := int64(100e9)
			if tc.name == "unknown mtime" {
				wantTime = 0
			}
			switch {
			case tc.valid:
				if err != nil || got.Gen != 9 || got.Detached != (tc.state != "live") || got.FilerMtimeNS != wantTime {
					t.Fatalf("valid proof: %+v %v", got, err)
				}
			case tc.sentinel != nil:
				if !errors.Is(err, tc.sentinel) {
					t.Fatalf("sentinel: %+v %v", got, err)
				}
			case err == nil:
				t.Fatalf("invalid proof accepted: %+v", got)
			}
		})
	}
}
