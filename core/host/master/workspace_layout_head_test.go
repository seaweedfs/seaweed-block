package master

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

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

func TestWorkspaceLayoutHEADUsesTheMasterIdentity(t *testing.T) {
	ctx := context.Background()
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	v := chunkstore.VolumeRef{Name: "v", ID: id}
	f := chunkstore.NewMemFiler()
	s, err := chunkstore.NewLayoutStore(f, workspaceHeadBlobs{}, "/chunks/"+v.Dir(), id)
	if err != nil {
		t.Fatal(err)
	}
	rn := layout.RootName{Hash: layout.Sum([]byte("committed root"))}
	h := layout.Head{VolumeID: id, Epoch: 3, Holder: "node/r", ClaimID: "claim", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn, Position: 42, State: layout.StateDetached}
	if err := s.CreateHead(ctx, layout.Unclaimed(id, nil)); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateHead(ctx, 1, h); err != nil {
		t.Fatal(err)
	}
	e, err := f.LookupEntry(ctx, "/chunks/"+v.Dir(), "HEAD")
	if err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(100, 0).UTC()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Last-Modified", time.Unix(200, 0).UTC().Format(http.TimeFormat))
		if r.URL.Path != "/chunks/"+v.Dir()+"/HEAD" || r.URL.RawQuery != "metadata=true" {
			t.Errorf("unbound HEAD path: %s", r.URL)
		}
		_ = json.NewEncoder(w).Encode(struct {
			FullPath string
			Content  []byte
			Extended map[string][]byte
			Mtime    time.Time
			Chunks   []*filerpb.FileChunk
		}{r.URL.Path, e.Content, e.Extended, mtime, nil})
	}))
	defer server.Close()
	read, err := NewLayoutWorkspaceHEADReader(server.URL+"/chunks", func(_ context.Context, name string) (chunkstore.VolumeRef, error) {
		if name != "v" {
			return chunkstore.VolumeRef{}, errors.New("not registered")
		}
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := read(ctx, "v")
	if err != nil || !got.Detached || got.Gen != 2 || got.Epoch != 3 || got.Holder != "node/r" || got.RemoteLSN != 42 || got.FilerMtimeNS != mtime.UnixNano() {
		t.Fatalf("proof: %+v %v", got, err)
	}
	if _, err := read(ctx, "other"); err == nil || calls.Load() != 1 {
		t.Fatalf("resolver failure contacted filer: %d %v", calls.Load(), err)
	}
	if _, err := NewLayoutWorkspaceHEADReader(server.URL, nil); err == nil {
		t.Fatal("nil identity resolver accepted")
	}
}

func TestWorkspaceLayoutHEADRefusalsAndStates(t *testing.T) {
	id, _ := layout.ParseVolumeID("00112233445566778899aabbccddeeff")
	v := chunkstore.VolumeRef{Name: "v", ID: id}
	rn := layout.RootName{Hash: layout.Sum([]byte("root"))}
	base := layout.Head{VolumeID: id, Epoch: 3, Holder: "node/r", ClaimID: "claim", Activity: layout.Activity{Kind: layout.ActIdle}, Current: &rn, Position: 42, State: layout.StateDetached}
	type response struct {
		FullPath string
		Content  []byte
		Extended map[string][]byte
		Mtime    time.Time
		Mode     uint32
		Chunks   []*filerpb.FileChunk
	}
	for _, tc := range []struct {
		name     string
		state    string
		alter    func(*response)
		status   int
		raw      string
		valid    bool
		sentinel error
	}{
		{name: "detached", valid: true},
		{name: "live", state: "live", valid: true},
		{name: "unknown mtime", valid: true, alter: func(e *response) { e.Mtime = time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{name: "unclaimed", state: "unclaimed", sentinel: authority.ErrWorkspaceHeadUnclaimed},
		{name: "deleted", state: layout.StateDeleted, sentinel: authority.ErrWorkspaceHeadDeleting},
		{name: "missing generation", alter: func(e *response) { delete(e.Extended, "swb.layout.gen") }},
		{name: "zero generation", alter: func(e *response) { e.Extended["swb.layout.gen"] = []byte("0") }},
		{name: "noncanonical generation", alter: func(e *response) { e.Extended["swb.layout.gen"] = []byte("09") }},
		{name: "wrong volume attribute", alter: func(e *response) { e.Extended["swb.layout.volume"] = []byte("ffffffffffffffffffffffffffffffff") }},
		{name: "wrong body volume", alter: func(e *response) {
			e.Content = bytes.ReplaceAll(e.Content, []byte(id.String()), []byte("ffffffffffffffffffffffffffffffff"))
		}},
		{name: "missing kind", alter: func(e *response) { delete(e.Extended, "swb.layout.kind") }},
		{name: "missing claim", alter: func(e *response) { e.Content = bytes.ReplaceAll(e.Content, []byte(`,"claim_id":"claim"`), nil) }},
		{name: "detached activity", alter: func(e *response) {
			e.Content = bytes.ReplaceAll(e.Content, []byte(`"kind":"idle"`), []byte(`"kind":"snap","session":"s"`))
		}},
		{name: "chunked HEAD", alter: func(e *response) { e.Chunks = []*filerpb.FileChunk{{FileId: "x"}} }},
		{name: "directory HEAD", alter: func(e *response) { e.Mode = 1 << 31 }},
		{name: "wrong path", alter: func(e *response) { e.FullPath = "/chunks/other/HEAD" }},
		{name: "legacy content", alter: func(e *response) {
			e.Content = []byte(`{"format":1,"gen":9,"epoch":3,"holder":"node/r","remote_lsn":42}`)
		}},
		{name: "oversized inline HEAD", alter: func(e *response) { e.Content = bytes.Repeat([]byte("x"), layout.MaxHeadBytes+1) }, sentinel: layout.ErrHeadTooLarge},
		{name: "not found", status: http.StatusNotFound},
		{name: "unavailable", status: http.StatusServiceUnavailable},
		{name: "malformed envelope", raw: "not JSON"},
		{name: "oversized envelope", raw: string(bytes.Repeat([]byte("x"), 4*layout.MaxHeadBytes+1))},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			e := response{FullPath: "/chunks/" + v.Dir() + "/HEAD", Content: raw, Extended: map[string][]byte{"swb.layout.gen": []byte("9"), "swb.layout.kind": []byte("head"), "swb.layout.volume": []byte(id.String())}}
			if tc.alter != nil {
				tc.alter(&e)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.status != 0 {
					w.WriteHeader(tc.status)
					return
				}
				if tc.raw != "" {
					_, _ = w.Write([]byte(tc.raw))
					return
				}
				_ = json.NewEncoder(w).Encode(e)
			}))
			defer server.Close()
			read, err := NewLayoutWorkspaceHEADReader(server.URL+"/chunks", func(context.Context, string) (chunkstore.VolumeRef, error) { return v, nil })
			if err != nil {
				t.Fatal(err)
			}
			got, err := read(context.Background(), "v")
			if tc.valid {
				if err != nil || got.Gen != 9 || got.Detached != (tc.state != "live") || got.FilerMtimeNS != 0 {
					t.Fatalf("valid proof: %+v %v", got, err)
				}
			} else if tc.sentinel != nil {
				if !errors.Is(err, tc.sentinel) {
					t.Fatalf("sentinel: %+v %v", got, err)
				}
			} else if err == nil {
				t.Fatalf("invalid proof accepted: %+v", got)
			}
		})
	}
}

func TestWorkspaceLayoutHEADNeverFollowsRedirects(t *testing.T) {
	var followed atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
	defer source.Close()
	read, err := NewLayoutWorkspaceHEADReader(source.URL, func(context.Context, string) (chunkstore.VolumeRef, error) {
		return chunkstore.VolumeRef{Name: "v", ID: layout.VolumeID{1}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := read(context.Background(), "v"); err == nil || followed.Load() != 0 {
		t.Fatalf("redirect followed: %d %v", followed.Load(), err)
	}
}
