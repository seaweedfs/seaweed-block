package chunkstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

// batchVolume is a volume server that records every upload by its path (the
// fid as sent), with the Authorization header and query.
type batchVolume struct {
	mu      sync.Mutex
	stored  map[string][]byte
	auth    map[string]string
	query   map[string]string
	refuse  func(fid string) (status int, header string) // non-zero status: refuse
	srv     *httptest.Server
	uploads []string
}

func newBatchVolume(t *testing.T) *batchVolume {
	v := &batchVolume{stored: map[string][]byte{}, auth: map[string]string{}, query: map[string]string{}}
	v.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fid := strings.TrimPrefix(r.URL.Path, "/")
		b, _ := io.ReadAll(r.Body)
		v.mu.Lock()
		defer v.mu.Unlock()
		if r.Method != http.MethodPost {
			if d, ok := v.stored[fid]; ok {
				_, _ = w.Write(d)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			return
		}
		v.uploads = append(v.uploads, fid)
		if v.refuse != nil {
			if st, h := v.refuse(fid); st != 0 {
				if h != "" {
					w.Header().Set("X-Seaweed-Write-Outcome", h)
				}
				w.WriteHeader(st)
				_, _ = w.Write([]byte(`{"error":"refused"}`))
				return
			}
		}
		v.stored[fid] = b
		v.auth[fid] = r.Header.Get("Authorization")
		v.query[fid] = r.URL.RawQuery
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"size":1}`))
	}))
	t.Cleanup(v.srv.Close)
	return v
}

func (v *batchVolume) host() string { return strings.TrimPrefix(v.srv.URL, "http://") }

func (v *batchVolume) snapshot() (uploads []string, stored map[string][]byte, auth map[string]string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	stored, auth = map[string][]byte{}, map[string]string{}
	for k, d := range v.stored {
		stored[k] = d
	}
	for k, a := range v.auth {
		auth[k] = a
	}
	return append([]string(nil), v.uploads...), stored, auth
}

// One reservation of n ids is addressed in SeaweedFS's native form: offset 0
// is the base fid, offset i is "<base>_<i>"; every offset carries the base's
// token and the assign's fsync policy.
func TestSWFSRange_NativeOffsetFids(t *testing.T) {
	vol := newBatchVolume(t)
	f := &fakeFilerGRPC{assignResp: &filerpb.AssignVolumeResponse{FileId: "7,01abcdef12", Count: 3, Auth: "tok", Fsync: true, Location: &filerpb.Location{Url: vol.host()}}}
	c := startFakeFiler(t, f)
	ctx := context.Background()
	r, err := c.reserve(ctx, 3, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if f.lastAssign.Count != 3 || f.lastAssign.ExpectedDataSize != 4096 || f.lastAssign.Collection != "swblock" || f.lastAssign.Path != "/swblock/vols/v1" {
		t.Fatalf("assign request %+v; want count 3 and the per-object expected size", f.lastAssign)
	}
	want := []string{"7,01abcdef12", "7,01abcdef12_1", "7,01abcdef12_2"}
	for i, w := range want {
		fid, err := r.fid(i)
		if err != nil || fid != w {
			t.Fatalf("offset %d: fid %q %v, want %q", i, fid, err, w)
		}
		if err := c.putRange(ctx, r, i, []byte(fmt.Sprintf("obj%d", i))); err != nil {
			t.Fatalf("offset %d: %v", i, err)
		}
	}
	_, stored, auth := vol.snapshot()
	vol.mu.Lock()
	query := vol.query
	vol.mu.Unlock()
	for i, w := range want {
		if !bytes.Equal(stored[w], []byte(fmt.Sprintf("obj%d", i))) {
			t.Fatalf("offset %d stored %q at %s", i, stored[w], w)
		}
		if auth[w] != "BEARER tok" {
			t.Fatalf("offset %d authorization %q; want the base token", i, auth[w])
		}
		if q := query[w]; q != "fsync=true" {
			t.Fatalf("offset %d query %q; want the assign's fsync policy", i, q)
		}
	}
	for _, i := range []int{-1, 3, 4} {
		if fid, err := r.fid(i); err == nil {
			t.Fatalf("offset %d outside a range of 3 gave fid %q", i, fid)
		}
	}
}

// A reservation reply is checked before any id in it is used.
func TestSWFSRange_ReserveValidates(t *testing.T) {
	loc := &filerpb.Location{Url: "127.0.0.1:1"}
	for _, c := range []struct {
		name string
		n    int
		resp *filerpb.AssignVolumeResponse
		ok   bool
		want int
	}{
		{"full grant", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 4, Location: loc}, true, 4},
		{"short grant", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 2, Location: loc}, true, 2},
		{"zero count", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 0, Location: loc}, false, 0},
		{"more than asked", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 5, Location: loc}, false, 0},
		{"no location", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 4}, false, 0},
		{"no fid", 4, &filerpb.AssignVolumeResponse{Count: 4, Location: loc}, false, 0},
		{"fid with a suffix", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e_1", Count: 4, Location: loc}, false, 0},
		{"fid without a comma", 4, &filerpb.AssignVolumeResponse{FileId: "30a0b0c0d0e", Count: 4, Location: loc}, false, 0},
		{"non-numeric volume", 4, &filerpb.AssignVolumeResponse{FileId: "x,0a0b0c0d0e", Count: 4, Location: loc}, false, 0},
		{"non-hex key", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0dzz", Count: 4, Location: loc}, false, 0},
		{"key too short for a cookie", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d", Count: 4, Location: loc}, false, 0},
		{"assign error", 4, &filerpb.AssignVolumeResponse{Error: "no free volumes"}, false, 0},
		{"location with a scheme", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 4, Location: &filerpb.Location{Url: "http://127.0.0.1:1"}}, false, 0},
		{"location without a port", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 4, Location: &filerpb.Location{Url: "127.0.0.1"}}, false, 0},
		{"location with a bad port", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 4, Location: &filerpb.Location{Url: "127.0.0.1:x"}}, false, 0},
		{"location with port 0", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 4, Location: &filerpb.Location{Url: "127.0.0.1:0"}}, false, 0},
		{"location with a path", 4, &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 4, Location: &filerpb.Location{Url: "a/b:1"}}, false, 0},
		{"widest key", 4, &filerpb.AssignVolumeResponse{FileId: "3,0123456789abcdef0a0b0c0d", Count: 4, Location: loc}, true, 4},
		{"key of 17 digits (leading zero)", 4, &filerpb.AssignVolumeResponse{FileId: "3,00123456789abcdef0a0b0c0d", Count: 4, Location: loc}, false, 0},
		{"key wider than 64 bits", 4, &filerpb.AssignVolumeResponse{FileId: "3,10123456789abcdef0a0b0c0d", Count: 4, Location: loc}, false, 0},
		{"last offset fits", 2, &filerpb.AssignVolumeResponse{FileId: "3,fffffffffffffffe0a0b0c0d", Count: 2, Location: loc}, true, 2},
		{"last offset overflows the key", 3, &filerpb.AssignVolumeResponse{FileId: "3,fffffffffffffffe0a0b0c0d", Count: 3, Location: loc}, false, 0},
		{"volume id wider than 32 bits", 4, &filerpb.AssignVolumeResponse{FileId: "4294967296,0a0b0c0d0e", Count: 4, Location: loc}, false, 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			s := startFakeFiler(t, &fakeFilerGRPC{assignResp: c.resp})
			r, err := s.reserve(context.Background(), c.n, 4096)
			if c.ok != (err == nil) {
				t.Fatalf("reserve: %+v %v; want ok=%v", r, err, c.ok)
			}
			if c.ok && r.count != c.want {
				t.Fatalf("count %d, want %d", r.count, c.want)
			}
		})
	}
	for _, n := range []int{1, 256, 4096} {
		s := startFakeFiler(t, &fakeFilerGRPC{assignResp: &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: int32(n), Location: loc}})
		r, err := s.reserve(context.Background(), n, 4096)
		if err != nil || r.count != n {
			t.Fatalf("reserve %d: %+v %v", n, r, err)
		}
		if _, err := r.fid(n - 1); err != nil {
			t.Fatalf("reserve %d: last offset: %v", n, err)
		}
		if fid, err := r.fid(n); err == nil {
			t.Fatalf("reserve %d: offset %d gave %q", n, n, fid)
		}
	}
	s := startFakeFiler(t, &fakeFilerGRPC{assignResp: &filerpb.AssignVolumeResponse{FileId: "3,0a0b0c0d0e", Count: 1, Location: loc}})
	for _, n := range []int{0, -1, maxRangeCount + 1} {
		if _, err := s.reserve(context.Background(), n, 4096); err == nil {
			t.Fatalf("reserve of %d ids accepted", n)
		}
	}
}

// A read-only volume (409, or a write refused before append) is a definite
// refusal: nothing was appended, so a fresh range may take the object. A 401
// is terminal. Anything else is not classified (the caller retries the same
// slot).
func TestSWFSRange_PutClassifiesRefusals(t *testing.T) {
	vol := newBatchVolume(t)
	f := &fakeFilerGRPC{assignResp: &filerpb.AssignVolumeResponse{FileId: "7,01abcdef12", Count: 4, Location: &filerpb.Location{Url: vol.host()}}}
	c := startFakeFiler(t, f)
	ctx := context.Background()
	r, err := c.reserve(ctx, 4, 4096)
	if err != nil {
		t.Fatal(err)
	}
	vol.mu.Lock()
	vol.refuse = func(fid string) (int, string) {
		switch fid {
		case "7,01abcdef12":
			return http.StatusConflict, ""
		case "7,01abcdef12_1":
			return http.StatusInternalServerError, "refused-before-append"
		case "7,01abcdef12_2":
			return http.StatusUnauthorized, ""
		case "7,01abcdef12_3":
			return http.StatusInternalServerError, ""
		}
		return 0, ""
	}
	vol.mu.Unlock()
	for i, want := range []error{errRangeRefused, errRangeRefused, errRangeAuth, nil} {
		err := c.putRange(ctx, r, i, []byte("x"))
		if err == nil {
			t.Fatalf("offset %d: refused upload reported success", i)
		}
		for _, e := range []error{errRangeRefused, errRangeAuth} {
			if errors.Is(err, e) != (e == want) {
				t.Fatalf("offset %d: %v; classified as %v=%v, want %v", i, err, e, errors.Is(err, e), want)
			}
		}
	}
}

// jwtWithExp returns an unsigned token whose payload names exp (unix seconds).
func jwtWithExp(exp int64) string {
	enc := base64.RawURLEncoding
	return enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." +
		enc.EncodeToString([]byte(fmt.Sprintf(`{"exp":%d,"fid":"7,01abcdef12"}`, exp))) + ".sig"
}

// The send deadline of a range is its token's stated expiry less a skew; a
// token with no readable expiry gives no deadline.
func TestSWFSRange_TokenSendDeadline(t *testing.T) {
	now := time.Unix(1700000000, 0)
	for _, c := range []struct {
		name string
		auth string
		ok   bool
		want time.Time
	}{
		{"no token", "", false, time.Time{}},
		{"opaque token", "tok", false, time.Time{}},
		{"no exp claim", base64.RawURLEncoding.EncodeToString([]byte(`{}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(`{"fid":"x"}`)) + ".s", false, time.Time{}},
		{"exp", jwtWithExp(now.Unix() + 10), true, now.Add(10*time.Second - jwtSendSkew)},
	} {
		got, ok := blobRange{auth: c.auth}.sendDeadline()
		if ok != c.ok || !got.Equal(c.want) {
			t.Errorf("%s: deadline %v %v, want %v %v", c.name, got, ok, c.want, c.ok)
		}
	}
}
