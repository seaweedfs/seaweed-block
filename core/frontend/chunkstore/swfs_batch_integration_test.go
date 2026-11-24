//go:build chunkstore_integration

package chunkstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

const jwtSecurity = `[jwt.signing]
key = "batch-assign-integration"
expires_after_seconds = 60
`

// arithmeticFid is the fid key+i with the base's cookie: a different needle
// id, which the base's token does not cover.
func arithmeticFid(t *testing.T, base string, i int) string {
	t.Helper()
	vid, kc, _ := strings.Cut(base, ",")
	key, err := strconv.ParseUint(kc[:len(kc)-8], 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s,%x%s", vid, key+uint64(i), kc[len(kc)-8:])
}

// With JWT on, every offset of a reservation is written with the base's
// token in the native "<base>_<i>" form and reads back as itself; the same
// token on the arithmetic fid key+i is refused.
func TestIntegrationBatch_JWTNativeOffsets(t *testing.T) {
	c := startWeedWith(t, jwtSecurity)
	cl := c.client(t, "/swblock/vols/v1")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const n = 5
	r, err := cl.reserve(ctx, n, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if r.auth == "" {
		t.Fatal("assign with JWT on returned no token")
	}
	if r.count != n {
		t.Fatalf("reserved %d of %d", r.count, n)
	}
	data := func(i int) []byte { return bytes.Repeat([]byte(fmt.Sprintf("offset-%d|", i)), 400) }
	for _, i := range []int{0, 2, n - 1} { // first, middle, last
		if err := cl.putRange(ctx, r, i, data(i)); err != nil {
			t.Fatalf("offset %d: %v", i, err)
		}
	}
	for _, i := range []int{0, 2, n - 1} {
		fid, _ := r.fid(i)
		got, err := cl.Get(ctx, fid)
		if err != nil || !bytes.Equal(got, data(i)) {
			t.Fatalf("offset %d (%s) read back %d bytes, %v", i, fid, len(got), err)
		}
	}
	arith := arithmeticFid(t, r.base, 1)
	err = cl.upload(ctx, r.url, arith, r.auth, r.fsync, data(1))
	if !errors.Is(err, errRangeAuth) {
		t.Fatalf("upload to the arithmetic fid %s with the base token: %v; want a rejected token", arith, err)
	}
}

// A round of more than one group writes through real assigns, binds every
// key, and reads every key back; it takes one assign per group.
func TestIntegrationBatch_RoundBindsAndReadsBack(t *testing.T) {
	c := startWeedWith(t, jwtSecurity)
	cl := c.client(t, "/swblock/vols/v1")
	r, err := NewChunkRemote(cl, cl, "/swblock/vols/v1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const n = 130 // three groups
	rd := r.BeginPutRound(n, 4096)
	for i := 0; i < n; i++ {
		if err := rd.Put(ctx, i, roundKey(i), []byte(roundKey(i))); err != nil {
			t.Fatalf("object %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		got, err := r.Get(ctx, roundKey(i))
		if err != nil || !bytes.Equal(got, []byte(roundKey(i))) {
			t.Fatalf("key %d read back %q, %v", i, got, err)
		}
	}
	if a := r.AssignCounts(); a.Initial != 3 || a.Extra() != 0 {
		t.Fatalf("assign counts %+v; want 3 initial", a)
	}
	// A range shares its base's cookie: n distinct needles from 3 ranges.
	fids, cookies := map[string]bool{}, map[string]bool{}
	for i := 0; i < n; i++ {
		fid := r.fids[roundKey(i)]
		fids[fid], cookies[fid[len(fid)-8:]] = true, true
	}
	if len(fids) != n || len(cookies) != 3 {
		t.Fatalf("%d needles with %d cookies; want %d from 3 ranges", len(fids), len(cookies), n)
	}
}
