package chunkstore

// Batch assign: one AssignVolume(Count = n) reserves n consecutive needle ids
// on one volume. They are addressed in SeaweedFS's native form: offset 0 is
// the base fid, offset i is "<base>_<i>". The volume server adds i to the
// needle key and checks the assign's token against the base fid, so every
// offset carries the base's token and shares its cookie. Never write such an
// id as a different fid (key+i): that one has no token.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
)

const (
	// maxRangeCount bounds one reservation.
	maxRangeCount = 1 << 16
	// jwtSendSkew: an upload is not sent within this much of its token's
	// stated expiry.
	jwtSendSkew = 2 * time.Second
)

var (
	// errRangeRefused: the volume refused the write before appending it
	// (read-only or full). Nothing was stored; a fresh range may take it.
	errRangeRefused = errors.New("chunkstore: volume refused the write before append")
	// errRangeAuth: the volume rejected the upload's token. Terminal.
	errRangeAuth = errors.New("chunkstore: volume rejected the upload token")
	// errBlobAbsent: the volume server holding a range has no needle at
	// the probed fid.
	errBlobAbsent = errors.New("chunkstore: no needle at fid")
)

// blobRange is one batch reservation: count ids starting at base, on the
// volume server at url, with the assign's token and fsync policy.
type blobRange struct {
	base  string
	count int
	url   string
	auth  string
	fsync bool
}

// fid returns the native id of offset i.
func (r blobRange) fid(i int) (string, error) {
	if i < 0 || i >= r.count {
		return "", fmt.Errorf("chunkstore: offset %d outside a range of %d ids", i, r.count)
	}
	if i == 0 {
		return r.base, nil
	}
	return r.base + "_" + strconv.Itoa(i), nil
}

// sendDeadline is the last time an upload with this range's token may be
// sent: the token's stated expiry less jwtSendSkew. The claim is read, not
// verified: it only ever stops an upload, it never authorizes one. A token
// without a readable expiry gives no deadline.
func (r blobRange) sendDeadline() (time.Time, bool) {
	parts := strings.Split(r.auth, ".")
	if len(parts) != 3 {
		return time.Time{}, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, false
	}
	var claims struct {
		Exp *int64 `json:"exp"`
	}
	if json.Unmarshal(payload, &claims) != nil || claims.Exp == nil {
		return time.Time{}, false
	}
	return time.Unix(*claims.Exp, 0).Add(-jwtSendSkew), true
}

// rangeBlobs is the optional batch capability of a blob store.
type rangeBlobs interface {
	reserve(ctx context.Context, n int, objSize uint64) (blobRange, error)
	putRange(ctx context.Context, r blobRange, i int, data []byte) error
	// probe reads offset i back from the range's volume server:
	// errBlobAbsent when that server definitely has no such needle.
	probe(ctx context.Context, r blobRange, i int) ([]byte, error)
}

var _ rangeBlobs = (*SWFS)(nil)

// validBaseFid checks "<vid>,<key><cookie>" for a range of count ids: a
// 32-bit decimal volume id, a 64-bit hex needle key (1 to 16 digits) and an
// 8-digit hex cookie, no offset suffix, and room for key + count - 1.
func validBaseFid(fid string, count int) error {
	vid, kc, ok := strings.Cut(fid, ",")
	if !ok || vid == "" {
		return fmt.Errorf("chunkstore: assign fid %q has no volume id", fid)
	}
	if _, err := strconv.ParseUint(vid, 10, 32); err != nil {
		return fmt.Errorf("chunkstore: assign fid %q: volume id: %v", fid, err)
	}
	if len(kc) <= 8 || len(kc) > 16+8 {
		return fmt.Errorf("chunkstore: assign fid %q: want a 1..16 digit key and an 8 digit cookie", fid)
	}
	for _, c := range kc {
		if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
			return fmt.Errorf("chunkstore: assign fid %q: not a plain hex key and cookie", fid)
		}
	}
	key, err := strconv.ParseUint(kc[:len(kc)-8], 16, 64)
	if err != nil {
		return fmt.Errorf("chunkstore: assign fid %q: key: %v", fid, err)
	}
	if count > 0 && key > math.MaxUint64-uint64(count-1) {
		return fmt.Errorf("chunkstore: assign fid %q: key + %d overflows", fid, count-1)
	}
	return nil
}

// validHostPort checks a volume server location: a plain host:port HTTP
// authority ([v6]:port included), with no scheme, path, user, escape,
// space or control byte, so the range's uploads can be addressed at all.
func validHostPort(loc string) error {
	for _, c := range loc {
		if c <= ' ' || c == 0x7f || strings.ContainsRune("/?#@\\", c) {
			return fmt.Errorf("chunkstore: assign location %q: not a plain host:port", loc)
		}
	}
	host, port, err := net.SplitHostPort(loc)
	if err != nil {
		return fmt.Errorf("chunkstore: assign location %q: %v", loc, err)
	}
	if host == "" {
		return fmt.Errorf("chunkstore: assign location %q: no host", loc)
	}
	if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
		return fmt.Errorf("chunkstore: assign location %q: bad port", loc)
	}
	if u, err := url.Parse("http://" + loc + "/"); err != nil || u.Host != loc {
		return fmt.Errorf("chunkstore: assign location %q: not an HTTP authority", loc)
	}
	return nil
}

// reserve asks for n ids of objSize bytes each (the master counts
// ExpectedDataSize per id). A shorter valid grant is returned as is; the
// caller reserves the rest.
func (c *SWFS) reserve(ctx context.Context, n int, objSize uint64) (blobRange, error) {
	if n <= 0 || n > maxRangeCount {
		return blobRange{}, fmt.Errorf("chunkstore: reserve %d ids: want 1..%d", n, maxRangeCount)
	}
	ar, err := c.fc.AssignVolume(ctx, &filerpb.AssignVolumeRequest{
		Count:            int32(n),
		Collection:       c.cfg.Collection,
		Replication:      c.cfg.Replication,
		Path:             c.cfg.AssignPath,
		ExpectedDataSize: objSize,
	})
	if err != nil {
		return blobRange{}, fmt.Errorf("chunkstore: assign %d: %w", n, err)
	}
	if ar.GetError() != "" {
		return blobRange{}, fmt.Errorf("chunkstore: assign %d: %s", n, ar.GetError())
	}
	if err := validHostPort(ar.GetLocation().GetUrl()); err != nil {
		return blobRange{}, err
	}
	count := int(ar.GetCount())
	if count <= 0 || count > n {
		return blobRange{}, fmt.Errorf("chunkstore: assign %d returned %d ids", n, count)
	}
	if err := validBaseFid(ar.GetFileId(), count); err != nil {
		return blobRange{}, err
	}
	return blobRange{base: ar.FileId, count: count, url: ar.Location.Url, auth: ar.GetAuth(), fsync: ar.GetFsync()}, nil
}

// putRange uploads data as offset i of r. A refusal before append is
// errRangeRefused, a rejected token errRangeAuth; any other failure is
// returned as is (the caller retries the same offset).
func (c *SWFS) putRange(ctx context.Context, r blobRange, i int, data []byte) error {
	fid, err := r.fid(i)
	if err != nil {
		return err
	}
	return c.upload(ctx, r.url, fid, r.auth, r.fsync, data)
}

// probe GETs offset i from the range's own volume server.
func (c *SWFS) probe(ctx context.Context, r blobRange, i int) ([]byte, error) {
	fid, err := r.fid(i)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+r.url+"/"+fid, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("chunkstore: probe %s: %w", fid, err)
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return io.ReadAll(resp.Body)
	case http.StatusNotFound:
		return nil, fmt.Errorf("%w %s", errBlobAbsent, fid)
	}
	return nil, fmt.Errorf("chunkstore: probe %s: http %d", fid, resp.StatusCode)
}

// upload POSTs data raw to fid on the volume server at host.
func (c *SWFS) upload(ctx context.Context, host, fid, auth string, fsync bool, data []byte) error {
	u := "http://" + host + "/" + fid
	if fsync {
		u += "?fsync=true"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if auth != "" {
		req.Header.Set("Authorization", "BEARER "+auth)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("chunkstore: upload %s: %w", fid, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		err := fmt.Errorf("chunkstore: upload %s: http %d: %s", fid, resp.StatusCode, strings.TrimSpace(string(body)))
		switch {
		case resp.StatusCode == http.StatusUnauthorized:
			return errors.Join(errRangeAuth, err)
		case resp.StatusCode == http.StatusConflict,
			resp.Header.Get("X-Seaweed-Write-Outcome") == "refused-before-append":
			return errors.Join(errRangeRefused, err)
		}
		return err
	}
	var ur struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(body, &ur) == nil && ur.Error != "" {
		return fmt.Errorf("chunkstore: upload %s: %s", fid, ur.Error)
	}
	return nil
}
