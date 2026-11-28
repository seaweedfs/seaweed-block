package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)

// SWFSConfig configures the minimal SeaweedFS client.
type SWFSConfig struct {
	FilerGRPC   string // filer gRPC address, host:port (default: filer HTTP port + 10000)
	Collection  string // collection for chunk needles (empty = filer default / storage rule)
	Replication string // replication for chunk needles (empty = filer default / storage rule)
	AssignPath  string // path passed to AssignVolume so filer storage rules (fsync, collection) apply
	HTTPTimeout time.Duration
}

// SWFS is a minimal SeaweedFS client built for the chunkstore PoC:
//   - filer gRPC (vendored proto subset, package filerpb) for HEAD/snapshot
//     entries, AssignVolume and LookupVolume;
//   - plain HTTP to volume servers for needle upload (POST) and read (GET).
//
// It deliberately does not import the SeaweedFS module.
type SWFS struct {
	cfg  SWFSConfig
	conn *grpc.ClientConn
	fc   filerpb.SeaweedFilerClient
	http *http.Client

	mu       sync.Mutex
	vidCache map[string][]string // volume id -> volume server urls
}

// filerNotFoundText is the text of seaweed-mono's filer_pb.ErrNotFound
// (pb/filer_pb/filer_pb_helper.go:261). LookupDirectoryEntry returns that raw
// sentinel, which crosses gRPC as codes.Unknown carrying only this text.
const filerNotFoundText = "no entry is found in filer store"

// DialSWFS connects to the filer's gRPC port. The connection is lazy.
func DialSWFS(cfg SWFSConfig) (*SWFS, error) {
	if cfg.FilerGRPC == "" {
		return nil, errors.New("chunkstore: filer gRPC address required")
	}
	if cfg.HTTPTimeout == 0 {
		cfg.HTTPTimeout = 60 * time.Second
	}
	conn, err := link.Dial(cfg.FilerGRPC,
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		return nil, fmt.Errorf("chunkstore: dial filer %s: %w", cfg.FilerGRPC, err)
	}
	return &SWFS{
		cfg:      cfg,
		conn:     conn,
		fc:       filerpb.NewSeaweedFilerClient(conn),
		http:     newBlobHTTPClient(cfg.HTTPTimeout),
		vidCache: map[string][]string{},
	}, nil
}

// blobIdleConnsPerHost is how many idle connections the blob client keeps
// per volume server: at least as many as it runs requests at once (uploads,
// hydration fetches and deletes, each at most 64), so a finished request's
// connection is reused instead of closed and replaced by a new one.
const blobIdleConnsPerHost = 64

// newBlobHTTPClient is the needle client's own transport. Go's shared
// default keeps only two idle connections per host: every request past two
// in flight to one volume server opened a new TCP connection and left a
// TIME_WAIT behind, enough to exhaust ephemeral ports at a high rate.
func newBlobHTTPClient(timeout time.Duration) *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = blobIdleConnsPerHost
	tr.MaxIdleConns = 4 * blobIdleConnsPerHost
	return &http.Client{Timeout: timeout, Transport: tr}
}

// Close releases the gRPC connection.
func (c *SWFS) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

// LookupEntry implements Filer.
func (c *SWFS) LookupEntry(ctx context.Context, dir, name string) (*filerpb.Entry, error) {
	resp, err := c.fc.LookupDirectoryEntry(ctx, &filerpb.LookupDirectoryEntryRequest{Directory: dir, Name: name})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound ||
				(st.Code() == codes.Unknown && strings.Contains(st.Message(), filerNotFoundText)) {
				return nil, ErrNotFound
			}
		}
		return nil, fmt.Errorf("chunkstore: lookup %s/%s: %w", dir, name, err)
	}
	if resp.GetEntry() == nil {
		return nil, ErrNotFound
	}
	return resp.Entry, nil
}

// CreateEntry implements Filer. A failed o_excl or condition is reported by
// the filer in CreateEntryResponse.ErrorCode with a nil gRPC error.
func (c *SWFS) CreateEntry(ctx context.Context, dir string, e *filerpb.Entry, oExcl bool) error {
	resp, err := c.fc.CreateEntry(ctx, &filerpb.CreateEntryRequest{Directory: dir, Entry: e, OExcl: oExcl})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("%w: create %s/%s: %v", ErrPrecondition, dir, e.GetName(), err)
		}
		return fmt.Errorf("chunkstore: create %s/%s: %w", dir, e.GetName(), err)
	}
	switch resp.GetErrorCode() {
	case filerpb.FilerError_OK:
		if resp.GetError() != "" {
			return fmt.Errorf("chunkstore: create %s/%s: %s", dir, e.GetName(), resp.GetError())
		}
		return nil
	case filerpb.FilerError_ENTRY_ALREADY_EXISTS:
		return fmt.Errorf("%w: %s/%s", ErrExists, dir, e.GetName())
	case filerpb.FilerError_PRECONDITION_FAILED:
		return fmt.Errorf("%w: create %s/%s", ErrPrecondition, dir, e.GetName())
	default:
		return fmt.Errorf("chunkstore: create %s/%s: %s (%s)", dir, e.GetName(), resp.GetError(), resp.GetErrorCode())
	}
}

// UpdateEntry implements Filer. A failed expected_extended comes back as gRPC
// FailedPrecondition. Any other error is ambiguous (the write may or may not
// have applied) and is returned unwrapped from the sentinels.
func (c *SWFS) UpdateEntry(ctx context.Context, dir string, e *filerpb.Entry, expected map[string][]byte) error {
	_, err := c.fc.UpdateEntry(ctx, &filerpb.UpdateEntryRequest{Directory: dir, Entry: e, ExpectedExtended: expected})
	if err == nil {
		return nil
	}
	switch status.Code(err) {
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: update %s/%s: %v", ErrPrecondition, dir, e.GetName(), err)
	case codes.NotFound:
		return fmt.Errorf("%w: update %s/%s", ErrNotFound, dir, e.GetName())
	}
	return fmt.Errorf("chunkstore: update %s/%s: %w", dir, e.GetName(), err)
}

// ListEntries implements Filer: one ListEntries stream of at most limit
// entries. The filer's not-found styles (gRPC NotFound, or its raw sentinel
// crossing as Unknown) mean an empty listing; any other error fails it.
func (c *SWFS) ListEntries(ctx context.Context, dir, prefix, startAfter string, limit uint32) ([]*filerpb.Entry, error) {
	stream, err := c.fc.ListEntries(ctx, &filerpb.ListEntriesRequest{
		Directory: dir, Prefix: prefix, StartFromFileName: startAfter, InclusiveStartFrom: false, Limit: limit,
	})
	if err != nil {
		if isFilerNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("chunkstore: list %s: %w", dir, err)
	}
	var out []*filerpb.Entry
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			if isFilerNotFound(err) && len(out) == 0 {
				return nil, nil
			}
			return nil, fmt.Errorf("chunkstore: list %s: %w", dir, err)
		}
		if resp.GetEntry() != nil {
			out = append(out, resp.Entry)
		}
	}
}

// DeleteEntry implements Filer. The filer reclaims the file's chunks; the
// request never recurses. A failed
// expected_extended (including a missing entry) comes back as gRPC
// FailedPrecondition; other failures are a gRPC error or the response's
// Error string, and are returned unwrapped from the sentinels.
func (c *SWFS) DeleteEntry(ctx context.Context, dir, name string, expected map[string][]byte) error {
	resp, err := c.fc.DeleteEntry(ctx, &filerpb.DeleteEntryRequest{
		Directory: dir, Name: name, IsDeleteData: true, IsRecursive: false, ExpectedExtended: expected,
	})
	if err != nil {
		if status.Code(err) == codes.FailedPrecondition {
			return fmt.Errorf("%w: delete %s/%s: %v", ErrPrecondition, dir, name, err)
		}
		return fmt.Errorf("chunkstore: delete %s/%s: %w", dir, name, err)
	}
	if resp.GetError() != "" {
		return fmt.Errorf("chunkstore: delete %s/%s: %s", dir, name, resp.GetError())
	}
	return nil
}

func isFilerNotFound(err error) bool {
	st, ok := status.FromError(err)
	return ok && (st.Code() == codes.NotFound || (st.Code() == codes.Unknown && strings.Contains(st.Message(), filerNotFoundText)))
}

// Put implements Blobs: filer AssignVolume, then a raw HTTP POST of data to
// the assigned volume server. The body is sent as application/octet-stream
// with no Content-Encoding, so the volume server stores it uncompressed (its
// auto-compress only fires for types it is sure about; octet-stream is
// cleared to "" first, storage/needle/needle_parse_upload.go:116-119).
func (c *SWFS) Put(ctx context.Context, data []byte) (string, error) {
	ar, err := c.fc.AssignVolume(ctx, &filerpb.AssignVolumeRequest{
		Count:            1,
		Collection:       c.cfg.Collection,
		Replication:      c.cfg.Replication,
		Path:             c.cfg.AssignPath,
		ExpectedDataSize: uint64(len(data)),
	})
	if err != nil {
		return "", fmt.Errorf("chunkstore: assign: %w", err)
	}
	if ar.GetError() != "" {
		return "", fmt.Errorf("chunkstore: assign: %s", ar.GetError())
	}
	if ar.GetFileId() == "" || ar.GetLocation().GetUrl() == "" {
		return "", fmt.Errorf("chunkstore: assign returned no fid/location")
	}
	if err := c.upload(ctx, ar.Location.Url, ar.FileId, ar.GetAuth(), ar.GetFsync(), data); err != nil {
		return "", err
	}
	return ar.FileId, nil
}

// Get implements Blobs: resolve the fid's volume through the filer, then GET
// the needle from each location until one answers. The needle is reported
// absent (ErrBlobNotFound) only when every location of a fresh lookup
// answers 404: cached locations can be stale after a volume moves, so their
// 404s are followed by one fresh lookup.
func (c *SWFS) Get(ctx context.Context, fid string) ([]byte, error) {
	vid, _, ok := strings.Cut(fid, ",")
	if !ok {
		return nil, fmt.Errorf("chunkstore: bad fid %q", fid)
	}
	urls, cached, err := c.lookupVolume(ctx, vid)
	if err != nil {
		return nil, err
	}
	b, allNotFound, lastErr := c.getFromAll(ctx, urls, fid)
	if lastErr == nil {
		return b, nil
	}
	c.mu.Lock()
	delete(c.vidCache, vid)
	c.mu.Unlock()
	if allNotFound && cached {
		if urls, _, err = c.lookupVolume(ctx, vid); err != nil {
			return nil, err
		}
		if b, allNotFound, lastErr = c.getFromAll(ctx, urls, fid); lastErr == nil {
			return b, nil
		}
		c.mu.Lock()
		delete(c.vidCache, vid)
		c.mu.Unlock()
	}
	if allNotFound {
		lastErr = &blobNotFoundError{lastErr}
	}
	return nil, fmt.Errorf("chunkstore: read %s: %w", fid, lastErr)
}

// getFromAll tries each location in turn. On failure lastErr is the last
// location's error and allNotFound whether every location answered 404.
func (c *SWFS) getFromAll(ctx context.Context, urls []string, fid string) (b []byte, allNotFound bool, lastErr error) {
	allNotFound = true
	for _, host := range urls {
		b, err := c.getFrom(ctx, host, fid)
		if err == nil {
			return b, false, nil
		}
		lastErr = err
		var se *blobStatusError
		if !errors.As(err, &se) || se.code != http.StatusNotFound {
			allNotFound = false
		}
	}
	return nil, allNotFound && lastErr != nil, lastErr
}

// blobStatusError is a volume server's non-200 answer to a needle GET.
type blobStatusError struct {
	code int
	host string
}

func (e *blobStatusError) Error() string { return fmt.Sprintf("http %d from %s", e.code, e.host) }

// blobNotFoundError marks a read every location answered 404: it reads as
// the last location's error and matches ErrBlobNotFound.
type blobNotFoundError struct{ err error }

func (e *blobNotFoundError) Error() string        { return e.err.Error() }
func (e *blobNotFoundError) Unwrap() error        { return e.err }
func (e *blobNotFoundError) Is(target error) bool { return target == ErrBlobNotFound }

func (c *SWFS) getFrom(ctx context.Context, host, fid string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+"/"+fid, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, &blobStatusError{code: resp.StatusCode, host: host}
	}
	return io.ReadAll(resp.Body)
}

// lookupVolume returns vid's locations; cached reports that they came from
// the cache rather than a lookup made now.
func (c *SWFS) lookupVolume(ctx context.Context, vid string) (urls []string, cached bool, err error) {
	c.mu.Lock()
	urls, ok := c.vidCache[vid]
	c.mu.Unlock()
	if ok && len(urls) > 0 {
		return urls, true, nil
	}
	resp, err := c.fc.LookupVolume(ctx, &filerpb.LookupVolumeRequest{VolumeIds: []string{vid}})
	if err != nil {
		return nil, false, fmt.Errorf("chunkstore: lookup volume %s: %w", vid, err)
	}
	for _, l := range resp.GetLocationsMap()[vid].GetLocations() {
		if l.GetUrl() != "" {
			urls = append(urls, l.Url)
		}
	}
	if len(urls) == 0 {
		return nil, false, fmt.Errorf("chunkstore: volume %s has no locations", vid)
	}
	c.mu.Lock()
	c.vidCache[vid] = urls
	c.mu.Unlock()
	return urls, false, nil
}

var (
	_ Filer = (*SWFS)(nil)
	_ Blobs = (*SWFS)(nil)
)

// ErrBlobDeleteRejected identifies a definite per-object refusal (HTTP 400).
// It never proves the needle absent.
var ErrBlobDeleteRejected = errors.New("chunkstore: blob DELETE rejected")

// DeleteBlob removes an immutable needle from every resolved replica. A
// partial/unknown result is an error, so GC retains its binding tombstone
// and retries later; no response body is used to infer success.
// SeaweedFS writeDeleteResult returns 202 after ReplicatedDelete completes.
func (c *SWFS) DeleteBlob(ctx context.Context, fid string) error {
	vid, needle, ok := strings.Cut(fid, ",")
	if !ok || vid == "" || needle == "" || strings.ContainsAny(fid, "/\\?#% \r\n") {
		return fmt.Errorf("chunkstore: bad deletion fid %q", fid)
	}
	urls, _, err := c.lookupVolume(ctx, vid)
	if err != nil {
		return err
	}
	for _, host := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://"+host+"/"+fid, nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return err
		}
		// Drain before close so the connection is reused.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		if resp.StatusCode == http.StatusBadRequest {
			return fmt.Errorf("%w: delete %s: http %d", ErrBlobDeleteRejected, fid, resp.StatusCode)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
			return fmt.Errorf("chunkstore: delete %s: http %d", fid, resp.StatusCode)
		}
	}
	return nil
}
