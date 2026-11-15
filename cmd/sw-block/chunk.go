package main

// sw-block ops chunk: the walstore-chunk control plane (B9 W2 §0.1, §3).
// cut, snapshot and detach call a blockvolume's /chunk/* routes
// (--status-addr). snapshots, snapshot-delete and fork read and write the
// filer directly (--chunk-filer, --chunk-path), so they work while the
// volume is offline. Each prints the operation's JSON body on stdout; a
// failure prints "<code>: <message> (retryable=...)" on stderr and exits 1.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

const (
	chunkExitFailed     = 1
	defaultChunkPathCLI = "/swblock/chunks"
	chunkResponseLimit  = 64 << 20
)

// dialChunkFiler opens the filer for the direct commands; tests replace it.
var dialChunkFiler = func(addr string) (chunkstore.Filer, func() error, error) {
	c, err := chunkstore.DialSWFS(chunkstore.SWFSConfig{FilerGRPC: addr})
	if err != nil {
		return nil, nil, err
	}
	return c, c.Close, nil
}

func runOpsChunk(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "sw-block ops chunk: expected cut|snapshot|snapshots|snapshot-delete|fork|detach")
		return ops.VolumeStatusExitInvalid
	}
	switch args[0] {
	case "cut":
		return chunkCut(args[1:], stdout, stderr)
	case "snapshot":
		return chunkSnapshot(args[1:], stdout, stderr)
	case "detach":
		return chunkDetach(args[1:], stdout, stderr)
	case "snapshots":
		return chunkSnapshots(args[1:], stdout, stderr)
	case "snapshot-delete":
		return chunkSnapshotDelete(args[1:], stdout, stderr)
	case "fork":
		return chunkFork(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "sw-block ops chunk: unknown subcommand %q\n", args[0])
		return ops.VolumeStatusExitInvalid
	}
}

// labelFlags collects repeated --label k=v.
type labelFlags map[string]string

func (l labelFlags) String() string { return "" }

func (l labelFlags) Set(v string) error {
	k, val, ok := strings.Cut(v, "=")
	if !ok || k == "" {
		return fmt.Errorf("label %q: want key=value", v)
	}
	l[k] = val
	return nil
}

func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func chunkFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet("sw-block ops chunk "+name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

// chunkPost sends one /chunk/* request and relays the outcome. The server
// bounds the operation by timeoutMs; the client waits a little longer.
func chunkPost(name, statusAddr, route string, timeout time.Duration, body map[string]any, stdout, stderr io.Writer) int {
	base := statusAddr
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	body["timeoutMs"] = timeout.Milliseconds()
	data, err := json.Marshal(body)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		return chunkExitFailed
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout+5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+route, bytes.NewReader(data))
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		return ops.VolumeStatusExitInvalid
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		return chunkExitFailed
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, chunkResponseLimit))
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: read response: %v\n", name, err)
		return chunkExitFailed
	}
	if resp.StatusCode == http.StatusOK {
		_, _ = stdout.Write(out)
		return ops.VolumeStatusExitOK
	}
	var e struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		Retryable bool   `json:"retryable"`
	}
	if json.Unmarshal(out, &e) == nil && e.Code != "" {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %s: %s (retryable=%v)\n", name, e.Code, e.Message, e.Retryable)
	} else {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: HTTP %d: %s\n", name, resp.StatusCode, strings.TrimSpace(string(out)))
	}
	return chunkExitFailed
}

func chunkCut(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("cut", stderr)
	var (
		volumeID, statusAddr string
		minLSN               uint64
		timeout              time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume status address or URL (with --chunk-control-endpoint)")
	fs.Uint64Var(&minLSN, "min-lsn", 0, "fail unless the cut reaches this LSN")
	fs.DurationVar(&timeout, "timeout", 60*time.Second, "operation bound (capped at the server's OpDeadline)")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if volumeID == "" || statusAddr == "" {
		fmt.Fprintln(stderr, "sw-block ops chunk cut: --volume and --status-addr are required")
		return ops.VolumeStatusExitInvalid
	}
	return chunkPost("cut", statusAddr, "/chunk/cut", timeout, map[string]any{"volumeID": volumeID, "minLSN": minLSN}, stdout, stderr)
}

func chunkSnapshot(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("snapshot", stderr)
	var (
		volumeID, statusAddr, name, requestID, consistency string
		expiresAtNs                                        int64
		timeout                                            time.Duration
	)
	labels := labelFlags{}
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume status address or URL (with --chunk-control-endpoint)")
	fs.StringVar(&name, "name", "", "snapshot name")
	fs.StringVar(&requestID, "request-id", "", "idempotency id; generated and printed when empty (reuse it to retry)")
	fs.StringVar(&consistency, "consistency", "crash", "crash, or app when the guest was frozen by the caller")
	fs.Int64Var(&expiresAtNs, "expires-at-ns", 0, "stored for the retention policy; not enforced here")
	fs.Var(labels, "label", "key=value label (repeatable)")
	fs.DurationVar(&timeout, "timeout", 60*time.Second, "operation bound (capped at the server's OpDeadline)")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if volumeID == "" || statusAddr == "" || name == "" {
		fmt.Fprintln(stderr, "sw-block ops chunk snapshot: --volume, --status-addr and --name are required")
		return ops.VolumeStatusExitInvalid
	}
	if requestID == "" {
		requestID = newRequestID()
		fmt.Fprintf(stderr, "sw-block ops chunk snapshot: request id %s (reuse it to retry)\n", requestID)
	}
	return chunkPost("snapshot", statusAddr, "/chunk/snapshot", timeout, map[string]any{"volumeID": volumeID, "name": name,
		"requestID": requestID, "labels": map[string]string(labels), "consistency": consistency, "expiresAtNs": expiresAtNs}, stdout, stderr)
}

func chunkDetach(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("detach", stderr)
	var (
		volumeID, statusAddr string
		timeout              time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume status address or URL (with --chunk-control-endpoint)")
	fs.DurationVar(&timeout, "timeout", 120*time.Second, "operation bound (capped at the server's OpDeadline)")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if volumeID == "" || statusAddr == "" {
		fmt.Fprintln(stderr, "sw-block ops chunk detach: --volume and --status-addr are required")
		return ops.VolumeStatusExitInvalid
	}
	return chunkPost("detach", statusAddr, "/chunk/detach", timeout, map[string]any{"volumeID": volumeID}, stdout, stderr)
}

// chunkCatalog opens the catalog under --chunk-path on --chunk-filer.
func chunkCatalog(name, filer, base string, stderr io.Writer) (*chunkstore.Catalog, func() error, bool) {
	if filer == "" {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: --chunk-filer is required\n", name)
		return nil, nil, false
	}
	f, closeFn, err := dialChunkFiler(filer)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		return nil, nil, false
	}
	cat, err := chunkstore.NewCatalog(f, base)
	if err != nil {
		_ = closeFn()
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		return nil, nil, false
	}
	return cat, closeFn, true
}

// chunkResult prints v as JSON, or err as "<code>: <message>".
func chunkResult(name string, v any, err error, stdout, stderr io.Writer) int {
	if err != nil {
		var ce *chunkstore.CatalogError
		if errors.As(err, &ce) {
			fmt.Fprintf(stderr, "sw-block ops chunk %s: %s: %s (retryable=%v)\n", name, ce.Code, err, ce.Retryable)
		} else {
			fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		}
		return chunkExitFailed
	}
	enc := json.NewEncoder(stdout)
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		return chunkExitFailed
	}
	return ops.VolumeStatusExitOK
}

func chunkSnapshots(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("snapshots", stderr)
	var (
		volumeID, filer, base, prefix, startAfter string
		limit                                     int
		includeDeleting                           bool
		timeout                                   time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.StringVar(&prefix, "prefix", "", "only names with this prefix")
	fs.StringVar(&startAfter, "start-after", "", "list after this name (the previous page's next)")
	fs.IntVar(&limit, "limit", 0, "page size (0: the default)")
	fs.BoolVar(&includeDeleting, "include-deleting", false, "also list snapshots marked deleting")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "filer timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if volumeID == "" {
		fmt.Fprintln(stderr, "sw-block ops chunk snapshots: --volume is required")
		return ops.VolumeStatusExitInvalid
	}
	cat, closeFn, ok := chunkCatalog("snapshots", filer, base, stderr)
	if !ok {
		return ops.VolumeStatusExitInvalid
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	list, next, err := cat.ListSnapshots(ctx, volumeID, chunkstore.ListOptions{Prefix: prefix, StartAfter: startAfter, Limit: limit, IncludeDeleting: includeDeleting})
	return chunkResult("snapshots", chunkstore.NewSnapshotListBody(volumeID, list, next), err, stdout, stderr)
}

func chunkSnapshotDelete(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("snapshot-delete", stderr)
	var (
		volumeID, filer, base, name string
		timeout                     time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&name, "name", "", "snapshot name")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "filer timeout")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if volumeID == "" || name == "" {
		fmt.Fprintln(stderr, "sw-block ops chunk snapshot-delete: --volume and --name are required")
		return ops.VolumeStatusExitInvalid
	}
	cat, closeFn, ok := chunkCatalog("snapshot-delete", filer, base, stderr)
	if !ok {
		return ops.VolumeStatusExitInvalid
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	state, err := cat.DeleteSnapshot(ctx, volumeID, name)
	return chunkResult("snapshot-delete", chunkstore.SnapshotDeleteBody{VolumeID: volumeID, Name: name, State: state}, err, stdout, stderr)
}

func chunkFork(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("fork", stderr)
	var (
		filer, base, source, snapshot, target, requestID string
		timeout                                          time.Duration
	)
	labels := labelFlags{}
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.StringVar(&source, "source", "", "source volume id")
	fs.StringVar(&snapshot, "snapshot", "", "source snapshot name")
	fs.StringVar(&target, "target", "", "new volume id")
	fs.StringVar(&requestID, "request-id", "", "idempotency id; generated and printed when empty (reuse it to retry)")
	fs.Var(labels, "label", "key=value label (repeatable)")
	fs.DurationVar(&timeout, "timeout", 6*time.Minute, "filer timeout (the fork's own HEAD deadline is OpDeadline)")
	if err := fs.Parse(args); err != nil {
		return ops.VolumeStatusExitInvalid
	}
	if source == "" || snapshot == "" || target == "" {
		fmt.Fprintln(stderr, "sw-block ops chunk fork: --source, --snapshot and --target are required")
		return ops.VolumeStatusExitInvalid
	}
	if requestID == "" {
		requestID = newRequestID()
		fmt.Fprintf(stderr, "sw-block ops chunk fork: request id %s (reuse it to retry)\n", requestID)
	}
	cat, closeFn, ok := chunkCatalog("fork", filer, base, stderr)
	if !ok {
		return ops.VolumeStatusExitInvalid
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := cat.Fork(ctx, chunkstore.ForkRequest{Source: source, Snapshot: snapshot, Target: target, RequestID: requestID, Labels: map[string]string(labels)})
	return chunkResult("fork", res, err, stdout, stderr)
}
