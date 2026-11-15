package main

// sw-block ops chunk: the walstore-chunk control plane (B9 W2 §0.1, §3).
// cut, snapshot and detach call a blockvolume's /chunk/* routes
// (--status-addr). snapshots, snapshot-delete and fork read and write the
// filer directly (--chunk-filer, --chunk-path), so they work while the
// volume is offline. Each prints the operation's JSON body on stdout. A
// failure prints the routes' error body {code,message,retryable} on stdout
// and a log line on stderr, and exits non-zero (2 for usage errors).

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
	"github.com/seaweedfs/seaweed-block/core/storage"
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
		return chunkFail("", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "expected cut|snapshot|snapshots|snapshot-delete|fork|detach|show", false)
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
	case "show":
		return chunkShow(args[1:], stdout, stderr)
	default:
		return chunkFail("", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, fmt.Sprintf("unknown subcommand %q", args[0]), false)
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
		return chunkFail(name, stdout, stderr, chunkExitFailed, codeInternalCLI, err.Error(), false)
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout+5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+route, bytes.NewReader(data))
	if err != nil {
		return chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return chunkFail(name, stdout, stderr, chunkExitFailed, codeUnavailableCLI, "blockvolume not reachable: "+err.Error(), true)
	}
	defer resp.Body.Close()
	out, err := io.ReadAll(io.LimitReader(resp.Body, chunkResponseLimit+1))
	if err != nil {
		return chunkFail(name, stdout, stderr, chunkExitFailed, codeUnavailableCLI, "read the response: "+err.Error(), true)
	}
	if int64(len(out)) > chunkResponseLimit {
		// Refuse rather than relay a truncated body; the operation may have
		// happened, and its result is what would be lost.
		return chunkFail(name, stdout, stderr, chunkExitFailed, codeInternalCLI,
			fmt.Sprintf("HTTP %d response exceeds %d bytes; not relaying a truncated body", resp.StatusCode, chunkResponseLimit), false)
	}
	if resp.StatusCode == http.StatusOK {
		if err := writeAll(stdout, out); err != nil {
			fmt.Fprintf(stderr, "sw-block ops chunk %s: write the result: %v\n", name, err)
			return chunkExitFailed
		}
		return ops.VolumeStatusExitOK
	}
	var e chunkErrBody
	if json.Unmarshal(out, &e) != nil || e.Code == "" {
		return chunkFail(name, stdout, stderr, chunkExitFailed, codeInternalCLI, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(out))), false)
	}
	// The route's error body is already the CLI's error body: relay it.
	fmt.Fprintf(stderr, "sw-block ops chunk %s: %s: %s\n", name, e.Code, e.Message)
	if err := writeAll(stdout, out); err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: write the error: %v\n", name, err)
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
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || statusAddr == "" {
		return chunkFail("cut", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume and --status-addr are required", false)
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
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || statusAddr == "" || name == "" {
		return chunkFail("snapshot", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume, --status-addr and --name are required", false)
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
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || statusAddr == "" {
		return chunkFail("detach", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume and --status-addr are required", false)
	}
	return chunkPost("detach", statusAddr, "/chunk/detach", timeout, map[string]any{"volumeID": volumeID}, stdout, stderr)
}

// chunkCatalog opens the catalog under --chunk-path on --chunk-filer.
// It returns exit -1 on success, else the exit code after reporting.
func chunkCatalog(name, filer, base string, stdout, stderr io.Writer) (*chunkstore.Catalog, func() error, int) {
	if filer == "" {
		return nil, nil, chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--chunk-filer is required", false)
	}
	f, closeFn, err := dialChunkFiler(filer)
	if err != nil {
		return nil, nil, chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	cat, err := chunkstore.NewCatalog(f, base)
	if err != nil {
		_ = closeFn()
		return nil, nil, chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	return cat, closeFn, -1
}

// chunkResult prints v as JSON, or err as "<code>: <message>".
func chunkResult(name string, v any, err error, stdout, stderr io.Writer) int {
	if err != nil {
		var ce *chunkstore.CatalogError
		switch {
		case errors.As(err, &ce):
			return chunkFail(name, stdout, stderr, chunkExitFailed, ce.Code, ce.Detail(), ce.Retryable)
		case errors.Is(err, context.DeadlineExceeded):
			return chunkFail(name, stdout, stderr, chunkExitFailed, chunkstore.CodeDeadlineExceeded, err.Error(), true)
		default:
			return chunkFail(name, stdout, stderr, chunkExitFailed, codeInternalCLI, err.Error(), false)
		}
	}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: %v\n", name, err)
		return chunkExitFailed
	}
	if err := writeAll(stdout, buf.Bytes()); err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk %s: write the result: %v\n", name, err)
		return chunkExitFailed
	}
	return ops.VolumeStatusExitOK
}

// CLI-side error codes, the same as the routes' (B9 §2.2 and the rulings
// of 2026-11-15).
const (
	codeBadRequestCLI  = "bad_request"
	codeInternalCLI    = "internal"
	codeUnavailableCLI = "remote_unavailable"
)

type chunkErrBody struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// chunkFail reports a failure (appendix 14:30Z): the error body
// {code,message,retryable} on stdout, as the routes answer it, a log line on
// stderr, and the exit code. The message does not repeat the code.
func chunkFail(name string, stdout, stderr io.Writer, exit int, code, message string, retryable bool) int {
	prefix := "sw-block ops chunk"
	if name != "" {
		prefix += " " + name
	}
	fmt.Fprintf(stderr, "%s: %s: %s\n", prefix, code, message)
	body, _ := json.Marshal(chunkErrBody{Code: code, Message: message, Retryable: retryable})
	if err := writeAll(stdout, append(body, '\n')); err != nil {
		fmt.Fprintf(stderr, "%s: write the error: %v\n", prefix, err)
	}
	return exit
}

func chunkName(fs *flag.FlagSet) string { return strings.TrimPrefix(fs.Name(), "sw-block ops chunk ") }

// writeAll writes p and fails on a failed or short write: a lost result
// must not exit 0.
func writeAll(w io.Writer, p []byte) error {
	n, err := w.Write(p)
	if err == nil && n != len(p) {
		err = io.ErrShortWrite
	}
	return err
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
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" {
		return chunkFail("snapshots", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume is required", false)
	}
	cat, closeFn, exit := chunkCatalog("snapshots", filer, base, stdout, stderr)
	if exit >= 0 {
		return exit
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
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || name == "" {
		return chunkFail("snapshot-delete", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume and --name are required", false)
	}
	cat, closeFn, exit := chunkCatalog("snapshot-delete", filer, base, stdout, stderr)
	if exit >= 0 {
		return exit
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
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if source == "" || snapshot == "" || target == "" {
		return chunkFail("fork", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--source, --snapshot and --target are required", false)
	}
	if requestID == "" {
		requestID = newRequestID()
		fmt.Fprintf(stderr, "sw-block ops chunk fork: request id %s (reuse it to retry)\n", requestID)
	}
	cat, closeFn, exit := chunkCatalog("fork", filer, base, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := cat.Fork(ctx, chunkstore.ForkRequest{Source: source, Snapshot: snapshot, Target: target, RequestID: requestID, Labels: map[string]string(labels)})
	return chunkResult("fork", res, err, stdout, stderr)
}

// chunkShow prints one volume's VOLUME entry and HEAD from the filer (§0.1).
func chunkShow(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("show", stderr)
	var (
		volumeID, filer, base string
		timeout               time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.DurationVar(&timeout, "timeout", 30*time.Second, "filer timeout")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" {
		return chunkFail("show", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume is required", false)
	}
	cat, closeFn, exit := chunkCatalog("show", filer, base, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	v, hasVolume, err := cat.ReadVolume(ctx, volumeID)
	if err != nil {
		return chunkResult("show", nil, err, stdout, stderr)
	}
	h, hasHead, err := cat.ReadHead(ctx, volumeID)
	if err != nil {
		return chunkResult("show", nil, err, stdout, stderr)
	}
	var vp *chunkstore.VolumeInfo
	if hasVolume {
		vp = &v
	}
	var hp *storage.ChunkHead
	if hasHead {
		hp = &h
	}
	return chunkResult("show", chunkstore.NewVolumeShowBody(volumeID, vp, hp), nil, stdout, stderr)
}
