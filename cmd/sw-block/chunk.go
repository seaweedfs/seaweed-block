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
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

const (
	chunkExitFailed = 1
	// chunkExitFindings: the command completed and found problems (verify).
	chunkExitFindings   = 3
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

// checkSingleFiler is the start check of a command that writes chunk
// metadata: block metadata goes through exactly one filer, the
// same rule blockvolume enforces at start. Tests replace it.
var checkSingleFiler = func(ctx context.Context, f chunkstore.Filer) error {
	if s, ok := f.(*chunkstore.SWFS); ok {
		return s.CheckSingleFiler(ctx)
	}
	return nil
}

// refuseUnlessSingleFiler runs the start check within the command's own
// timeout (a stalled filer or master cannot hold the command longer); exit
// >= 0 means refused.
func refuseUnlessSingleFiler(name string, f chunkstore.Filer, timeout time.Duration, stdout, stderr io.Writer) int {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	err := checkSingleFiler(ctx, f)
	switch {
	case err == nil:
		return -1
	case errors.Is(err, chunkstore.ErrNotSingleFiler):
		return chunkFail(name, stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	default:
		return chunkFail(name, stdout, stderr, chunkExitFailed, codeUnavailableCLI, "single-filer start check: "+err.Error(), true)
	}
}

func runOpsChunk(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return chunkFail("", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "expected cut|snapshot|snapshots|snapshot-delete|detach|show|usage|verify|volume-delete|restore|release|retain", false)
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
	case "show":
		return chunkShow(args[1:], stdout, stderr)
	case "usage":
		return chunkUsage(args[1:], stdout, stderr)
	case "release":
		return chunkRelease(args[1:], stdout, stderr)
	case "retain":
		return chunkRetain(args[1:], stdout, stderr)
	case "verify":
		return chunkVerify(args[1:], stdout, stderr)
	case "volume-delete":
		return chunkVolumeDelete(args[1:], stdout, stderr)
	case "restore":
		return chunkRestore(args[1:], stdout, stderr)
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

// chunkRequestBound is the server's bound on one /chunk/* request
// (chunkstore.DefaultOpDeadline). A var only so tests can shorten it.
var chunkRequestBound = chunkstore.DefaultOpDeadline

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
	fs.DurationVar(&timeout, "timeout", 60*time.Second, chunkTimeoutHelp)
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || statusAddr == "" {
		return chunkFail("cut", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume and --status-addr are required", false)
	}
	return chunkPostLoop("cut", statusAddr, "/chunk/cut", timeout, map[string]any{"volumeID": volumeID, "minLSN": minLSN}, stdout, stderr)
}

func chunkSnapshot(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("snapshot", stderr)
	var (
		volumeID, statusAddr, name, requestID, consistency, class string
		expiresAtNs                                               int64
		timeout, expires                                          time.Duration
		pin                                                       bool
	)
	labels := labelFlags{}
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume status address or URL (with --chunk-control-endpoint)")
	fs.StringVar(&name, "name", "", "snapshot name")
	fs.StringVar(&requestID, "request-id", "", "idempotency id; generated and printed when empty (reuse it to retry)")
	fs.StringVar(&consistency, "consistency", "crash", "crash, or app when the guest was frozen by the caller")
	fs.Int64Var(&expiresAtNs, "expires-at-ns", 0, "pins the snapshot until this time (unix ns); retention expires it afterwards")
	fs.DurationVar(&expires, "expires", 0, "pins the snapshot for this long from now (sets --expires-at-ns)")
	fs.StringVar(&class, "class", "", "retention class: semantic (an automatic checkpoint, expired by the policy) or pinned (kept until deleted)")
	fs.BoolVar(&pin, "pin", false, "shorthand for --class pinned")
	fs.Var(labels, "label", "key=value label (repeatable)")
	fs.DurationVar(&timeout, "timeout", 60*time.Second, chunkTimeoutHelp)
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || statusAddr == "" || name == "" {
		return chunkFail("snapshot", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume, --status-addr and --name are required", false)
	}
	if pin {
		if class != "" && class != layout.ClassPinned {
			return chunkFail("snapshot", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--pin conflicts with --class "+class, false)
		}
		class = layout.ClassPinned
	}
	if expires < 0 || expires > 0 && expiresAtNs != 0 {
		return chunkFail("snapshot", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--expires must be positive and does not combine with --expires-at-ns", false)
	}
	if expires > 0 {
		expiresAtNs = time.Now().Add(expires).UnixNano()
	}
	if requestID == "" {
		requestID = newRequestID()
		fmt.Fprintf(stderr, "sw-block ops chunk snapshot: request id %s (reuse it to retry)\n", requestID)
	}
	body := map[string]any{"volumeID": volumeID, "name": name,
		"requestID": requestID, "labels": map[string]string(labels), "consistency": consistency, "expiresAtNs": expiresAtNs}
	if class != "" {
		body["class"] = class // only when set: the request otherwise keeps its spec fields
	}
	return chunkPostLoop("snapshot", statusAddr, "/chunk/snapshot", timeout, body, stdout, stderr)
}

func chunkDetach(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("detach", stderr)
	var (
		volumeID, statusAddr           string
		timeout                        time.Duration
		plain, frozen, allowOverBudget bool
		freezeStartedAtNs              int64
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume status address or URL (with --chunk-control-endpoint)")
	fs.DurationVar(&timeout, "timeout", 120*time.Second, chunkTimeoutHelp)
	fs.BoolVar(&plain, "plain", false, "seal an idle volume without pre-copy; prints JSON headGen as sealed HEAD generation on stderr")
	fs.BoolVar(&frozen, "frozen", false, "not supported in this release: the server refuses a frozen detach (frozen_detach_unsupported)")
	fs.Int64Var(&freezeStartedAtNs, "freeze-started-at-ns", 0, "not supported in this release; only valid with --frozen")
	fs.BoolVar(&allowOverBudget, "allow-over-budget", false, "not supported in this release; only valid with --frozen")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || statusAddr == "" {
		return chunkFail("detach", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume and --status-addr are required", false)
	}
	// M5 PlainStop -> Quiesce, INV-CUT-PLAIN-DETACH-001: an idle seal has
	// its own finite caller bound and cannot encode frozen policy fields.
	if plain && (timeout <= 0 || frozen || freezeStartedAtNs != 0 || allowOverBudget || fs.NArg() != 0) {
		return chunkFail("detach", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--plain requires positive --timeout, no extra arguments and no frozen options", false)
	}
	if (frozen && freezeStartedAtNs <= 0) || (!frozen && (freezeStartedAtNs != 0 || allowOverBudget)) {
		return chunkFail("detach", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--frozen requires --freeze-started-at-ns; --allow-over-budget requires --frozen", false)
	}
	body := map[string]any{"volumeID": volumeID}
	if plain {
		body["plain"] = true
		return chunkPlainDetachPost(statusAddr, volumeID, timeout, body, stdout, stderr)
	}
	if frozen {
		body["frozen"] = true
		body["freezeStartedAtNs"] = freezeStartedAtNs
		if allowOverBudget {
			body["allowOverBudget"] = true
		}
	}
	return chunkPostLoop("detach", statusAddr, "/chunk/detach", timeout, body, stdout, stderr)
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
		case layoutCode(err) != "":
			return chunkFail(name, stdout, stderr, chunkExitFailed, layoutCode(err), err.Error(), errors.Is(err, layout.ErrPrecondition))
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

// chunkFail reports a failure (B9 W2 appendix): the error body
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
