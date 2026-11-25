package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/layout"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// chunkVerify: sw-block ops chunk verify --volume v --chunk-filer host:port
// checks the volume's store read-only: VOLUME, HEAD, every root HEAD names
// (the current image and every snapshot), their segments and chunks
// (chunkstore.VerifyLayout). --orphans also reports the volume's own names
// that no reachable root names (it never deletes them). Exit 0: clean; 3:
// the pass completed with findings; 1: it could not complete (the partial
// result is printed with the reason); 2: usage.
func chunkVerify(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("verify", stderr)
	var (
		volumeID, filer, base, snapshot, depth string
		allSnapshots, orphans                  bool
		concurrency                            int
		bytesPerSec                            int64
		timeout                                time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.StringVar(&snapshot, "snapshot", "", "accepted for compatibility: every snapshot HEAD names is always checked")
	fs.BoolVar(&allSnapshots, "all-snapshots", false, "accepted for compatibility: every snapshot HEAD names is always checked")
	fs.StringVar(&depth, "depth", "bindings", "meta or bindings: every named object exists; data: and every chunk's bytes hash to its name")
	fs.BoolVar(&orphans, "orphans", false, "also report the volume's own names no reachable root names (report only)")
	fs.IntVar(&concurrency, "concurrency", 1, "accepted for compatibility: chunks are read one at a time")
	fs.Int64Var(&bytesPerSec, "bytes-per-sec", 0, "accepted for compatibility: chunk reads are not paced")
	fs.DurationVar(&timeout, "timeout", 10*time.Minute, "bound for the whole pass; required with --depth data")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	timeoutSet := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "timeout" {
			timeoutSet = true
		}
	})
	usage := func(msg string) int {
		return chunkFail("verify", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, msg, false)
	}
	d := map[string]string{"meta": layout.DepthNames, "bindings": layout.DepthNames, "data": layout.DepthFull}[depth]
	switch {
	case fs.NArg() != 0:
		return usage("unexpected arguments")
	case volumeID == "":
		return usage("--volume is required")
	case filer == "":
		return usage("--chunk-filer is required")
	case d == "":
		return usage(fmt.Sprintf("--depth %q: meta, bindings or data", depth))
	case snapshot != "" && allSnapshots:
		return usage("--snapshot and --all-snapshots do not combine")
	case d == layout.DepthFull && !timeoutSet:
		return usage("--depth data reads every chunk: give an explicit --timeout for the pass")
	case timeout <= 0 || concurrency <= 0 || bytesPerSec < 0:
		return usage("--timeout and --concurrency must be positive and --bytes-per-sec not negative")
	}
	// One bound for the whole pass, the single-filer start check included.
	// Verify writes nothing, but against a split filer view it would report
	// false findings, so it runs the check.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	f, closeFn, err := dialChunkFiler(filer)
	if err != nil {
		return usage(err.Error())
	}
	defer closeFn()
	if exit := refuseUnlessSingleFiler("verify", f, timeout, stdout, stderr); exit >= 0 {
		return exit
	}
	res, err := chunkstore.VerifyLayout(ctx, f, base, volumeID, layout.VerifyOptions{Depth: d, Orphans: orphans})
	if err != nil {
		return verifyIncomplete(res, err, stdout, stderr)
	}
	out, err := verifyJSON(res)
	if err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk verify: %v\n", err)
		return chunkExitFailed
	}
	if err := writeAll(stdout, out); err != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk verify: write the result: %v\n", err)
		return chunkExitFailed
	}
	if !res.Complete {
		fmt.Fprintf(stderr, "sw-block ops chunk verify: %s: incomplete: %s\n", volumeID, res.Incomplete)
		return chunkExitFailed
	}
	if len(res.Findings) != 0 {
		fmt.Fprintf(stderr, "sw-block ops chunk verify: %s: %d findings\n", volumeID, len(res.Findings))
		return chunkExitFindings
	}
	return ops.VolumeStatusExitOK
}

// verifyJSON is the result as one JSON object whose findings array holds
// one finding per line.
func verifyJSON(res layout.VerifyResult) ([]byte, error) {
	findings := res.Findings
	res.Findings = nil
	head, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	const tail = `"Findings":null}`
	if !bytes.HasSuffix(head, []byte(tail)) {
		return nil, errors.New("verify result: unexpected field order")
	}
	var buf bytes.Buffer
	buf.Write(head[:len(head)-len(tail)])
	buf.WriteString(`"Findings":[`)
	for i, f := range findings {
		line, err := json.Marshal(f)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('\n')
		buf.Write(line)
	}
	if len(findings) > 0 {
		buf.WriteByte('\n')
	}
	buf.WriteString("]}\n")
	return buf.Bytes(), nil
}

// verifyIncomplete prints the error body with the partial result under
// "verify", and fails.
func verifyIncomplete(res layout.VerifyResult, err error, stdout, stderr io.Writer) int {
	body := struct {
		chunkErrBody
		Verify layout.VerifyResult `json:"verify"`
	}{Verify: res}
	body.Code, body.Message = codeInternalCLI, err.Error()
	if errors.Is(err, context.DeadlineExceeded) {
		body.Code, body.Retryable = chunkstore.CodeDeadlineExceeded, true
	}
	fmt.Fprintf(stderr, "sw-block ops chunk verify: %s: %s (incomplete)\n", body.Code, body.Message)
	chunkResult("verify", body, nil, stdout, stderr)
	return chunkExitFailed
}
