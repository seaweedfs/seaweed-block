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
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// chunkVerify: sw-block ops chunk verify --volume v --chunk-filer host:port
// checks the volume's remote image against its HEAD (and, on request, its
// snapshots), read-only (chunkstore.Catalog.Verify). Exit 0: clean; 3: the
// pass completed with findings; 1: it could not complete (the partial result
// is printed with the error); 2: usage.
func chunkVerify(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("verify", stderr)
	var (
		volumeID, filer, base, snapshot, depth string
		allSnapshots                           bool
		concurrency                            int
		bytesPerSec                            int64
		timeout                                time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.StringVar(&snapshot, "snapshot", "", "also check this snapshot")
	fs.BoolVar(&allSnapshots, "all-snapshots", false, "also check every snapshot")
	fs.StringVar(&depth, "depth", string(chunkstore.VerifyBindings), "meta, bindings or data (data reads every chunk once)")
	fs.IntVar(&concurrency, "concurrency", chunkstore.DefaultVerifyConcurrency, "chunk reads in flight (depth data)")
	fs.Int64Var(&bytesPerSec, "bytes-per-sec", chunkstore.DefaultVerifyBytesPerSecond, "pace chunk reads (depth data); 0 leaves them unpaced")
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
	d := chunkstore.VerifyDepth(depth)
	switch {
	case fs.NArg() != 0:
		return usage("unexpected arguments")
	case volumeID == "":
		return usage("--volume is required")
	case d != chunkstore.VerifyMeta && d != chunkstore.VerifyBindings && d != chunkstore.VerifyData:
		return usage(fmt.Sprintf("--depth %q: meta, bindings or data", depth))
	case snapshot != "" && allSnapshots:
		return usage("--snapshot and --all-snapshots do not combine")
	case d == chunkstore.VerifyData && !timeoutSet:
		return usage("--depth data reads every chunk: give an explicit --timeout for the pass")
	case timeout <= 0 || concurrency <= 0 || bytesPerSec < 0:
		return usage("--timeout and --concurrency must be positive and --bytes-per-sec not negative")
	}
	// One bound for the whole pass, the single-filer check included.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cat, closeFn, exit := chunkCatalog(ctx, "verify", filer, base, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	if bytesPerSec == 0 {
		bytesPerSec = -1 // the library's "unpaced"
	}
	res, err := cat.Verify(ctx, volumeID, chunkstore.VerifyOptions{Depth: d, Snapshot: snapshot, AllSnapshots: allSnapshots,
		Concurrency: concurrency, BytesPerSecond: bytesPerSec})
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
	if len(res.Findings) != 0 {
		fmt.Fprintf(stderr, "sw-block ops chunk verify: %s: %d findings %v\n", volumeID, len(res.Findings), res.FindingsByClass)
		if res.ChunksNotRead != 0 {
			// READ_ERROR is not damage: those objects were not verified.
			fmt.Fprintf(stderr, "sw-block ops chunk verify: %s: %d could not be read and were not verified; re-run\n", volumeID, res.ChunksNotRead)
		}
		return chunkExitFindings
	}
	return ops.VolumeStatusExitOK
}

// verifyJSON is the result as one JSON object whose findings array holds
// one finding per line.
func verifyJSON(res chunkstore.VerifyResult) ([]byte, error) {
	findings := res.Findings
	res.Findings = nil
	head, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	const tail = `"findings":null}`
	if !bytes.HasSuffix(head, []byte(tail)) {
		return nil, errors.New("verify result: unexpected field order")
	}
	var buf bytes.Buffer
	buf.Write(head[:len(head)-len(tail)])
	buf.WriteString(`"findings":[`)
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
func verifyIncomplete(res chunkstore.VerifyResult, err error, stdout, stderr io.Writer) int {
	body := struct {
		chunkErrBody
		Verify chunkstore.VerifyResult `json:"verify"`
	}{Verify: res}
	body.Code, body.Message = codeInternalCLI, err.Error()
	var ce *chunkstore.CatalogError
	switch {
	case errors.As(err, &ce):
		body.Code, body.Message, body.Retryable = ce.Code, ce.Detail(), ce.Retryable
	case errors.Is(err, context.DeadlineExceeded):
		body.Code, body.Retryable = chunkstore.CodeDeadlineExceeded, true
	}
	fmt.Fprintf(stderr, "sw-block ops chunk verify: %s: %s (incomplete)\n", body.Code, body.Message)
	chunkResult("verify", body, nil, stdout, stderr)
	return chunkExitFailed
}
