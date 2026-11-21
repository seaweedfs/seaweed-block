package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// chunkUsage: sw-block ops chunk usage --volume v --chunk-filer host:port
// prints the volume's space accounting and retention policy (from
// manifests and filer listings; no chunk data reads).
func chunkUsage(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("usage", stderr)
	var (
		volumeID, filer, base string
		timeout               time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.DurationVar(&timeout, "timeout", 60*time.Second, "filer timeout")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" {
		return chunkFail("usage", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume is required", false)
	}
	cat, closeFn, exit := chunkCatalog("usage", filer, base, false, timeout, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	u, err := cat.Usage(ctx, volumeID)
	return chunkResult("usage", u, err, stdout, stderr)
}

// chunkRetain: sw-block ops chunk retain --volume v --chunk-filer host:port
// [--dry-run] applies the volume's retention policy (deletes the expired
// snapshots; GC reclaims their chunks). With --set-policy it stores the
// policy given by the --keep-* flags on the VOLUME entry instead.
func chunkRetain(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("retain", stderr)
	var (
		volumeID, filer, base                         string
		timeout, keepYounger                          time.Duration
		dryRun, setPolicy                             bool
		keepNewest, keepHourly, keepDaily, keepWeekly int
	)
	fs.StringVar(&volumeID, "volume", "", "volume id")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.DurationVar(&timeout, "timeout", 5*time.Minute, "bound for this pass")
	fs.BoolVar(&dryRun, "dry-run", false, "list what would be deleted; write nothing")
	fs.BoolVar(&setPolicy, "set-policy", false, "store the --keep-* policy on the volume instead of applying it")
	fs.IntVar(&keepNewest, "keep-newest", chunkstore.DefaultKeepNewest, "with --set-policy: keep the newest N unpinned snapshots")
	fs.DurationVar(&keepYounger, "keep-younger", chunkstore.DefaultKeepYounger, "with --set-policy: keep every snapshot younger than this")
	fs.IntVar(&keepHourly, "keep-hourly", 0, "with --set-policy: keep the newest snapshot of each of the last N hours")
	fs.IntVar(&keepDaily, "keep-daily", 0, "with --set-policy: keep the newest snapshot of each of the last N days")
	fs.IntVar(&keepWeekly, "keep-weekly", 0, "with --set-policy: keep the newest snapshot of each of the last N weeks")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || timeout <= 0 {
		return chunkFail("retain", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume is required and --timeout must be positive", false)
	}
	if setPolicy && dryRun {
		return chunkFail("retain", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--set-policy and --dry-run do not combine", false)
	}
	cat, closeFn, exit := chunkCatalog("retain", filer, base, !dryRun, timeout, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if setPolicy {
		p := chunkstore.RetentionPolicy{KeepNewest: keepNewest, KeepYoungerNs: int64(keepYounger), KeepHourly: keepHourly, KeepDaily: keepDaily, KeepWeekly: keepWeekly}
		if err := cat.SetRetentionPolicy(ctx, volumeID, p); err != nil {
			return chunkResult("retain", nil, err, stdout, stderr)
		}
		return chunkResult("retain", map[string]any{"volumeID": volumeID, "policy": p, "policySet": true}, nil, stdout, stderr)
	}
	res, err := cat.Retain(ctx, volumeID, dryRun)
	if err != nil && res.VolumeID != "" {
		// The pass stopped part way: report what it did (confirmed
		// deletions, the failing snapshot with whether its outcome is
		// uncertain, what it never reached) with the error, and fail.
		return retainPartial(res, err, stdout, stderr)
	}
	return chunkResult("retain", res, err, stdout, stderr)
}

// retainOutput is a retention result together with the error that
// stopped it.
type retainOutput struct {
	chunkstore.RetentionResult
	Error *chunkErrBody `json:"error,omitempty"`
}

func retainPartial(res chunkstore.RetentionResult, err error, stdout, stderr io.Writer) int {
	body := chunkErrBody{Code: chunkstore.CodeOf(err), Message: err.Error(), Retryable: true}
	var ce *chunkstore.CatalogError
	if errors.As(err, &ce) {
		body = chunkErrBody{Code: ce.Code, Message: ce.Detail(), Retryable: ce.Retryable}
	}
	if body.Code == "" {
		body.Code = codeInternalCLI
	}
	fmt.Fprintf(stderr, "sw-block ops chunk retain: %s: %s (deleted %d, not attempted %d)\n", body.Code, body.Message, len(res.Deleted), len(res.NotAttempted))
	out, mErr := json.Marshal(retainOutput{RetentionResult: res, Error: &body})
	if mErr != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk retain: %v\n", mErr)
		return chunkExitFailed
	}
	if wErr := writeAll(stdout, append(out, '\n')); wErr != nil {
		fmt.Fprintf(stderr, "sw-block ops chunk retain: write the result: %v\n", wErr)
	}
	return chunkExitFailed
}
