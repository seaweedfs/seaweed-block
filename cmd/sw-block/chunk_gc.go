package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)

// Same clock seam as GCConfig; production always uses wall time. Tests can
// advance grace without sleeping while using actual master/store services.
var chunkGCNow = time.Now

func chunkGC(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("gc", stderr)
	var filer, base, master string
	var apply bool
	var grace, ttl, timeout time.Duration
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "chunk catalog namespace")
	fs.StringVar(&master, "master", "", "authoritative blockmaster gRPC address")
	fs.BoolVar(&apply, "apply", false, "settle intents, save completed marks and reclaim eligible chunks; default is read-only")
	fs.DurationVar(&grace, "grace", time.Hour, "two-mark interval and minimum upload age (at least the operation deadline + clock skew + CAS apply bound, and more than the operation deadline plus mark time)")
	fs.DurationVar(&ttl, "intent-ttl", time.Hour, "minimum capture-intent age before authoritative epoch expiry")
	fs.DurationVar(&timeout, "timeout", time.Minute, "bound for this pass, including master/filer/blob calls")
	if e := fs.Parse(args); e != nil {
		return chunkFail("gc", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, e.Error(), false)
	}
	if fs.NArg() != 0 || filer == "" || master == "" || timeout <= 0 || ttl <= 0 || grace <= chunkstore.DefaultOpDeadline {
		return chunkFail("gc", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--chunk-filer and --master are required; durations must be positive and grace must exceed the operation deadline", false)
	}
	f, closeFn, e := dialChunkFiler(filer)
	if e != nil {
		return chunkResult("gc", nil, e, stdout, stderr)
	}
	defer closeFn()
	if apply {
		if exit := refuseUnlessSingleFiler("gc", f, timeout, stdout, stderr); exit >= 0 {
			return exit
		}
	}
	blobs, ok := f.(chunkstore.BlobDeleter)
	if !ok {
		return chunkFail("gc", stdout, stderr, chunkExitFailed, codeInternalCLI, "chunk store cannot delete immutable blobs", false)
	}
	cat, e := chunkstore.NewCatalog(f, base)
	if e != nil {
		return chunkFail("gc", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, e.Error(), false)
	}
	evidence, closeMaster, e := masterEvidence(master)
	if e != nil {
		return chunkResult("gc", nil, e, stdout, stderr)
	}
	defer closeMaster()
	issuedEpoch := func(ctx context.Context, vol string) (uint64, error) {
		reply, e := evidence.QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: vol})
		if e != nil {
			return 0, e
		}
		if reply.GetVolumeId() != vol || !reply.GetAssigned() || reply.GetEpoch() == 0 {
			return 0, fmt.Errorf("blockmaster has no issued authority for %s", vol)
		}
		return reply.Epoch, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, e := cat.ExecuteGC(ctx, blobs, chunkstore.GCConfig{Grace: grace, IntentTTL: ttl, Now: chunkGCNow, IssuedEpoch: issuedEpoch}, apply)
	return chunkGCResult(out, e, stdout, stderr)
}

// masterEvidence dials the blockmaster's evidence service (gc and
// volume-delete read volume status from it).
func masterEvidence(master string) (control.EvidenceServiceClient, func() error, error) {
	conn, e := link.DialMasterFromEnv(master)
	if e != nil {
		return nil, nil, e
	}
	return control.NewEvidenceServiceClient(conn), conn.Close, nil
}

// Error status and code remain failures, but a failure must not erase known
// effects or imply that a lost write response means the write did not commit.
func chunkGCResult(out chunkstore.GCExecution, err error, stdout, stderr io.Writer) int {
	if err == nil {
		return chunkResult("gc", out, nil, stdout, stderr)
	}
	body := struct {
		chunkErrBody
		GC chunkstore.GCExecution `json:"gc"`
	}{GC: out}
	body.Code, body.Message = codeInternalCLI, err.Error()
	var ce *chunkstore.CatalogError
	switch {
	case errors.As(err, &ce):
		body.Code, body.Message, body.Retryable = ce.Code, ce.Detail(), ce.Retryable
	case errors.Is(err, context.DeadlineExceeded):
		body.Code, body.Retryable = chunkstore.CodeDeadlineExceeded, true
	}
	fmt.Fprintf(stderr, "sw-block ops chunk gc: %s: %s\n", body.Code, body.Message)
	// chunkResult also propagates output errors; either outcome remains FAIL.
	chunkResult("gc", body, nil, stdout, stderr)
	return chunkExitFailed
}
