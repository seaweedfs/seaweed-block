package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	fs.DurationVar(&grace, "grace", time.Hour, "two-mark interval and minimum upload age (must exceed the operation deadline plus mark time)")
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
	blobs, ok := f.(chunkstore.BlobDeleter)
	if !ok {
		return chunkFail("gc", stdout, stderr, chunkExitFailed, codeInternalCLI, "chunk store cannot delete immutable blobs", false)
	}
	cat, e := chunkstore.NewCatalog(f, base)
	if e != nil {
		return chunkFail("gc", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, e.Error(), false)
	}
	conn, e := grpc.NewClient(master, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if e != nil {
		return chunkResult("gc", nil, e, stdout, stderr)
	}
	defer conn.Close()
	evidence := control.NewEvidenceServiceClient(conn)
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
	return chunkResult("gc", out, e, stdout, stderr)
}
