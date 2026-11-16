package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// chunkVolumeDelete: sw-block ops chunk volume-delete <vol> --chunk-filer
// host:port --master host:port deletes a chunk volume (Catalog.DeleteVolume)
// after the blockmaster admits it (MarkVolumeDeleting: refused while the
// volume has live or pending authority; afterwards nothing assigns, places
// or binds it again). A failure prints what was done with the error.
func chunkVolumeDelete(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("volume-delete", stderr)
	var (
		volumeID, filer, base, master string
		timeout                       time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id (or the first argument)")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.StringVar(&master, "master", "", "authoritative blockmaster gRPC address (admits the delete)")
	fs.DurationVar(&timeout, "timeout", time.Minute, "bound for this delete")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" && fs.NArg() == 1 {
		volumeID = fs.Arg(0)
	} else if fs.NArg() != 0 {
		return chunkFail("volume-delete", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "give the volume once, as an argument or --volume", false)
	}
	if volumeID == "" || master == "" || timeout <= 0 {
		return chunkFail("volume-delete", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "a volume and --master are required; --timeout must be positive", false)
	}
	cat, closeFn, exit := chunkCatalog("volume-delete", filer, base, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	conn, err := grpc.NewClient(master, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return chunkResult("volume-delete", nil, err, stdout, stderr)
	}
	defer conn.Close()
	lifecycle := control.NewLifecycleServiceClient(conn)
	admit := func(ctx context.Context, vol string) error {
		_, err := lifecycle.MarkVolumeDeleting(ctx, &control.MarkVolumeDeletingRequest{VolumeId: vol})
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := cat.DeleteVolume(ctx, volumeID, admit)
	if err != nil {
		if red := true; red { // RED: partial effects dropped
			return chunkResult("volume-delete", nil, err, stdout, stderr)
		}
		return volumeDeletePartial(res, err, stdout, stderr)
	}
	return chunkResult("volume-delete", res, nil, stdout, stderr)
}

// volumeDeletePartial prints what a failed delete did, with the error, and
// fails.
func volumeDeletePartial(res chunkstore.VolumeDeleteResult, err error, stdout, stderr io.Writer) int {
	body := chunkErrBody{Code: codeInternalCLI, Message: err.Error()}
	var ce *chunkstore.CatalogError
	if errors.As(err, &ce) {
		body = chunkErrBody{Code: ce.Code, Message: ce.Detail(), Retryable: ce.Retryable}
	}
	fmt.Fprintf(stderr, "sw-block ops chunk volume-delete: %s: %s (headMarked=%t completed=%t uncertain=%t)\n", body.Code, body.Message, res.HeadMarked, res.Completed, res.Uncertain)
	out, mErr := json.Marshal(struct {
		chunkstore.VolumeDeleteResult
		Error *chunkErrBody `json:"error"`
	}{res, &body})
	if mErr == nil {
		if wErr := writeAll(stdout, append(out, '\n')); wErr != nil {
			fmt.Fprintf(stderr, "sw-block ops chunk volume-delete: write the result: %v\n", wErr)
		}
	}
	return chunkExitFailed
}
