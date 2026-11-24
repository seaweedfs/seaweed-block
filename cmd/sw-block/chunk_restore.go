package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
)

// chunkRestore restores a snapshot into a new independent volume, or drives
// an existing restore.
//
// With --source, --snapshot and --target it creates the target volume from
// the snapshot (Catalog.Restore, idempotent by --request-id) and, when
// --status-addr names the blockvolume serving the target, starts the restore
// driver there and prints its status (the create result goes to stderr). With --volume and --status-addr it
// starts (or joins) the driver of an existing restore and prints its status;
// --status only prints the status. The driver runs in blockvolume and
// outlives this call.
func chunkRestore(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("restore", stderr)
	var (
		volumeID, statusAddr                             string
		filer, base, source, snapshot, target, requestID string
		statusOnly                                       bool
		timeout                                          time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "restored volume id (drive an existing restore)")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume status address or URL serving the restored volume (with --chunk-control-endpoint)")
	fs.BoolVar(&statusOnly, "status", false, "print the restore status without starting a run")
	fs.StringVar(&source, "source", "", "source volume id (create a restore)")
	fs.StringVar(&snapshot, "snapshot", "", "source snapshot name (create a restore)")
	fs.StringVar(&target, "target", "", "new volume id (create a restore)")
	fs.StringVar(&requestID, "request-id", "", "idempotency id for the create; generated and printed when empty (reuse it to retry)")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port (create a restore)")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	place := chunkPlacementFlags(fs)
	fs.DurationVar(&timeout, "timeout", 6*time.Minute, "filer timeout for the create")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	create := source != "" || snapshot != "" || target != ""
	switch {
	case create && volumeID != "":
		return chunkFail("restore", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "use --source/--snapshot/--target to create a restore, or --volume to drive one, not both", false)
	case create && (source == "" || snapshot == "" || target == ""):
		return chunkFail("restore", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--source, --snapshot and --target are required to create a restore", false)
	case create && statusOnly:
		return chunkFail("restore", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--status reads an existing restore: use it with --volume", false)
	case !create && (volumeID == "" || statusAddr == ""):
		return chunkFail("restore", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume and --status-addr are required (or --source, --snapshot and --target to create a restore)", false)
	}
	if !create {
		body := map[string]any{"volumeID": volumeID, "statusOnly": statusOnly}
		return chunkPost("restore", statusAddr, "/chunk/restore", 30*time.Second, body, stdout, stderr)
	}

	if requestID == "" {
		requestID = newRequestID()
		fmt.Fprintf(stderr, "sw-block ops chunk restore: request id %s (reuse it to retry)\n", requestID)
	}
	cat, closeFn, exit := chunkCatalog("restore", filer, base, place, true, timeout, stdout, stderr)
	if exit >= 0 {
		return exit
	}
	defer closeFn()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := cat.Restore(ctx, chunkstore.ForkRequest{Source: source, Snapshot: snapshot, Target: target, RequestID: requestID})
	if err != nil || statusAddr == "" {
		if err == nil {
			fmt.Fprintf(stderr, "sw-block ops chunk restore: %s created; attach it, then run: sw-block ops chunk restore --volume %s --status-addr <its blockvolume>\n", target, target)
		}
		return chunkResult("restore", res, err, stdout, stderr)
	}
	created, err := json.Marshal(res)
	if err != nil {
		return chunkFail("restore", stdout, stderr, chunkExitFailed, codeInternalCLI, err.Error(), false)
	}
	// stdout stays one JSON document (the driver status); the create result
	// goes to stderr.
	fmt.Fprintf(stderr, "sw-block ops chunk restore: created %s\n", created)
	body := map[string]any{"volumeID": target}
	code := chunkPost("restore", statusAddr, "/chunk/restore", 30*time.Second, body, stdout, stderr)
	if code != ops.VolumeStatusExitOK {
		fmt.Fprintf(stderr, "sw-block ops chunk restore: %s was created, but the restore did not start at %s (is the target attached there?); once it is, run: sw-block ops chunk restore --volume %s --status-addr <its blockvolume>\n", target, statusAddr, target)
	}
	return code
}
