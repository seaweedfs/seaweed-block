package main

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/ops"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// chunkVolumeDelete: sw-block ops chunk volume-delete <vol> --chunk-filer
// host:port --master host:port deletes a chunk volume (Catalog.DeleteVolume):
// refused while it is attached, assigned or served (volume_in_use) or while
// a live fork claims one of its snapshots (snapshot_claimed); GC reclaims.
func chunkVolumeDelete(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("volume-delete", stderr)
	var (
		volumeID, filer, base, master string
		timeout                       time.Duration
	)
	fs.StringVar(&volumeID, "volume", "", "volume id (or the first argument)")
	fs.StringVar(&filer, "chunk-filer", "", "SeaweedFS filer gRPC address host:port")
	fs.StringVar(&base, "chunk-path", defaultChunkPathCLI, "filer directory holding one directory per volume")
	fs.StringVar(&master, "master", "", "authoritative blockmaster gRPC address (refuses an assigned or served volume)")
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
	evidence, closeMaster, err := masterEvidence(master)
	if err != nil {
		return chunkResult("volume-delete", nil, err, stdout, stderr)
	}
	defer closeMaster()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	res, err := cat.DeleteVolume(ctx, volumeID, masterInUse(evidence))
	return chunkResult("volume-delete", res, err, stdout, stderr)
}

// masterInUse: the blockmaster has the volume assigned, or a frontend
// serves it.
func masterInUse(evidence control.EvidenceServiceClient) chunkstore.InUseCheck {
	return func(ctx context.Context, vol string) (bool, string, error) {
		st, err := evidence.QueryVolumeStatus(ctx, &control.StatusRequest{VolumeId: vol})
		if err != nil {
			return false, "", err
		}
		if st.GetAssigned() {
			return true, fmt.Sprintf("the blockmaster has it assigned (replica %q, epoch %d)", st.GetReplicaId(), st.GetEpoch()), nil
		}
		if n := len(st.GetFrontends()); n > 0 {
			return true, fmt.Sprintf("%d frontend(s) serve it", n), nil
		}
		return false, "", nil
	}
}
