package main

import (
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/ops"
)

// chunkRestore starts (or joins) the independent-restore driver of a volume
// restored from a snapshot and prints its status; with --status it only
// prints the status. The driver runs in blockvolume and outlives this call.
func chunkRestore(args []string, stdout, stderr io.Writer) int {
	fs := chunkFlagSet("restore", stderr)
	var (
		volumeID, statusAddr string
		statusOnly           bool
	)
	fs.StringVar(&volumeID, "volume", "", "restored volume id")
	fs.StringVar(&statusAddr, "status-addr", "", "blockvolume status address or URL (with --chunk-control-endpoint)")
	fs.BoolVar(&statusOnly, "status", false, "print the restore status without starting a run")
	if err := fs.Parse(args); err != nil {
		return chunkFail(chunkName(fs), stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, err.Error(), false)
	}
	if volumeID == "" || statusAddr == "" {
		return chunkFail("restore", stdout, stderr, ops.VolumeStatusExitInvalid, codeBadRequestCLI, "--volume and --status-addr are required", false)
	}
	body := map[string]any{"volumeID": volumeID, "statusOnly": statusOnly}
	return chunkPost("restore", statusAddr, "/chunk/restore", 30*time.Second, body, stdout, stderr)
}
