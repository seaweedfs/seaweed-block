package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	coresnapshot "github.com/seaweedfs/seaweed-block/core/snapshot"
)

func runRestoreDiscardCommand(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("blockvolume restore-discard", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var req coresnapshot.RestoreDiscardRequest
	var evidenceFile string
	fs.StringVar(&req.RootPath, "root", "", "offline replica durable root (required)")
	fs.StringVar(&req.OperationID, "operation-id", "", "durable abort operation identity (required)")
	fs.StringVar(&req.SnapshotID, "snapshot-id", "", "source snapshot identity (required)")
	fs.StringVar(&req.TargetVolumeID, "volume-id", "", "restore target volume identity (required)")
	fs.StringVar(&req.TargetReplicaID, "replica-id", "", "restore target replica identity (required)")
	fs.BoolVar(&req.AllowActivated, "allow-activated", false, "confirm the caller fenced authority and stopped the activated workload")
	fs.StringVar(&evidenceFile, "evidence-file", "", "optional file receiving the same terminal JSON evidence")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(stderr, "blockvolume restore-discard: unexpected positional arguments")
		return 2
	}
	result, err := coresnapshot.DiscardRestoreTarget(req)
	if err != nil {
		fmt.Fprintln(stderr, "blockvolume restore-discard:", err)
		return 1
	}
	raw, err := json.Marshal(result)
	if err != nil {
		fmt.Fprintln(stderr, "blockvolume restore-discard: encode evidence:", err)
		return 1
	}
	if evidenceFile != "" {
		if err := os.WriteFile(evidenceFile, append(raw, '\n'), 0o600); err != nil {
			fmt.Fprintln(stderr, "blockvolume restore-discard: write evidence:", err)
			return 1
		}
	}
	if _, err := fmt.Fprintln(stdout, string(raw)); err != nil {
		fmt.Fprintln(stderr, "blockvolume restore-discard: write evidence:", err)
		return 1
	}
	return 0
}
