package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/frontend"
)

type primaryOpenProvider interface {
	Open(context.Context, string) (frontend.Backend, error)
}

func startReadyAssignmentLoop(ch <-chan adapter.AssignmentInfo, f flags, prov primaryOpenProvider, stdout, stderr io.Writer) {
	go func() {
		for info := range ch {
			handleReadyAssignment(info, f, prov, stdout, stderr)
		}
	}()
}

func handleReadyAssignment(info adapter.AssignmentInfo, f flags, prov primaryOpenProvider, stdout, stderr io.Writer) {
	if f.printReadyLine && stdout != nil {
		_ = json.NewEncoder(stdout).Encode(readyLine{
			Component:       "blockvolume",
			Phase:           "assignment-received",
			VolumeID:        info.VolumeID,
			ReplicaID:       info.ReplicaID,
			Epoch:           info.Epoch,
			EndpointVersion: info.EndpointVersion,
		})
	}
	if prov == nil || info.VolumeID != f.volumeID || info.ReplicaID != f.replicaID || info.Epoch == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := prov.Open(ctx, info.VolumeID); err != nil {
		if stderr != nil {
			fmt.Fprintf(stderr, "blockvolume: durable primary lineage ensure failed volume=%s replica=%s epoch=%d ev=%d: %v\n",
				info.VolumeID, info.ReplicaID, info.Epoch, info.EndpointVersion, err)
		}
		return
	}
	if stderr != nil {
		fmt.Fprintf(stderr, "blockvolume: durable primary lineage ensured volume=%s replica=%s epoch=%d ev=%d\n",
			info.VolumeID, info.ReplicaID, info.Epoch, info.EndpointVersion)
	}
}
