package csi

import (
	"bytes"
	"context"
	"log"
	"strings"
	"testing"

	csipb "github.com/container-storage-interface/spec/lib/go/csi"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type countingObservationClient struct {
	fakeObservationClient
	calls int
}

func (c *countingObservationClient) ReportClusterEvent(ctx context.Context, event *control.ClusterEvent, opts ...grpc.CallOption) (*control.ClusterEventAck, error) {
	c.calls++
	return c.fakeObservationClient.ReportClusterEvent(ctx, event, opts...)
}

// A CSI node holds a viewer certificate, so blockmaster refuses its reattach
// event: the stage still succeeds, the refusal is one local log line, and the
// event is not retried.
func TestNodeStage_RefusedReattachEventIsOneLogLineNoRetry(t *testing.T) {
	mi, mm := newMockISCSIUtil(), newMockMountUtil()
	lookup := &sequenceLookup{targets: []PublishTarget{{
		VolumeID:  "v1",
		ReplicaID: "r2",
		Epoch:     2,
		Protocol:  ProtocolISCSI,
		ISCSIAddr: "127.0.0.1:3261",
		IQN:       "iqn.lookup:v1",
	}}}
	client := &countingObservationClient{fakeObservationClient: fakeObservationClient{
		err: status.Error(codes.PermissionDenied, "viewer may not call ReportClusterEvent"),
	}}
	var logs bytes.Buffer
	ns := NewNodeServer(NodeConfig{
		NodeID:        "node-a",
		IQNPrefix:     "iqn.2026-05.example.v3",
		ISCSIUtil:     mi,
		MountUtil:     mm,
		Lookup:        lookup,
		EventReporter: NewControlEventReporter(client),
		Logger:        log.New(&logs, "", 0),
	})

	_, err := ns.NodeStageVolume(context.Background(), &csipb.NodeStageVolumeRequest{
		VolumeId:          "v1",
		StagingTargetPath: t.TempDir(),
		VolumeCapability:  testVolumeCapability(),
		PublishContext:    map[string]string{"iscsiAddr": "127.0.0.1:3260", "iqn": "iqn.stale:v1"},
	})
	if err != nil {
		t.Fatalf("NodeStageVolume with the event refused: %v", err)
	}
	if client.calls != 1 {
		t.Fatalf("ReportClusterEvent called %d times, want once", client.calls)
	}
	var refused []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, EventTypeCSIReattachObserved) && strings.Contains(line, "PermissionDenied") {
			refused = append(refused, line)
		}
	}
	if len(refused) != 1 {
		t.Fatalf("refusal log lines %q, want one\n%s", refused, logs.String())
	}
}
