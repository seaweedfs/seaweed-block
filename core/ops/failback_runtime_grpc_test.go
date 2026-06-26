package ops

import (
	"context"
	"net"
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
)

func TestGRPCFailbackRuntimeCallsFailbackService(t *testing.T) {
	server := grpc.NewServer()
	fake := &fakeFailbackService{}
	control.RegisterFailbackServiceServer(server, fake)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		done <- server.Serve(ln)
	}()
	t.Cleanup(func() {
		server.Stop()
		_ = ln.Close()
		<-done
	})

	result, err := NewGRPCFailbackRuntime(ln.Addr().String()).ExecuteFailback(context.Background(), FailbackRuntimeRequest{
		VolumeID:                     "pvc-demo",
		ReplicaID:                    "r1",
		TargetDataAddr:               "data-r1",
		TargetCtrlAddr:               "ctrl-r1",
		ExpectedCurrentReplicaID:     "r2",
		ExpectedCurrentEpoch:         7,
		AckEligible:                  true,
		FrontendFencedBeforeFailback: true,
		DurableFrontierCovered:       true,
		NoCrossVolumeIdentityChange:  true,
		EvidenceRefs:                 []string{"target.txt"},
	})
	if err != nil {
		t.Fatalf("ExecuteFailback: %v", err)
	}
	if fake.request.GetVolumeId() != "pvc-demo" ||
		fake.request.GetReplicaId() != "r1" ||
		fake.request.GetTargetDataAddr() != "data-r1" ||
		fake.request.GetTargetCtrlAddr() != "ctrl-r1" ||
		fake.request.GetExpectedCurrentReplicaId() != "r2" ||
		fake.request.GetExpectedCurrentEpoch() != 7 ||
		!fake.request.GetAckEligible() ||
		!fake.request.GetFrontendFencedBeforeFailback() ||
		!fake.request.GetDurableFrontierCovered() ||
		!fake.request.GetNoCrossVolumeIdentityChange() {
		t.Fatalf("request=%+v", fake.request)
	}
	if !result.FailbackStarted ||
		!result.AuthorityEpochAdvanced ||
		!result.SinglePrimaryAfterFailback ||
		!result.PublishTargetSwappedAfterFailback ||
		!result.NoStorageMutation ||
		!result.NoCrossVolumeIdentityChange ||
		!failbackExecutorStringSliceContains(result.EvidenceRefs, "grpc-runtime.txt") {
		t.Fatalf("result=%+v", result)
	}
}

func TestGRPCFailbackRuntimeRequiresAddress(t *testing.T) {
	_, err := NewGRPCFailbackRuntime("").ExecuteFailback(context.Background(), FailbackRuntimeRequest{})
	if err == nil || err.Error() != "failback runtime gRPC address is required" {
		t.Fatalf("err=%v", err)
	}
}

type fakeFailbackService struct {
	control.UnimplementedFailbackServiceServer
	request *control.FailbackRequest
}

func (f *fakeFailbackService) ExecuteFailback(_ context.Context, req *control.FailbackRequest) (*control.FailbackResponse, error) {
	f.request = req
	return &control.FailbackResponse{
		FailbackStarted:                   true,
		AuthorityEpochAdvanced:            true,
		SinglePrimaryAfterFailback:        true,
		PublishTargetSwappedAfterFailback: true,
		NoStorageMutation:                 true,
		NoCrossVolumeIdentityChange:       true,
		EvidenceRefs:                      []string{"grpc-runtime.txt"},
	}, nil
}
