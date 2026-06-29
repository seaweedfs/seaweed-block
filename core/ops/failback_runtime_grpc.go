package ops

import (
	"context"
	"fmt"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCFailbackRuntime struct {
	Addr string
	Opts []grpc.DialOption
}

func NewGRPCFailbackRuntime(addr string, opts ...grpc.DialOption) *GRPCFailbackRuntime {
	return &GRPCFailbackRuntime{Addr: addr, Opts: append([]grpc.DialOption(nil), opts...)}
}

func (r *GRPCFailbackRuntime) ExecuteFailback(ctx context.Context, req FailbackRuntimeRequest) (FailbackRuntimeResult, error) {
	if r == nil || r.Addr == "" {
		return FailbackRuntimeResult{}, fmt.Errorf("failback runtime gRPC address is required")
	}
	opts := append([]grpc.DialOption(nil), r.Opts...)
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	conn, err := grpc.NewClient(r.Addr, opts...)
	if err != nil {
		return FailbackRuntimeResult{}, err
	}
	defer conn.Close()

	resp, err := control.NewFailbackServiceClient(conn).ExecuteFailback(ctx, &control.FailbackRequest{
		VolumeId:                     req.VolumeID,
		ReplicaId:                    req.ReplicaID,
		TargetDataAddr:               req.TargetDataAddr,
		TargetCtrlAddr:               req.TargetCtrlAddr,
		ExpectedCurrentReplicaId:     req.ExpectedCurrentReplicaID,
		ExpectedCurrentEpoch:         req.ExpectedCurrentEpoch,
		AckEligible:                  req.AckEligible,
		FrontendFencedBeforeFailback: req.FrontendFencedBeforeFailback,
		DurableFrontierCovered:       req.DurableFrontierCovered,
		NoCrossVolumeIdentityChange:  req.NoCrossVolumeIdentityChange,
		EvidenceRefs:                 append([]string(nil), req.EvidenceRefs...),
	})
	if err != nil {
		return FailbackRuntimeResult{}, err
	}
	return FailbackRuntimeResult{
		FailbackStarted:                   resp.GetFailbackStarted(),
		AuthorityEpochAdvanced:            resp.GetAuthorityEpochAdvanced(),
		SinglePrimaryAfterFailback:        resp.GetSinglePrimaryAfterFailback(),
		PublishTargetSwappedAfterFailback: resp.GetPublishTargetSwappedAfterFailback(),
		NoStorageMutation:                 resp.GetNoStorageMutation(),
		NoCrossVolumeIdentityChange:       resp.GetNoCrossVolumeIdentityChange(),
		EvidenceRefs:                      append([]string(nil), resp.GetEvidenceRefs()...),
	}, nil
}
