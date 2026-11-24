package master

import (
	"github.com/seaweedfs/seaweed-block/core/auth"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"google.golang.org/grpc"
)

// masterRules is the authorization table of every blockmaster gRPC method
// (phase 0): the minimum person role, and whether a node may call it. A node
// call's own-server-id check is the handler's (auth.RequireNode); a forced
// move's admin check is MoveWorkspace's (auth.RequireRole). A method missing
// here is refused (TestMasterAuthorizationTableCoversEveryMethod).
var masterRules = map[string]auth.Rule{
	control.ObservationService_ReportHeartbeat_FullMethodName:    {Node: true},
	control.ObservationService_ReportClusterEvent_FullMethodName: {Min: auth.RoleOperator},

	control.AssignmentService_SubscribeAssignments_FullMethodName: {Node: true},

	control.EvidenceService_QueryVolumeStatus_FullMethodName: {Min: auth.RoleViewer, Node: true},

	control.ClusterEvidenceService_GetClusterStatus_FullMethodName:   {Min: auth.RoleViewer},
	control.ClusterEvidenceService_ListVolumes_FullMethodName:        {Min: auth.RoleViewer},
	control.ClusterEvidenceService_GetVolumeStatus_FullMethodName:    {Min: auth.RoleViewer},
	control.ClusterEvidenceService_GetVolumeTimeline_FullMethodName:  {Min: auth.RoleViewer},
	control.ClusterEvidenceService_WatchClusterEvents_FullMethodName: {Min: auth.RoleViewer},

	control.LifecycleService_CreateVolume_FullMethodName:         {Min: auth.RoleOperator},
	control.LifecycleService_DeleteVolume_FullMethodName:         {Min: auth.RoleAdmin},
	control.LifecycleService_MarkVolumeDeleting_FullMethodName:   {Min: auth.RoleAdmin},
	control.LifecycleService_AbortVolumeDeleting_FullMethodName:  {Min: auth.RoleAdmin},
	control.LifecycleService_CommitVolumeDeleting_FullMethodName: {Min: auth.RoleAdmin},

	control.FailbackService_ExecuteFailback_FullMethodName: {Min: auth.RoleAdmin},

	control.WorkspaceService_RegisterWorkspaceBinding_FullMethodName: {Min: auth.RoleAdmin},
	control.WorkspaceService_PlaceWorkspace_FullMethodName:           {Min: auth.RoleOperator},
	control.WorkspaceService_RenewWorkspaceLease_FullMethodName:      {Node: true},
	control.WorkspaceService_AckWorkspaceRevoked_FullMethodName:      {Node: true},
	control.WorkspaceService_RevokeWorkspace_FullMethodName:          {Min: auth.RoleOperator},
	control.WorkspaceService_MoveWorkspace_FullMethodName:            {Min: auth.RoleOperator}, // forced: admin, in the handler
	control.WorkspaceService_QueryWorkspace_FullMethodName:           {Min: auth.RoleViewer},
	control.WorkspaceService_QueryWorkspaceLostRange_FullMethodName:  {Min: auth.RoleViewer},
	control.WorkspaceService_PreviewWorkspaceMove_FullMethodName:     {Min: auth.RoleViewer},
}

// serverAuth returns the interceptor chain of the master's gRPC server: the
// authenticator's when authentication is configured, else development mode
// (plaintext, loopback only; every call marked as such).
func serverAuth(a *auth.Authenticator) link.ServerConfig {
	if a == nil {
		return link.ServerConfig{
			Unary:  []grpc.UnaryServerInterceptor{auth.DevUnaryInterceptor()},
			Stream: []grpc.StreamServerInterceptor{auth.DevStreamInterceptor()},
		}
	}
	return link.ServerConfig{
		Unary:  []grpc.UnaryServerInterceptor{a.UnaryInterceptor(masterRules)},
		Stream: []grpc.StreamServerInterceptor{a.StreamInterceptor(masterRules)},
	}
}
