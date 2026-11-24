package auth_test

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodexStreamRoleChangeMustReauthorizeBeforeSend(t *testing.T) {
	for _, tc := range []struct {
		name, replacement string
		want              codes.Code
	}{
		{"removed", "", codes.Unauthenticated},
		{"node_becomes_viewer", "viewer", codes.PermissionDenied},
		{"unchanged_node", "node", codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ca := authtest.NewCA(t, "ca")
			node := ca.Issue(t, "s1", time.Now().Add(time.Hour))
			a := auth.NewAuthenticator(table(t, node.Fingerprint+" node s1"), nil)
			ss := &fakeStream{ctx: peerCtx(node, ca)}
			rules := map[string]auth.Rule{"/node-stream": {Node: true}}
			err := a.StreamInterceptor(rules)(nil, ss, &grpc.StreamServerInfo{FullMethod: "/node-stream"}, func(_ any, s grpc.ServerStream) error {
				if err := auth.RequireNode(s.Context(), "s1"); err != nil {
					return err
				}
				next := "# caller removed"
				if tc.replacement != "" {
					next = node.Fingerprint + " " + tc.replacement + " s1"
				}
				a.Swap(table(t, next))
				return s.SendMsg("message after reload")
			})
			if status.Code(err) != tc.want {
				t.Fatalf("after role reload: err=%v code=%v sent=%d; want %v", err, status.Code(err), ss.sent, tc.want)
			}
		})
	}
}
