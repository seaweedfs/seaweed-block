package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodexStrictMutationRefusesWithoutAnAuditLog(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	admin := ca.Issue(t, "root", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, admin.Fingerprint+" admin root"), nil)
	rules := map[string]auth.Rule{"/change": {Min: auth.RoleAdmin, Strict: true}}
	ran := false
	_, err := a.UnaryInterceptor(rules)(peerCtx(admin, ca), nil, &grpc.UnaryServerInfo{FullMethod: "/change"}, func(context.Context, any) (any, error) { ran = true; return nil, nil })
	if status.Code(err) != codes.Unavailable || ran {
		t.Fatalf("no audit log: err=%v handlerRan=%t; want Unavailable and no action", err, ran)
	}
}
