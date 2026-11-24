package auth_test

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestRuleAllows(t *testing.T) {
	p := func(r auth.Role) auth.Principal { return auth.Principal{Role: r, ID: "x"} }
	for _, c := range []struct {
		rule auth.Rule
		role auth.Role
		want bool
	}{
		{auth.Rule{Min: auth.RoleViewer}, auth.RoleViewer, true},
		{auth.Rule{Min: auth.RoleViewer}, auth.RoleAdmin, true},
		{auth.Rule{Min: auth.RoleOperator}, auth.RoleViewer, false},
		{auth.Rule{Min: auth.RoleAdmin}, auth.RoleOperator, false},
		{auth.Rule{Min: auth.RoleAdmin}, auth.RoleNode, false},
		{auth.Rule{Node: true}, auth.RoleNode, true},
		{auth.Rule{Node: true}, auth.RoleAdmin, false},
		{auth.Rule{Min: auth.RoleViewer, Node: true}, auth.RoleNode, true},
	} {
		if got := c.rule.Allows(p(c.role)); got != c.want {
			t.Errorf("%+v allows %s = %v; want %v", c.rule, c.role, got, c.want)
		}
	}
}

// The handler checks: a node only for its own server id; a person only
// with the role; development mode admits; a call no interceptor saw is
// refused (fail closed).
func TestRequireNodeAndRole(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	node := ca.Issue(t, "s1", time.Now().Add(time.Hour))
	op := ca.Issue(t, "alice", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, node.Fingerprint+" node s1", op.Fingerprint+" operator alice"), nil)
	rules := map[string]auth.Rule{"/m": {Min: auth.RoleViewer, Node: true}}
	ctxOf := func(leaf *authtest.Leaf) context.Context {
		var got context.Context
		_, err := a.UnaryInterceptor(rules)(peerCtx(leaf, ca), nil, &grpc.UnaryServerInfo{FullMethod: "/m"}, func(ctx context.Context, _ any) (any, error) {
			got = ctx
			return nil, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	nodeCtx, opCtx := ctxOf(node), ctxOf(op)
	if err := auth.RequireNode(nodeCtx, "s1"); err != nil {
		t.Fatalf("node for its own id: %v", err)
	}
	if err := auth.RequireNode(nodeCtx, "s2"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("node for another id: %v; want PERMISSION_DENIED", err)
	}
	if err := auth.RequireNode(opCtx, "s1"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("operator as a node: %v; want PERMISSION_DENIED", err)
	}
	if err := auth.RequireRole(opCtx, auth.RoleOperator); err != nil {
		t.Fatalf("operator needing operator: %v", err)
	}
	if err := auth.RequireRole(opCtx, auth.RoleAdmin); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("operator needing admin: %v; want PERMISSION_DENIED", err)
	}
	if err := auth.RequireRole(nodeCtx, auth.RoleViewer); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("node needing viewer: %v; want PERMISSION_DENIED", err)
	}
	if err := auth.RequireNode(context.Background(), "s1"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no interceptor ran: %v; want UNAUTHENTICATED (fail closed)", err)
	}
	var devCtx context.Context
	_, _ = auth.DevUnaryInterceptor()(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/m"}, func(ctx context.Context, _ any) (any, error) {
		devCtx = ctx
		return nil, nil
	})
	if auth.RequireNode(devCtx, "anything") != nil || auth.RequireRole(devCtx, auth.RoleAdmin) != nil {
		t.Fatal("development mode must admit")
	}
	if _, ok := auth.FromContext(devCtx); ok {
		t.Fatal("development mode has no principal")
	}
}

// The unary interceptor refuses a caller without a role on the method
// (PERMISSION_DENIED), an unauthenticated one (UNAUTHENTICATED), and any
// method without a rule (fail closed).
func TestUnaryInterceptor(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	op := ca.Issue(t, "alice", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, op.Fingerprint+" operator alice"), nil)
	rules := map[string]auth.Rule{"/viewer": {Min: auth.RoleViewer}, "/admin": {Min: auth.RoleAdmin}}
	call := func(ctx context.Context, method string) error {
		_, err := a.UnaryInterceptor(rules)(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(ctx context.Context, _ any) (any, error) {
			if p, ok := auth.FromContext(ctx); !ok || p.ID != "alice" {
				t.Errorf("handler principal %+v %v", p, ok)
			}
			return nil, nil
		})
		return err
	}
	if err := call(peerCtx(op, ca), "/viewer"); err != nil {
		t.Fatalf("operator on a viewer method: %v", err)
	}
	if err := call(peerCtx(op, ca), "/admin"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("operator on an admin method: %v; want PERMISSION_DENIED", err)
	}
	if err := call(peerCtx(op, ca), "/unlisted"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a method without a rule: %v; want PERMISSION_DENIED", err)
	}
	if err := call(peerCtx(nil, ca), "/viewer"); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no client certificate: %v; want UNAUTHENTICATED", err)
	}
}

type fakeStream struct {
	ctx  context.Context
	sent int
}

func (s *fakeStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeStream) SetTrailer(metadata.MD)       {}
func (s *fakeStream) Context() context.Context     { return s.ctx }
func (s *fakeStream) SendMsg(any) error            { s.sent++; return nil }
func (s *fakeStream) RecvMsg(any) error            { return nil }

// T4: a stream whose caller is removed from the role file ends at the reload
// with UNAUTHENTICATED, without waiting for its next message; a send after an
// expiry is refused too.
func TestStreamEndsAtTheReloadThatRevokesItsCaller(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	node := ca.Issue(t, "s1", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, node.Fingerprint+" node s1"), nil)
	rules := map[string]auth.Rule{"/stream": {Node: true}}
	ss := &fakeStream{ctx: peerCtx(node, ca)}
	done := make(chan error, 1)
	opened := make(chan struct{})
	go func() {
		done <- a.StreamInterceptor(rules)(nil, ss, &grpc.StreamServerInfo{FullMethod: "/stream"}, func(_ any, s grpc.ServerStream) error {
			if err := s.SendMsg("first"); err != nil {
				return err
			}
			close(opened)
			<-s.Context().Done() // an idle stream: no message is pending
			return s.Context().Err()
		})
	}()
	<-opened
	a.Swap(table(t, "# s1 removed"))
	select {
	case err := <-done:
		if status.Code(err) != codes.Unauthenticated {
			t.Fatalf("stream after its caller was removed: %v; want UNAUTHENTICATED", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stream did not end at the reload")
	}
	if ss.sent != 1 {
		t.Fatalf("sent %d messages; want 1", ss.sent)
	}
	// A reload that keeps the caller does not end the stream.
	a2 := auth.NewAuthenticator(table(t, node.Fingerprint+" node s1"), nil)
	ss2 := &fakeStream{ctx: peerCtx(node, ca)}
	release := make(chan struct{})
	done2 := make(chan error, 1)
	go func() {
		done2 <- a2.StreamInterceptor(rules)(nil, ss2, &grpc.StreamServerInfo{FullMethod: "/stream"}, func(_ any, s grpc.ServerStream) error {
			select {
			case <-s.Context().Done():
				return s.Context().Err()
			case <-release:
				return s.SendMsg("after the reload")
			}
		})
	}()
	a2.Swap(table(t, node.Fingerprint+" node s1", "# reloaded"))
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-done2; err != nil {
		t.Fatalf("a reload that keeps the caller ended its stream: %v", err)
	}
}
