package auth

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Rule is who may call one method: people from Min upwards (viewer <
// operator < admin; "" admits no person), and nodes when Node is set (a
// handler then checks the node's own server id with RequireNode).
type Rule struct {
	Min  Role
	Node bool
}

func rank(r Role) int {
	switch r {
	case RoleViewer:
		return 1
	case RoleOperator:
		return 2
	case RoleAdmin:
		return 3
	}
	return 0
}

// Allows reports whether p may call a method with this rule.
func (r Rule) Allows(p Principal) bool {
	if p.Role == RoleNode {
		return r.Node
	}
	return r.Min != "" && rank(p.Role) >= rank(r.Min)
}

type ctxKey struct{}

// callAuth is what the interceptor decided for one call: the principal, or
// development mode (plaintext on loopback, no role file).
type callAuth struct {
	p   Principal
	dev bool
}

// FromContext returns the call's principal (false in development mode or
// when no interceptor ran).
func FromContext(ctx context.Context) (Principal, bool) {
	c, ok := ctx.Value(ctxKey{}).(callAuth)
	return c.p, ok && !c.dev
}

func forbidden(format string, args ...any) error {
	return status.Errorf(codes.PermissionDenied, format, args...)
}

// decided is the call's decision; a call no interceptor saw is refused, so a
// handler's own checks fail closed if the server was built without one.
func decided(ctx context.Context) (callAuth, error) {
	c, ok := ctx.Value(ctxKey{}).(callAuth)
	if !ok {
		return callAuth{}, unauthenticated("no caller identity on this call")
	}
	return c, nil
}

// RequireNode admits a node calling for its own server id (development mode
// admits every call). A person, or a node naming another server id, is
// refused.
func RequireNode(ctx context.Context, serverID string) error {
	c, err := decided(ctx)
	if err != nil || c.dev {
		return err
	}
	if c.p.Role != RoleNode {
		return forbidden("%s %s is not a node", c.p.Role, c.p.ID)
	}
	if c.p.ID != serverID {
		return forbidden("node %s may act only for its own server id, not %q", c.p.ID, serverID)
	}
	return nil
}

// RequireRole admits a person with at least min (development mode admits
// every call).
func RequireRole(ctx context.Context, min Role) error {
	c, err := decided(ctx)
	if err != nil || c.dev {
		return err
	}
	if !(Rule{Min: min}).Allows(c.p) {
		return forbidden("%s %s may not do this: it needs %s", c.p.Role, c.p.ID, min)
	}
	return nil
}

func (a *Authenticator) admit(ctx context.Context, method string, rules map[string]Rule) (context.Context, error) {
	p, err := a.PeerIdentity(ctx)
	if err != nil {
		return nil, err
	}
	r, ok := rules[method]
	if !ok {
		return nil, forbidden("method %s has no authorization rule", method)
	}
	if !r.Allows(p) {
		return nil, forbidden("%s %s may not call %s", p.Role, p.ID, method)
	}
	return context.WithValue(ctx, ctxKey{}, callAuth{p: p}), nil
}

// UnaryInterceptor authenticates every call (PeerIdentity) and applies its
// method's rule.
func (a *Authenticator) UnaryInterceptor(rules map[string]Rule) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := a.admit(ctx, info.FullMethod, rules)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

// StreamInterceptor authenticates a stream when it opens, again before every
// message it sends, and again at every role-file reload: a caller removed
// from the file loses its open stream at the reload (UNAUTHENTICATED), not at
// the next message.
func (a *Authenticator) StreamInterceptor(rules map[string]Rule) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		changed := a.Changed()
		ctx, err := a.admit(ss.Context(), info.FullMethod, rules)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-changed:
					changed = a.Changed()
					if _, err := a.PeerIdentity(ss.Context()); err != nil {
						cancel(err)
						return
					}
				}
			}
		}()
		err = handler(srv, &authStream{ServerStream: ss, ctx: ctx, a: a})
		if cause := context.Cause(ctx); cause != nil && status.Code(cause) == codes.Unauthenticated {
			return cause
		}
		return err
	}
}

type authStream struct {
	grpc.ServerStream
	ctx context.Context
	a   *Authenticator
}

func (s *authStream) Context() context.Context { return s.ctx }

func (s *authStream) SendMsg(m any) error {
	if _, err := s.a.PeerIdentity(s.ServerStream.Context()); err != nil {
		return err
	}
	return s.ServerStream.SendMsg(m)
}

// DevUnaryInterceptor and DevStreamInterceptor mark every call as
// development mode (plaintext on loopback, no role file): RequireNode and
// RequireRole then admit it.
func DevUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(context.WithValue(ctx, ctxKey{}, callAuth{dev: true}), req)
	}
}

func DevStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return handler(srv, &devStream{ServerStream: ss, ctx: context.WithValue(ss.Context(), ctxKey{}, callAuth{dev: true})})
	}
}

type devStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *devStream) Context() context.Context { return s.ctx }
