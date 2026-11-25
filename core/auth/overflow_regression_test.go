package auth_test

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCodexOverflowWarningDoesNotBlockAReader(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	op := ca.Issue(t, "alice", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, op.Fingerprint+" operator alice"), nil)
	w := newCodexBlockedAudit()
	al := auth.NewAuditLog(w, nil)
	a.SetAudit(al)
	rules := map[string]auth.Rule{"/observe": {Min: auth.RoleViewer}}
	call := func() {
		_, _ = a.UnaryInterceptor(rules)(peerCtx(op, ca), nil, &grpc.UnaryServerInfo{FullMethod: "/observe"}, func(context.Context, any) (any, error) { return nil, nil })
	}
	call()
	<-w.entered
	for i := 0; i < 1024; i++ {
		call()
	}
	burst := newCodexBlockedAudit()
	old := log.Writer()
	log.SetOutput(burst)
	defer log.SetOutput(old)
	done := make(chan struct{})
	go func() { call(); close(done) }()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Error("reader waits on synchronous overflow warning output")
	}
	close(burst.release)
	<-done
	close(w.release)
	al.Flush()
}

func TestCodexStrictQueueFullIsCountedAsADroppedLine(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	admin := ca.Issue(t, "root", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, admin.Fingerprint+" admin root"), nil)
	w := newCodexBlockedAudit()
	al := auth.NewAuditLog(w, nil)
	a.SetAudit(al)
	rules := map[string]auth.Rule{"/observe": {Min: auth.RoleViewer}, "/change": {Min: auth.RoleAdmin, Strict: true}}
	call := func(method string, handler grpc.UnaryHandler) error {
		_, err := a.UnaryInterceptor(rules)(peerCtx(admin, ca), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		return err
	}
	read := func(context.Context, any) (any, error) { return nil, nil }
	_ = call("/observe", read)
	<-w.entered
	for i := 0; i < 1024; i++ {
		_ = call("/observe", read)
	}
	ran := false
	err := call("/change", func(context.Context, any) (any, error) { ran = true; return nil, nil })
	if status.Code(err) != codes.Unavailable || ran {
		t.Errorf("queue-full strict call: err=%v ran=%t", err, ran)
	}
	if n := al.WriteErrors(); n == 0 {
		t.Error("strict attempt discarded on full queue but dropped-line count remains zero")
	}
	close(w.release)
	al.Flush()
}
