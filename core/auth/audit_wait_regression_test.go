package auth_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc"
)

type codexBlockedAudit struct {
	entered, release chan struct{}
	once             sync.Once
}

func (w *codexBlockedAudit) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(p), nil
}
func newCodexBlockedAudit() *codexBlockedAudit {
	return &codexBlockedAudit{entered: make(chan struct{}), release: make(chan struct{})}
}

func TestCodexAuditWaitDoesNotDelayUnauditedNodeResponse(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	node := ca.Issue(t, "s1", time.Now().Add(time.Hour))
	op := ca.Issue(t, "alice", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, node.Fingerprint+" node s1", op.Fingerprint+" operator alice"), nil)
	w := newCodexBlockedAudit()
	a.SetAudit(auth.NewAuditLog(w, nil))
	rules := map[string]auth.Rule{"/renew": {Node: true}, "/observe": {Min: auth.RoleViewer}}
	call := func(leaf *authtest.Leaf, method string) {
		_, _ = a.UnaryInterceptor(rules)(peerCtx(leaf, ca), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return nil, nil })
	}
	personDone := make(chan struct{})
	go func() { call(op, "/observe"); close(personDone) }()
	<-w.entered
	nodeDone := make(chan struct{})
	go func() { call(node, "/renew"); close(nodeDone) }()
	select {
	case <-nodeDone:
	case <-time.After(100 * time.Millisecond):
		t.Error("accepted unaudited node response waits on another caller's audit I/O")
	}
	close(w.release)
	<-personDone
	<-nodeDone
}

func TestCodexValidatedRoleReloadPublishesBeforeAuditWait(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	node := ca.Issue(t, "s1", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, node.Fingerprint+" node s1"), nil)
	w := newCodexBlockedAudit()
	a.SetAudit(auth.NewAuditLog(w, nil))
	path := filepath.Join(t.TempDir(), "roles")
	if err := os.WriteFile(path, []byte("# s1 removed\n"), 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Reload(path) }()
	<-w.entered
	if _, err := a.PeerIdentity(peerCtx(node, ca)); err == nil {
		t.Error("validated revocation still accepts removed node while audit writer waits")
	}
	close(w.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := a.PeerIdentity(peerCtx(node, ca)); err == nil {
		t.Error("revocation did not publish after writer released")
	}
}

// A stalled audit writer never blocks a caller: past the bounded queue the
// lines are dropped and counted, and every call returns.
func TestAuditQueueFullDropsAndCountsInsteadOfBlocking(t *testing.T) {
	ca := authtest.NewCA(t, "ca")
	op := ca.Issue(t, "alice", time.Now().Add(time.Hour))
	a := auth.NewAuthenticator(table(t, op.Fingerprint+" operator alice"), nil)
	w := newCodexBlockedAudit()
	log := auth.NewAuditLog(w, nil)
	a.SetAudit(log)
	rules := map[string]auth.Rule{"/observe": {Min: auth.RoleViewer}}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1100; i++ { // more than the queue holds
			_, _ = a.UnaryInterceptor(rules)(peerCtx(op, ca), nil, &grpc.UnaryServerInfo{FullMethod: "/observe"}, func(context.Context, any) (any, error) { return nil, nil })
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(w.release)
		t.Fatal("audited calls blocked on a stalled writer")
	}
	if log.WriteErrors() == 0 {
		t.Error("lines past the bounded queue were not counted as dropped")
	}
	close(w.release)
	log.Flush()
}
