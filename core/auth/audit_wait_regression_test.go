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
