package auth_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/auth"
	"github.com/seaweedfs/seaweed-block/core/auth/authtest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *lockedBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *lockedBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

// A strict (mutating) call writes its attempt line before its handler runs;
// with the audit writer stalled it is refused as "audit unavailable" within
// the bound and its handler never runs, while a read still proceeds.
func TestStrictCallsNeedTheirAttemptLine(t *testing.T) {
	old := auth.AuditAttemptBound
	auth.AuditAttemptBound = 150 * time.Millisecond
	t.Cleanup(func() { auth.AuditAttemptBound = old })
	ca := authtest.NewCA(t, "ca")
	admin := ca.Issue(t, "root", time.Now().Add(time.Hour))
	tbl := table(t, admin.Fingerprint+" admin root")
	rules := map[string]auth.Rule{"/delete": {Min: auth.RoleAdmin, Strict: true}, "/observe": {Min: auth.RoleViewer}}
	call := func(a *auth.Authenticator, method string, handler grpc.UnaryHandler) error {
		_, err := a.UnaryInterceptor(rules)(peerCtx(admin, ca), nil, &grpc.UnaryServerInfo{FullMethod: method}, handler)
		return err
	}

	// Working writer: the attempt line is on disk before the handler runs.
	a := auth.NewAuthenticator(tbl, nil)
	var buf lockedBuffer
	log := auth.NewAuditLog(&buf, nil)
	a.SetAudit(log)
	sawAttempt := false
	if err := call(a, "/delete", func(context.Context, any) (any, error) {
		sawAttempt = strings.Contains(buf.String(), `"event":"attempt"`) && strings.Contains(buf.String(), `"method":"/delete"`)
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !sawAttempt {
		t.Fatal("the strict call's handler ran before its attempt line was written")
	}
	log.Flush()

	// Stalled writer: refused, handler not run; a read proceeds.
	b := auth.NewAuthenticator(tbl, nil)
	w := newCodexBlockedAudit()
	stalled := auth.NewAuditLog(w, nil)
	b.SetAudit(stalled)
	ran := false
	start := time.Now()
	err := call(b, "/delete", func(context.Context, any) (any, error) { ran = true; return nil, nil })
	if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "audit unavailable") || ran {
		t.Fatalf("strict call with a stalled audit writer: err=%v ran=%t; want Unavailable 'audit unavailable', not run", err, ran)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the refusal took %v; want it within the bound", d)
	}
	readDone := make(chan error, 1)
	go func() { readDone <- call(b, "/observe", func(context.Context, any) (any, error) { return nil, nil }) }()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read with a stalled writer: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("a read waited on the stalled audit writer")
	}
	close(w.release)
	stalled.Flush()
}
