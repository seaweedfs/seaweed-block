package replication

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"syscall"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
)

// INV-FE-ERROR-RETRY-CLASS-001: every error value this package declares has
// one row here, with the retry class a frontend must report for it. A new
// value without a row fails, so no replication refusal falls to the
// permanent default unnoticed.
var replicationErrorClasses = map[string]struct {
	err   error
	class frontend.BackendErrorClass
}{
	"ErrDurabilityQuorumLost":    {ErrDurabilityQuorumLost, frontend.BackendErrorNotReady},
	"ErrDurabilityBarrierFailed": {ErrDurabilityBarrierFailed, frontend.BackendErrorNotReady},
	"ErrPeerNotInSync":           {ErrPeerNotInSync, frontend.BackendErrorNotReady},
	"ErrBarrierBelowTargetLSN":   {ErrBarrierBelowTargetLSN, frontend.BackendErrorNotReady},
	"ErrPeerQueueClosed":         {ErrPeerQueueClosed, frontend.BackendErrorNotReady},
	"ErrMembershipUpdating":      {ErrMembershipUpdating, frontend.BackendErrorNotReady},
	"ErrPeerQueueSaturated":      {ErrPeerQueueSaturated, frontend.BackendErrorBusy},
	"ErrVolumeClosed":            {ErrVolumeClosed, frontend.BackendErrorClosed},
}

func TestEveryReplicationErrorValueHasOneRetryClass(t *testing.T) {
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var declared []string
	fset := token.NewFileSet()
	for _, f := range files {
		name := f.Name()
		if f.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				for _, id := range spec.(*ast.ValueSpec).Names {
					if strings.HasPrefix(id.Name, "Err") {
						declared = append(declared, id.Name)
					}
				}
			}
		}
	}
	sort.Strings(declared)
	if len(declared) != len(replicationErrorClasses) {
		t.Fatalf("declared error values %v; the class table has %d rows", declared, len(replicationErrorClasses))
	}
	for _, name := range declared {
		row, ok := replicationErrorClasses[name]
		if !ok {
			t.Fatalf("%s has no retry-class row", name)
		}
		// As the frontends receive it: wrapped with context, and joined with
		// a peer's cause the way durabilityAckError builds it.
		for _, got := range []error{
			row.err,
			fmt.Errorf("%w: volume v1", row.err),
			durabilityAckError(row.err, "write LSN 7", 1, 2, errors.New("peer r2 timed out")),
		} {
			if class := frontend.ClassifyBackendError(context.Background(), got); class != row.class {
				t.Fatalf("%s as %q: class %d, want %d", name, got, class, row.class)
			}
		}
		// A media fault beside it still decides: it stays permanent.
		if class := frontend.ClassifyBackendError(context.Background(), errors.Join(row.err, syscall.EIO)); class != frontend.BackendErrorPermanent {
			t.Fatalf("%s joined with EIO: class %d, want permanent", name, class)
		}
	}
}

// The FH-Q path: a sync-quorum flush whose peers all fail returns the quorum
// refusal, and the guest must be told to retry (not-ready), not that the
// medium failed. A failed local fsync stays a permanent error.
func TestASyncQuorumShortfallIsNotReadyAndALocalFaultIsPermanent(t *testing.T) {
	run := func(mode DurabilityMode, localErr error, peerErrs ...error) error {
		local := make(chan localSyncResult, 1)
		local <- localSyncResult{err: localErr}
		results := make(chan peerWorkResult, len(peerErrs))
		for i, e := range peerErrs {
			results <- peerWorkResult{peerID: fmt.Sprintf("r%d", i+2), err: e}
		}
		return waitForSyncAcks(context.Background(), mode, 9, len(peerErrs), local, results)
	}
	notInSync := fmt.Errorf("%w: peer r2 is degraded", ErrPeerNotInSync)
	for _, tc := range []struct {
		name  string
		err   error
		is    error
		class frontend.BackendErrorClass
	}{
		{"sync-quorum, both peers refused", run(DurabilitySyncQuorum, nil, notInSync, errors.New("dial r3: refused")), ErrDurabilityQuorumLost, frontend.BackendErrorNotReady},
		{"sync-all, one peer refused", run(DurabilitySyncAll, nil, nil, notInSync), ErrDurabilityBarrierFailed, frontend.BackendErrorNotReady},
		{"local fsync failed", run(DurabilitySyncQuorum, syscall.EIO, nil, nil), syscall.EIO, frontend.BackendErrorPermanent},
	} {
		if !errors.Is(tc.err, tc.is) {
			t.Fatalf("%s: got %v, want %v", tc.name, tc.err, tc.is)
		}
		if class := frontend.ClassifyBackendError(context.Background(), tc.err); class != tc.class {
			t.Fatalf("%s: class %d, want %d (err %v)", tc.name, class, tc.class, tc.err)
		}
	}
}
