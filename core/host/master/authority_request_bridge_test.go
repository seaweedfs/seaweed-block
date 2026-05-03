package master

import (
	"context"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
)

func TestG9F2_UnverifiedPlacementProducesNoAssignmentAsk(t *testing.T) {
	asks, err := assignmentRequestsFromVerifiedPlacement(lifecycle.VerifiedPlacement{
		VolumeID: "vol-a",
		Verified: false,
		Reason:   lifecycle.VerifyReasonMissingObservation,
	})
	if err != nil {
		t.Fatalf("unverified placement err=%v, want nil", err)
	}
	if len(asks) != 0 {
		t.Fatalf("unverified placement produced asks=%+v", asks)
	}
}

func TestG9F2_VerifiedExistingReplicaProducesBindAsk(t *testing.T) {
	asks, err := assignmentRequestsFromVerifiedPlacement(verifiedExistingReplicaPlacement())
	if err != nil {
		t.Fatalf("verified placement err=%v", err)
	}
	if len(asks) != 1 {
		t.Fatalf("asks=%+v want one", asks)
	}
	got := asks[0]
	if got.VolumeID != "vol-a" || got.ReplicaID != "r2" || got.DataAddr != "127.0.0.1:9202" || got.CtrlAddr != "127.0.0.1:9102" {
		t.Fatalf("ask=%+v does not mirror verified placement", got)
	}
	if got.Intent != authority.IntentBind {
		t.Fatalf("intent=%v want IntentBind", got.Intent)
	}
}

func TestG15b_ProductLoop_RF2PlacementEmitsSingleDeterministicBind(t *testing.T) {
	asks, err := assignmentRequestsFromVerifiedPlacement(lifecycle.VerifiedPlacement{
		VolumeID: "v1",
		Verified: true,
		Slots: []lifecycle.VerifiedPlacementSlot{
			{
				ServerID:  "s1",
				ReplicaID: "r1",
				Source:    lifecycle.PlacementSourceExistingReplica,
				DataAddr:  "127.0.0.1:19101",
				CtrlAddr:  "127.0.0.1:19102",
			},
			{
				ServerID:  "s2",
				ReplicaID: "r2",
				Source:    lifecycle.PlacementSourceExistingReplica,
				DataAddr:  "127.0.0.1:19201",
				CtrlAddr:  "127.0.0.1:19202",
			},
		},
	})
	if err != nil {
		t.Fatalf("assignmentRequestsFromVerifiedPlacement: %v", err)
	}
	if len(asks) != 1 {
		t.Fatalf("asks=%+v want exactly one frontend-primary bind", asks)
	}
	if got := asks[0].ReplicaID; got != "r1" {
		t.Fatalf("replica=%q want first verified slot r1", got)
	}
}

func TestG9F2_AssignmentRequestShapeHasNoEpochOrEndpointVersion(t *testing.T) {
	typ := reflect.TypeOf(authority.AssignmentAsk{})
	for _, forbidden := range []string{"Epoch", "EndpointVersion", "Assignment", "Ready", "Healthy", "Primary"} {
		if _, ok := typ.FieldByName(forbidden); ok {
			t.Fatalf("AssignmentAsk bridge output must not carry %s", forbidden)
		}
	}
}

func TestG9F2_PlacementBridgeOutputPublishesThroughAuthorityPublisher(t *testing.T) {
	asks, err := assignmentRequestsFromVerifiedPlacement(verifiedExistingReplicaPlacement())
	if err != nil {
		t.Fatalf("verified placement err=%v", err)
	}
	dir := authority.NewStaticDirective(nil)
	pub := authority.NewPublisher(dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pub.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("publisher did not stop")
		}
	}()

	dir.Append(asks[0])
	line := waitAuthorityLine(t, pub, "vol-a")
	if line.ReplicaID != "r2" || line.Epoch != 1 || line.EndpointVersion != 1 {
		t.Fatalf("line=%+v want publisher-minted bind for r2 epoch=1 ev=1", line)
	}
}

func TestG9F2_LifecyclePackageDoesNotImportAuthority(t *testing.T) {
	lifecycleDir := filepath.Clean(filepath.Join("..", "..", "lifecycle"))
	files, err := filepath.Glob(filepath.Join(lifecycleDir, "*.go"))
	if err != nil {
		t.Fatalf("glob lifecycle: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no lifecycle files found")
	}
	fset := token.NewFileSet()
	for _, path := range files {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, imp := range file.Imports {
			if strings.Contains(strings.Trim(imp.Path.Value, `"`), "/core/authority") {
				t.Fatalf("lifecycle package must not import authority; found in %s", path)
			}
		}
	}
}

func verifiedExistingReplicaPlacement() lifecycle.VerifiedPlacement {
	return lifecycle.VerifiedPlacement{
		VolumeID: "vol-a",
		Verified: true,
		Reason:   lifecycle.VerifyReasonOK,
		Slots: []lifecycle.VerifiedPlacementSlot{{
			ServerID:   "node-b",
			ReplicaID:  "r2",
			Source:     lifecycle.PlacementSourceExistingReplica,
			DataAddr:   "127.0.0.1:9202",
			CtrlAddr:   "127.0.0.1:9102",
			VerifiedBy: "observation_store",
		}},
	}
}

func waitAuthorityLine(t *testing.T, pub *authority.Publisher, volumeID string) authority.AuthorityBasis {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if line, ok := pub.VolumeAuthorityLine(volumeID); ok {
			return line
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for authority line %s", volumeID)
	return authority.AuthorityBasis{}
}
