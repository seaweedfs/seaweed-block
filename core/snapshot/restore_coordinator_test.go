package snapshot

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/storage"
)

func TestPhase175RestoreCoordinatorAppliesAllBeforeActivateAndCompletesGate(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	resolver := &fakeRestoreResolver{plans: []RestorePlan{{Targets: []RestoreReplicaTarget{
		testRestoreReplicaTarget(rec, "r2", "https://10.0.0.2:24443"),
		testRestoreReplicaTarget(rec, "r1", "https://10.0.0.1:24443"),
	}}, {Targets: []RestoreReplicaTarget{
		testRestoreReplicaTarget(rec, "r1", "https://10.0.0.1:24443"),
		testRestoreReplicaTarget(rec, "r2", "https://10.0.0.2:24443"),
	}}}}
	runtime := &fakeRestoreRuntime{}
	coordinator, err := NewCoordinator(manager, fixedSnapshotResolver{}, fixedCaptureRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ConfigureRestore(resolver, runtime); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Restore(context.Background(), rec.SnapshotID, "target-vol")
	if err != nil {
		t.Fatal(err)
	}
	if result.ReplicaCount != 2 || resolver.completed != "target-vol/"+rec.SnapshotID {
		t.Fatalf("result=%+v completed=%q", result, resolver.completed)
	}
	wantCalls := []string{"apply:r1", "apply:r2", "activate:r1", "activate:r2"}
	if fmt.Sprint(runtime.calls) != fmt.Sprint(wantCalls) {
		t.Fatalf("calls=%v want %v", runtime.calls, wantCalls)
	}
}

func TestPhase175RestoreCoordinatorNeverActivatesPartialOrChangedApply(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	targets := []RestoreReplicaTarget{
		testRestoreReplicaTarget(rec, "r1", "https://10.0.0.1:24443"),
		testRestoreReplicaTarget(rec, "r2", "https://10.0.0.2:24443"),
	}
	for _, tc := range []struct {
		name     string
		resolver *fakeRestoreResolver
		runtime  *fakeRestoreRuntime
	}{
		{name: "apply failure", resolver: &fakeRestoreResolver{plans: []RestorePlan{{Targets: targets}}}, runtime: &fakeRestoreRuntime{failApplyReplica: "r2"}},
		{name: "placement drift", resolver: &fakeRestoreResolver{plans: []RestorePlan{{Targets: targets}, {Targets: []RestoreReplicaTarget{testRestoreReplicaTarget(rec, "r1", "https://10.0.0.9:24443")}}}}, runtime: &fakeRestoreRuntime{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			coordinator, err := NewCoordinator(manager, fixedSnapshotResolver{}, fixedCaptureRuntime{})
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.ConfigureRestore(tc.resolver, tc.runtime); err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.Restore(context.Background(), rec.SnapshotID, "target-vol"); err == nil {
				t.Fatal("unsafe restore succeeded")
			}
			for _, call := range tc.runtime.calls {
				if len(call) >= len("activate:") && call[:len("activate:")] == "activate:" {
					t.Fatalf("partial restore activated: %v", tc.runtime.calls)
				}
			}
			if tc.resolver.completed != "" {
				t.Fatalf("partial restore completed gate: %q", tc.resolver.completed)
			}
		})
	}
}

func TestPhase175RestoreCoordinatorRejectsChangedDurableStoreEvidence(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	targets := []RestoreReplicaTarget{testRestoreReplicaTarget(rec, "r1", "https://10.0.0.1:24443")}
	for _, tc := range []struct {
		name    string
		runtime *fakeRestoreRuntime
	}{
		{name: "apply store generation", runtime: &fakeRestoreRuntime{applyStorageID: "replacement-store"}},
		{name: "activation store generation", runtime: &fakeRestoreRuntime{activateStorageID: "replacement-store"}},
		{name: "activation frontier", runtime: &fakeRestoreRuntime{activateFrontier: rec.Frontier + 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := &fakeRestoreResolver{plans: []RestorePlan{{Targets: targets}, {Targets: targets}}}
			coordinator, err := NewCoordinator(manager, fixedSnapshotResolver{}, fixedCaptureRuntime{})
			if err != nil {
				t.Fatal(err)
			}
			if err := coordinator.ConfigureRestore(resolver, tc.runtime); err != nil {
				t.Fatal(err)
			}
			if _, err := coordinator.Restore(context.Background(), rec.SnapshotID, "target-vol"); !errors.Is(err, ErrRestoreUnsafe) {
				t.Fatalf("restore error=%v want ErrRestoreUnsafe", err)
			}
			if resolver.completed != "" {
				t.Fatalf("unsafe evidence completed authority gate: %q", resolver.completed)
			}
		})
	}
}

func TestPhase175RestoreCoordinatorHoldsSnapshotLeaseAcrossAllReplicas(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	targets := []RestoreReplicaTarget{
		testRestoreReplicaTarget(rec, "r1", "https://10.0.0.1:24443"),
		testRestoreReplicaTarget(rec, "r2", "https://10.0.0.2:24443"),
	}
	var deleteErr error
	runtime := &fakeRestoreRuntime{afterApply: func(replicaID string) {
		if replicaID == "r1" {
			deleteErr = manager.Delete(rec.SnapshotID)
		}
	}}
	coordinator, err := NewCoordinator(manager, fixedSnapshotResolver{}, fixedCaptureRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.ConfigureRestore(&fakeRestoreResolver{plans: []RestorePlan{{Targets: targets}, {Targets: targets}}}, runtime); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Restore(context.Background(), rec.SnapshotID, "target-vol"); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(deleteErr, ErrInUse) {
		t.Fatalf("delete between replicas=%v want ErrInUse", deleteErr)
	}
	if err := manager.Delete(rec.SnapshotID); err != nil {
		t.Fatalf("delete after restore: %v", err)
	}
}

func TestPhase175RestoreCoordinatorAcceptsConcurrentCompletionAfterApply(t *testing.T) {
	manager, rec, _ := createStreamFixture(t)
	targets := []RestoreReplicaTarget{testRestoreReplicaTarget(rec, "r1", "https://10.0.0.1:24443")}
	coordinator, err := NewCoordinator(manager, fixedSnapshotResolver{}, fixedCaptureRuntime{})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &fakeRestoreRuntime{}
	if err := coordinator.ConfigureRestore(&fakeRestoreResolver{plans: []RestorePlan{{Targets: targets}, {AlreadyComplete: true}}}, runtime); err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.Restore(context.Background(), rec.SnapshotID, "target-vol")
	if err != nil {
		t.Fatal(err)
	}
	if !result.AlreadyComplete || result.ReplicaCount != 1 {
		t.Fatalf("result=%+v", result)
	}
	if fmt.Sprint(runtime.calls) != "[apply:r1]" {
		t.Fatalf("calls=%v", runtime.calls)
	}
}

type fakeRestoreResolver struct {
	plans     []RestorePlan
	calls     int
	completed string
}

func (r *fakeRestoreResolver) ResolveSnapshotRestoreTargets(context.Context, string, Record) (RestorePlan, error) {
	if r.calls >= len(r.plans) {
		return RestorePlan{}, errors.New("unexpected resolve")
	}
	plan := r.plans[r.calls]
	r.calls++
	return plan, nil
}

func (r *fakeRestoreResolver) CompleteSnapshotRestore(_ context.Context, volumeID, snapshotID string, targets []RestoreReplicaTarget) error {
	if len(targets) == 0 {
		return errors.New("missing completion target evidence")
	}
	r.completed = volumeID + "/" + snapshotID
	return nil
}

type fakeRestoreRuntime struct {
	calls             []string
	failApplyReplica  string
	applyStorageID    string
	activateStorageID string
	activateFrontier  uint64
	afterApply        func(replicaID string)
}

func (r *fakeRestoreRuntime) Apply(ctx context.Context, req RuntimeRestoreRequest, source ArchiveStreamer) (RestoreApplyResult, error) {
	r.calls = append(r.calls, "apply:"+req.TargetReplicaID)
	if req.TargetReplicaID == r.failApplyReplica {
		return RestoreApplyResult{}, errors.New("injected apply failure")
	}
	var archive bytes.Buffer
	streamed, err := source.StreamArchive(ctx, req.Snapshot.SnapshotID, &archive)
	if err != nil || !sameRestoreRecord(streamed, req.Snapshot) {
		return RestoreApplyResult{}, errors.New("invalid archive source")
	}
	target := storage.NewBlockStore(req.Snapshot.NumBlocks, req.Snapshot.BlockSize)
	if _, err := ApplyArchiveStream(ctx, bytes.NewReader(archive.Bytes()), req.Snapshot, func(lba uint32, data []byte) error {
		_, err := target.Write(lba, data)
		return err
	}); err != nil {
		return RestoreApplyResult{}, err
	}
	if r.afterApply != nil {
		r.afterApply(req.TargetReplicaID)
	}
	storageID := req.TargetReplicaID + "-store"
	if r.applyStorageID != "" {
		storageID = r.applyStorageID
	}
	return RestoreApplyResult{
		State:           RestoreStateApplied,
		TargetStorageID: storageID,
		TargetNumBlocks: req.Snapshot.NumBlocks,
		TargetBlockSize: req.Snapshot.BlockSize,
		RestoredBlocks:  req.Snapshot.RecordCount,
		RestoredBytes:   req.Snapshot.DataBytes,
		TargetFrontier:  req.Snapshot.Frontier,
	}, nil
}

func (r *fakeRestoreRuntime) Activate(_ context.Context, req RuntimeRestoreRequest) (RestoreMarker, error) {
	r.calls = append(r.calls, "activate:"+req.TargetReplicaID)
	storageID := req.TargetReplicaID + "-store"
	if r.activateStorageID != "" {
		storageID = r.activateStorageID
	}
	frontier := req.Snapshot.Frontier
	if r.activateFrontier != 0 {
		frontier = r.activateFrontier
	}
	rec := req.Snapshot
	return RestoreMarker{
		State:           RestoreStateActivated,
		SnapshotID:      rec.SnapshotID,
		TargetVolumeID:  req.TargetVolumeID,
		TargetReplicaID: req.TargetReplicaID,
		TargetStorageID: storageID,
		TargetNumBlocks: rec.NumBlocks,
		TargetBlockSize: rec.BlockSize,
		Snapshot:        &rec,
		RestoredBlocks:  rec.RecordCount,
		RestoredBytes:   rec.DataBytes,
		TargetFrontier:  frontier,
	}, nil
}

func testRestoreReplicaTarget(rec Record, replicaID, endpoint string) RestoreReplicaTarget {
	return RestoreReplicaTarget{
		VolumeID:        "target-vol",
		ReplicaID:       replicaID,
		RuntimeEndpoint: endpoint,
		TargetStorageID: replicaID + "-store",
		TargetNumBlocks: rec.NumBlocks,
		TargetBlockSize: rec.BlockSize,
	}
}

type fixedSnapshotResolver struct{}

func (fixedSnapshotResolver) ResolveSnapshotSource(context.Context, string) (SourceAuthority, error) {
	return SourceAuthority{VolumeID: "unused", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, RuntimeEndpoint: "https://127.0.0.1:24443", SizeBytes: 4096}, nil
}

type fixedCaptureRuntime struct{}

func (fixedCaptureRuntime) CaptureSnapshot(context.Context, RuntimeCaptureRequest, storage.SnapshotBlockSink) (storage.SnapshotCut, error) {
	return storage.SnapshotCut{}, errors.New("unused")
}
