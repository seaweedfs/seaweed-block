package ops

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWriteVolumeInventoryArtifacts_EmptyClusterBundle(t *testing.T) {
	dir := t.TempDir()
	inventory, code, err := WriteVolumeInventoryArtifacts(context.Background(), dir, StaticVolumeInventoryCollector(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test", Host: "m02", Scenario: "empty"},
		ProductRevision: "product-rev",
		RunnerRevision:  "runner-rev",
	}))
	if err != nil {
		t.Fatalf("write artifacts: %v", err)
	}
	if code != VolumeStatusExitOK || inventory.Status != "ok" {
		t.Fatalf("code=%d status=%s issues=%v", code, inventory.Status, VolumeInventoryIssues(inventory))
	}
	for _, name := range []string{VolumeInventoryArtifact, VolumeInventorySummaryArtifact, OpsInventoryBundleArtifact} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}

	rawInventory, err := os.ReadFile(filepath.Join(dir, VolumeInventoryArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var decoded VolumeInventory
	if err := json.Unmarshal(rawInventory, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Volumes) != 0 || len(decoded.NonClaims) == 0 {
		t.Fatalf("decoded inventory=%+v", decoded)
	}

	summary, err := os.ReadFile(filepath.Join(dir, VolumeInventorySummaryArtifact))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(summary), "volumes: total=0") {
		t.Fatalf("summary missing empty inventory evidence:\n%s", summary)
	}

	rawBundle, err := os.ReadFile(filepath.Join(dir, OpsInventoryBundleArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var bundle OpsInventoryBundle
	if err := json.Unmarshal(rawBundle, &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.Command != "sw-block ops inventory" || bundle.Status != "ok" || bundle.VolumeCount != 0 {
		t.Fatalf("bundle=%+v", bundle)
	}
	if len(bundle.Artifacts) != 3 || len(bundle.NonClaims) == 0 {
		t.Fatalf("bundle artifacts/nonclaims=%+v", bundle)
	}
}

func TestWriteVolumeInventoryArtifacts_PreservesCollectionErrors(t *testing.T) {
	dir := t.TempDir()
	_, code, err := WriteVolumeInventoryArtifacts(context.Background(), dir, VolumeInventoryCollectorFunc(func(context.Context) (VolumeInventory, error) {
		return BuildVolumeInventory(VolumeInventoryInput{
			CapturedAt:      time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC),
			Source:          ReportSource{Component: "component-test"},
			ProductRevision: "product-rev",
		}), errors.New("kubernetes_unreachable: dial tcp 127.0.0.1:6443: connect refused")
	}))
	if err == nil {
		t.Fatal("expected collection error")
	}
	if code != VolumeStatusExitInvalid {
		t.Fatalf("code=%d want %d", code, VolumeStatusExitInvalid)
	}
	rawBundle, readErr := os.ReadFile(filepath.Join(dir, OpsInventoryBundleArtifact))
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !strings.Contains(string(rawBundle), "kubernetes_unreachable") {
		t.Fatalf("bundle missing collection error:\n%s", rawBundle)
	}
}

func TestWriteVolumeInventoryArtifacts_UnhealthyRowsStillExitOK(t *testing.T) {
	dir := t.TempDir()
	inventory, code, err := WriteVolumeInventoryArtifacts(context.Background(), dir, StaticVolumeInventoryCollector(VolumeInventoryInput{
		CapturedAt:      time.Date(2026, 5, 12, 12, 0, 0, 0, time.UTC),
		Source:          ReportSource{Component: "component-test"},
		ProductRevision: "product-rev",
		Volumes: []VolumeInventoryVolumeInput{{
			VolumeID:          "v1",
			Namespace:         "default",
			PVCName:           "app",
			PVName:            "pv-v1",
			ReplicationFactor: 2,
			Replicas: []VolumeInventoryReplicaInput{{
				ReplicaID:            "r1",
				ServerID:             "m02",
				NodeName:             "m02",
				GeneratedDeployment:  "sw-blockvolume-v1-r1",
				Protocol:             "iscsi",
				FrontendAddress:      "127.0.0.1:3260",
				StatusAddress:        "127.0.0.1:23260",
				Observed:             true,
				AuthorityRole:        "primary",
				Healthy:              true,
				FrontendPrimaryReady: true,
				ReplicationRole:      "none",
			}},
		}},
	}))
	if err != nil {
		t.Fatalf("write artifacts: %v", err)
	}
	if code != VolumeStatusExitOK || inventory.Status != "unhealthy" {
		t.Fatalf("code=%d status=%s issues=%v", code, inventory.Status, VolumeInventoryIssues(inventory))
	}
	rawBundle, err := os.ReadFile(filepath.Join(dir, OpsInventoryBundleArtifact))
	if err != nil {
		t.Fatal(err)
	}
	var bundle OpsInventoryBundle
	if err := json.Unmarshal(rawBundle, &bundle); err != nil {
		t.Fatal(err)
	}
	if bundle.ExitCode != VolumeStatusExitOK || bundle.Status != "ok" || bundle.InventoryStatus != "unhealthy" {
		t.Fatalf("bundle=%+v", bundle)
	}
}

func TestWriteVolumeInventoryArtifacts_RequiresArtifactDir(t *testing.T) {
	_, code, err := WriteVolumeInventoryArtifacts(context.Background(), "", StaticVolumeInventoryCollector(VolumeInventoryInput{}))
	if err == nil {
		t.Fatal("expected error")
	}
	if code != VolumeStatusExitInvalid {
		t.Fatalf("code=%d", code)
	}
}
