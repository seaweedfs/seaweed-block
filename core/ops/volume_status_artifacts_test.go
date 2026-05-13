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

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestWriteVolumeStatusArtifacts_WritesJSONAndSummary(t *testing.T) {
	dir := t.TempDir()
	collector := artifactTestCollector(nil)

	report, code, err := WriteVolumeStatusArtifacts(context.Background(), dir, collector)
	if err != nil {
		t.Fatalf("write artifacts: %v", err)
	}
	if code != VolumeStatusExitOK {
		t.Fatalf("exit=%d want %d issues=%v", code, VolumeStatusExitOK, VolumeStatusReportIssues(report))
	}

	rawReport, err := os.ReadFile(filepath.Join(dir, VolumeStatusReportArtifact))
	if err != nil {
		t.Fatalf("read report artifact: %v", err)
	}
	var decoded VolumeStatusReport
	if err := json.Unmarshal(rawReport, &decoded); err != nil {
		t.Fatalf("decode report artifact: %v", err)
	}
	if decoded.Volume.VolumeID != "v1" || decoded.Authority.AuthorityRole != hostvolume.AuthorityRolePrimary {
		t.Fatalf("decoded report mismatch: %+v", decoded)
	}

	rawSummary, err := os.ReadFile(filepath.Join(dir, VolumeStatusSummaryArtifact))
	if err != nil {
		t.Fatalf("read summary artifact: %v", err)
	}
	summary := string(rawSummary)
	for _, want := range []string{
		"status: ok",
		"volume: id=v1 replica=r1",
		"issues: none",
	} {
		if !strings.Contains(summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, summary)
		}
	}

	rawBundle, err := os.ReadFile(filepath.Join(dir, OpsStatusBundleArtifact))
	if err != nil {
		t.Fatalf("read bundle artifact: %v", err)
	}
	var bundle OpsStatusBundle
	if err := json.Unmarshal(rawBundle, &bundle); err != nil {
		t.Fatalf("decode bundle artifact: %v", err)
	}
	if bundle.SchemaVersion != "1.0" || bundle.Command != "sw-block ops status" || bundle.VolumeID != "v1" {
		t.Fatalf("bundle identity mismatch: %+v", bundle)
	}
	if bundle.ExitCode != VolumeStatusExitOK || bundle.Status != "ok" {
		t.Fatalf("bundle classification mismatch: %+v", bundle)
	}
	if !containsString(bundle.Unchecked, "processes") {
		t.Fatalf("bundle unchecked classes missing: %+v", bundle.Unchecked)
	}
	if !bundleHasArtifact(bundle, VolumeStatusReportArtifact) ||
		!bundleHasArtifact(bundle, VolumeStatusSummaryArtifact) ||
		!bundleHasArtifact(bundle, OpsStatusBundleArtifact) {
		t.Fatalf("bundle artifact list incomplete: %+v", bundle.Artifacts)
	}
	if len(bundle.NonClaims) == 0 {
		t.Fatalf("bundle should carry explicit non-claims: %+v", bundle)
	}
}

func TestWriteVolumeStatusArtifacts_ChmodFailureDoesNotDropEvidence(t *testing.T) {
	oldChmod := chmodTempFile
	chmodTempFile = func(*os.File, os.FileMode) error {
		return errors.New("operation not permitted")
	}
	t.Cleanup(func() { chmodTempFile = oldChmod })

	dir := t.TempDir()
	report, code, err := WriteVolumeStatusArtifacts(context.Background(), dir, artifactTestCollector(nil))
	if err != nil {
		t.Fatalf("write artifacts despite chmod failure: %v", err)
	}
	if code != VolumeStatusExitOK || report.Volume.VolumeID != "v1" {
		t.Fatalf("code=%d report=%+v", code, report.Volume)
	}
	for _, name := range []string{VolumeStatusReportArtifact, VolumeStatusSummaryArtifact, OpsStatusBundleArtifact} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("missing artifact %s after chmod failure: %v", name, err)
		}
	}
}

func TestWriteVolumeStatusArtifacts_PreservesPartialReportOnCollectionError(t *testing.T) {
	dir := t.TempDir()
	collector := artifactTestCollector(errors.New("master temporarily unavailable"))

	report, code, err := WriteVolumeStatusArtifacts(context.Background(), dir, collector)
	if err == nil {
		t.Fatal("expected collection error")
	}
	if !strings.Contains(err.Error(), "collect master status: master temporarily unavailable") {
		t.Fatalf("unexpected error: %v", err)
	}
	if code != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d want %d issues=%v", code, VolumeStatusExitUnhealthy, VolumeStatusReportIssues(report))
	}
	rawReport, readReportErr := os.ReadFile(filepath.Join(dir, VolumeStatusReportArtifact))
	if readReportErr != nil {
		t.Fatalf("partial report artifact missing: %v", readReportErr)
	}
	var decoded VolumeStatusReport
	if err := json.Unmarshal(rawReport, &decoded); err != nil {
		t.Fatalf("decode partial report artifact: %v", err)
	}
	if got := ClassifyVolumeStatusReport(decoded); got != VolumeStatusExitUnhealthy {
		t.Fatalf("decoded report exit=%d want %d issues=%v", got, VolumeStatusExitUnhealthy, VolumeStatusReportIssues(decoded))
	}
	if len(decoded.CollectionErrors) != 1 || !strings.Contains(decoded.CollectionErrors[0], "collect master status: master temporarily unavailable") {
		t.Fatalf("decoded collection errors missing source failure: %+v", decoded.CollectionErrors)
	}
	rawSummary, readErr := os.ReadFile(filepath.Join(dir, VolumeStatusSummaryArtifact))
	if readErr != nil {
		t.Fatalf("summary artifact missing: %v", readErr)
	}
	if !strings.Contains(string(rawSummary), "status: unhealthy") {
		t.Fatalf("summary should classify partial/error report unhealthy:\n%s", rawSummary)
	}
	if !strings.Contains(string(rawSummary), "collection_error: collect master status: master temporarily unavailable") {
		t.Fatalf("summary should include collection error:\n%s", rawSummary)
	}
	rawBundle, err := os.ReadFile(filepath.Join(dir, OpsStatusBundleArtifact))
	if err != nil {
		t.Fatalf("partial bundle artifact missing: %v", err)
	}
	var bundle OpsStatusBundle
	if err := json.Unmarshal(rawBundle, &bundle); err != nil {
		t.Fatalf("decode partial bundle artifact: %v", err)
	}
	if bundle.ExitCode != VolumeStatusExitUnhealthy || bundle.Status != "unhealthy" {
		t.Fatalf("partial bundle classification mismatch: %+v", bundle)
	}
	if len(bundle.CollectionErrors) != 1 || !strings.Contains(bundle.CollectionErrors[0], "collect master status: master temporarily unavailable") {
		t.Fatalf("partial bundle collection errors missing source failure: %+v", bundle.CollectionErrors)
	}
}

func TestWriteVolumeStatusArtifacts_SplitsJoinedCollectionErrors(t *testing.T) {
	dir := t.TempDir()
	collector := artifactTestCollector(errors.New("master temporarily unavailable"))
	collector.Durable = func(context.Context) ([]durable.VolumeStatus, error) {
		return nil, errors.New("durable temporarily unavailable")
	}

	report, code, err := WriteVolumeStatusArtifacts(context.Background(), dir, collector)
	if err == nil {
		t.Fatal("expected collection error")
	}
	if code != VolumeStatusExitUnhealthy {
		t.Fatalf("exit=%d want %d issues=%v", code, VolumeStatusExitUnhealthy, VolumeStatusReportIssues(report))
	}
	if got, want := len(report.CollectionErrors), 2; got != want {
		t.Fatalf("collection_errors=%v want %d entries", report.CollectionErrors, want)
	}
	for _, errText := range report.CollectionErrors {
		if strings.Contains(errText, "\n") {
			t.Fatalf("collection error should not contain joined newline: %q", errText)
		}
	}
}

func TestWriteVolumeStatusArtifacts_RequiresArtifactDir(t *testing.T) {
	_, code, err := WriteVolumeStatusArtifacts(context.Background(), "", VolumeStatusReportCollector{})
	if err == nil {
		t.Fatal("expected missing dir error")
	}
	if code != VolumeStatusExitInvalid {
		t.Fatalf("exit=%d want %d", code, VolumeStatusExitInvalid)
	}
}

func artifactTestCollector(masterErr error) VolumeStatusReportCollector {
	return VolumeStatusReportCollector{
		Now:             func() time.Time { return time.Date(2026, 5, 11, 23, 0, 0, 0, time.UTC) },
		Source:          ReportSource{Component: "artifact-test", Host: "m02", Scenario: "ops-artifacts"},
		ProductRevision: "product-rev",
		RunnerRevision:  "runner-rev",
		MasterStatus: func(context.Context) (*control.StatusResponse, error) {
			if masterErr != nil {
				return nil, masterErr
			}
			return &control.StatusResponse{
				VolumeId:        "v1",
				ReplicaId:       "r1",
				Epoch:           7,
				EndpointVersion: 2,
				Assigned:        true,
				Frontends: []*control.FrontendTarget{
					{Protocol: "iscsi", Addr: "127.0.0.1:3260", Iqn: "iqn.2026-05.io.seaweedfs:v1", Lun: 0},
				},
			}, nil
		},
		LocalStatus: func(context.Context) (*hostvolume.StatusProjection, error) {
			return &hostvolume.StatusProjection{
				Projection: frontend.Projection{
					VolumeID:        "v1",
					ReplicaID:       "r1",
					Epoch:           7,
					EndpointVersion: 2,
					Healthy:         true,
				},
				FrontendPrimaryReady: true,
				AuthorityRole:        hostvolume.AuthorityRolePrimary,
				ReplicationRole:      hostvolume.ReplicationRoleNone,
			}, nil
		},
		Durable: func(context.Context) ([]durable.VolumeStatus, error) {
			return []durable.VolumeStatus{{
				VolumeID:        "v1",
				Impl:            "smartwal",
				Path:            "/var/lib/sw-block/v1.blk",
				ReplicaID:       "r1",
				Epoch:           7,
				EndpointVersion: 2,
				Latched:         true,
				Operational:     true,
			}}, nil
		},
		Residue: func(context.Context) (ResidueReport, error) {
			return ResidueReport{
				HostInitiator: HostInitiatorResidue{
					ISCSISessions:  []string{},
					NVMESubsystems: []string{},
				},
				Processes:    []string{},
				Kubernetes:   []string{},
				StoragePaths: []string{},
				Unchecked:    []string{"processes"},
			}, nil
		},
	}
}

func bundleHasArtifact(bundle OpsStatusBundle, name string) bool {
	for _, artifact := range bundle.Artifacts {
		if artifact.Name == name {
			return true
		}
	}
	return false
}
