package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

const (
	VolumeStatusReportArtifact  = "volume-status-report.json"
	VolumeStatusSummaryArtifact = "volume-status-summary.txt"
	OpsStatusBundleArtifact     = "ops-status-bundle.json"
)

type OpsStatusBundle struct {
	SchemaVersion    string                       `json:"schema_version"`
	Command          string                       `json:"command"`
	CapturedAt       string                       `json:"captured_at"`
	VolumeID         string                       `json:"volume_id"`
	ProductRevision  string                       `json:"product_revision"`
	RunnerRevision   string                       `json:"runner_revision,omitempty"`
	ExitCode         int                          `json:"exit_code"`
	Status           string                       `json:"status"`
	Artifacts        []OpsStatusBundleArtifactRef `json:"artifacts"`
	Unchecked        []string                     `json:"unchecked"`
	CollectionErrors []string                     `json:"collection_errors"`
	NonClaims        []string                     `json:"non_claims"`
}

type OpsStatusBundleArtifactRef struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// WriteVolumeStatusArtifacts collects a report and writes the stable operator
// artifact pair into dir. Collection errors still produce partial artifacts
// when possible; collection errors are recorded in the report and therefore
// contribute to the returned classification.
func WriteVolumeStatusArtifacts(ctx context.Context, dir string, collector VolumeStatusReportCollector) (VolumeStatusReport, int, error) {
	if dir == "" {
		return VolumeStatusReport{}, VolumeStatusExitInvalid, fmt.Errorf("artifact dir is required")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return VolumeStatusReport{}, VolumeStatusExitInvalid, fmt.Errorf("create artifact dir: %w", err)
	}

	report, collectErr := collector.Collect(ctx)
	if collectErr != nil {
		report.CollectionErrors = append(report.CollectionErrors, splitErrorMessages(collectErr)...)
	}
	classification := ClassifyVolumeStatusReport(report)

	raw, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return report, VolumeStatusExitInvalid, fmt.Errorf("marshal volume status report: %w", err)
	}
	if err := writeFileViaTemp(filepath.Join(dir, VolumeStatusReportArtifact), append(raw, '\n'), 0o644); err != nil {
		return report, VolumeStatusExitInvalid, fmt.Errorf("write %s: %w", VolumeStatusReportArtifact, err)
	}
	if err := writeFileViaTemp(filepath.Join(dir, VolumeStatusSummaryArtifact), []byte(RenderVolumeStatusSummary(report)), 0o644); err != nil {
		return report, VolumeStatusExitInvalid, fmt.Errorf("write %s: %w", VolumeStatusSummaryArtifact, err)
	}
	bundleRaw, err := json.MarshalIndent(BuildOpsStatusBundle(report, classification), "", "  ")
	if err != nil {
		return report, VolumeStatusExitInvalid, fmt.Errorf("marshal ops status bundle: %w", err)
	}
	if err := writeFileViaTemp(filepath.Join(dir, OpsStatusBundleArtifact), append(bundleRaw, '\n'), 0o644); err != nil {
		return report, VolumeStatusExitInvalid, fmt.Errorf("write %s: %w", OpsStatusBundleArtifact, err)
	}
	if collectErr != nil {
		return report, classification, collectErr
	}
	return report, classification, nil
}

func BuildOpsStatusBundle(report VolumeStatusReport, exitCode int) OpsStatusBundle {
	return OpsStatusBundle{
		SchemaVersion:    "1.0",
		Command:          "sw-block ops status",
		CapturedAt:       report.CapturedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		VolumeID:         report.Volume.VolumeID,
		ProductRevision:  report.ProductRevision,
		RunnerRevision:   report.RunnerRevision,
		ExitCode:         exitCode,
		Status:           volumeStatusExitLabel(exitCode),
		Artifacts:        opsStatusBundleArtifacts(),
		Unchecked:        copyStringSlice(report.Residue.Unchecked),
		CollectionErrors: copyStringSlice(report.CollectionErrors),
		NonClaims: []string{
			"Read-only diagnostic bundle; it does not mutate product state.",
			"Not a block/data snapshot, backup, rollback, or restore point.",
			"Not a repair, force-detach, cleanup, or failover authorization.",
			"Process, Kubernetes, and storage-path residue may be unchecked unless explicitly listed.",
		},
	}
}

func volumeStatusExitLabel(code int) string {
	switch code {
	case VolumeStatusExitOK:
		return "ok"
	case VolumeStatusExitUnhealthy:
		return "unhealthy"
	default:
		return "invalid"
	}
}

func opsStatusBundleArtifacts() []OpsStatusBundleArtifactRef {
	return []OpsStatusBundleArtifactRef{
		{Name: VolumeStatusReportArtifact, Description: "machine-readable volume status evidence"},
		{Name: VolumeStatusSummaryArtifact, Description: "human-readable status summary"},
		{Name: OpsStatusBundleArtifact, Description: "self-describing support bundle manifest"},
	}
}

func splitErrorMessages(err error) []string {
	if err == nil {
		return nil
	}
	type unwrapper interface {
		Unwrap() []error
	}
	if joined, ok := err.(unwrapper); ok {
		var out []string
		for _, child := range joined.Unwrap() {
			out = append(out, splitErrorMessages(child)...)
		}
		return out
	}
	return []string{err.Error()}
}

func writeFileViaTemp(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}
