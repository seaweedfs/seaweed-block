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
)

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
	if collectErr != nil {
		return report, classification, collectErr
	}
	return report, classification, nil
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
