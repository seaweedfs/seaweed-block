package ops

import (
	"strings"
	"testing"
)

func TestCleanupEvidenceProjectionOwnsSummaryAndReportShape(t *testing.T) {
	cleanup := CleanupEvidenceFromSummary(map[string]string{
		CleanupSummaryStatusKey:            "failed",
		CleanupSummaryKubernetesResidueKey: "1",
		CleanupSummaryISCSIResidueKey:      "2",
		CleanupSummaryMultipathResidueKey:  "3",
		CleanupSummaryProcessResidueKey:    "4",
		CleanupSummaryHostPathResidueKey:   "5",
		CleanupSummaryFailureKey:           "6",
		CleanupSummaryFailedPhaseKey:       "collect_and_cleanup",
		CleanupSummaryReasonCodesKey:       "k8s_residue,multipath_residue",
	}, "cleanup-summary.txt")

	if cleanup == nil {
		t.Fatal("cleanup evidence is nil")
	}
	if cleanup.Status != "failed" ||
		cleanup.KubernetesResidueCount != 1 ||
		cleanup.ISCSIResidueCount != 2 ||
		cleanup.MultipathResidueCount != 3 ||
		cleanup.ProcessResidueCount != 4 ||
		cleanup.HostPathResidueCount != 5 ||
		cleanup.FailureCount != 6 ||
		cleanup.FailedPhase != "collect_and_cleanup" ||
		cleanup.EvidenceRef != "cleanup-summary.txt" {
		t.Fatalf("cleanup evidence=%+v", cleanup)
	}
	if len(cleanup.ReasonCodes) != 2 || cleanup.ReasonCodes[1] != "multipath_residue" {
		t.Fatalf("reason codes=%+v", cleanup.ReasonCodes)
	}

	lines := strings.Join(cleanup.ReportSummaryLines(), "\n")
	for _, want := range []string{
		"cleanup_status=failed",
		"k8s_residue_count=1",
		"iscsi_residue_count=2",
		"multipath_residue_count=3",
		"process_residue_count=4",
		"hostpath_residue_count=5",
		"failure_count=6",
		"failed_phase=collect_and_cleanup",
		"cleanup_evidence=cleanup-summary.txt",
	} {
		if !strings.Contains(lines, want) {
			t.Fatalf("summary lines missing %q:\n%s", want, lines)
		}
	}

	row := cleanup.ReportRow()
	if row.StatusClass != "bad" || row.Status != "failed" || row.EvidenceRef != "cleanup-summary.txt" {
		t.Fatalf("row=%+v", row)
	}
}

func TestCleanupEvidenceProjectionMarksCleanStatusOK(t *testing.T) {
	cleanup := CleanupEvidence{
		Status: ObservationStatusOK,
	}
	row := cleanup.ReportRow()
	if row.StatusClass != "ok" {
		t.Fatalf("row=%+v", row)
	}
}

func TestCleanupEvidenceRejectsNegativeCounters(t *testing.T) {
	cleanup := CleanupEvidenceFromSummary(map[string]string{
		CleanupSummaryStatusKey:            "failed",
		CleanupSummaryKubernetesResidueKey: "-1",
		CleanupSummaryFailureKey:           "-2",
	}, "cleanup-summary.txt")
	if cleanup.KubernetesResidueCount != 0 || cleanup.FailureCount != 0 {
		t.Fatalf("negative counters should parse as zero, got %+v", cleanup)
	}
}
