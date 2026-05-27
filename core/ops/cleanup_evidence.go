package ops

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	CleanupSummaryStatusKey            = "cleanup_status"
	CleanupSummaryKubernetesResidueKey = "k8s_residue_count"
	CleanupSummaryISCSIResidueKey      = "iscsi_residue_count"
	CleanupSummaryMultipathResidueKey  = "multipath_residue_count"
	CleanupSummaryProcessResidueKey    = "process_residue_count"
	CleanupSummaryHostPathResidueKey   = "hostpath_residue_count"
	CleanupSummaryFailureKey           = "failure_count"
	CleanupSummaryFailedPhaseKey       = "failed_phase"
	CleanupSummaryReasonCodesKey       = "reason_codes"
	CleanupSummaryEvidenceKey          = "cleanup_evidence"
)

type CleanupEvidenceReportRow struct {
	Status                 string
	KubernetesResidueCount int
	ISCSIResidueCount      int
	MultipathResidueCount  int
	ProcessResidueCount    int
	HostPathResidueCount   int
	FailureCount           int
	EvidenceRef            string
	StatusClass            string
}

func CleanupEvidenceFromSummary(summary map[string]string, evidencePath string) *CleanupEvidence {
	if len(summary) == 0 {
		return nil
	}
	cleanup := &CleanupEvidence{
		Status:      defaultString(summary[CleanupSummaryStatusKey], ObservationStatusUnavailable),
		EvidenceRef: evidencePath,
	}
	cleanup.KubernetesResidueCount = cleanupIntFromSummary(summary, CleanupSummaryKubernetesResidueKey)
	cleanup.ISCSIResidueCount = cleanupIntFromSummary(summary, CleanupSummaryISCSIResidueKey)
	cleanup.MultipathResidueCount = cleanupIntFromSummary(summary, CleanupSummaryMultipathResidueKey)
	cleanup.ProcessResidueCount = cleanupIntFromSummary(summary, CleanupSummaryProcessResidueKey)
	cleanup.HostPathResidueCount = cleanupIntFromSummary(summary, CleanupSummaryHostPathResidueKey)
	cleanup.FailureCount = cleanupIntFromSummary(summary, CleanupSummaryFailureKey)
	cleanup.FailedPhase = summary[CleanupSummaryFailedPhaseKey]
	if reasons := strings.TrimSpace(summary[CleanupSummaryReasonCodesKey]); reasons != "" {
		for _, reason := range strings.Split(reasons, ",") {
			reason = strings.TrimSpace(reason)
			if reason != "" {
				cleanup.ReasonCodes = append(cleanup.ReasonCodes, reason)
			}
		}
	}
	return cleanup
}

func (cleanup CleanupEvidence) ReportSummaryLines() []string {
	lines := []string{
		fmt.Sprintf("%s=%s", CleanupSummaryStatusKey, emptyAsDash(cleanup.Status)),
		fmt.Sprintf("%s=%d", CleanupSummaryKubernetesResidueKey, cleanup.KubernetesResidueCount),
		fmt.Sprintf("%s=%d", CleanupSummaryISCSIResidueKey, cleanup.ISCSIResidueCount),
		fmt.Sprintf("%s=%d", CleanupSummaryMultipathResidueKey, cleanup.MultipathResidueCount),
		fmt.Sprintf("%s=%d", CleanupSummaryProcessResidueKey, cleanup.ProcessResidueCount),
		fmt.Sprintf("%s=%d", CleanupSummaryHostPathResidueKey, cleanup.HostPathResidueCount),
		fmt.Sprintf("%s=%d", CleanupSummaryFailureKey, cleanup.FailureCount),
	}
	if cleanup.FailedPhase != "" {
		lines = append(lines, fmt.Sprintf("%s=%s", CleanupSummaryFailedPhaseKey, cleanup.FailedPhase))
	}
	if cleanup.EvidenceRef != "" {
		lines = append(lines, fmt.Sprintf("%s=%s", CleanupSummaryEvidenceKey, cleanup.EvidenceRef))
	}
	return lines
}

func (cleanup CleanupEvidence) ReportRow() CleanupEvidenceReportRow {
	class := "ok"
	if cleanup.Status != ObservationStatusOK || cleanup.FailureCount > 0 {
		class = "bad"
	}
	return CleanupEvidenceReportRow{
		Status:                 emptyAsDash(cleanup.Status),
		KubernetesResidueCount: cleanup.KubernetesResidueCount,
		ISCSIResidueCount:      cleanup.ISCSIResidueCount,
		MultipathResidueCount:  cleanup.MultipathResidueCount,
		ProcessResidueCount:    cleanup.ProcessResidueCount,
		HostPathResidueCount:   cleanup.HostPathResidueCount,
		FailureCount:           cleanup.FailureCount,
		EvidenceRef:            emptyAsDash(cleanup.EvidenceRef),
		StatusClass:            class,
	}
}

func cleanupIntFromSummary(summary map[string]string, key string) int {
	value := strings.TrimSpace(summary[key])
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0
	}
	return parsed
}
