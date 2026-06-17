package ops

import "strings"

const (
	InstallDriftStatusOK       = "ok"
	InstallDriftStatusMismatch = "mismatch"
	InstallDriftStatusUnknown  = "unknown"
)

func InstallDriftEvidenceFromSummary(summary map[string]string, evidenceRef string) *InstallDriftEvidence {
	if len(summary) == 0 {
		return nil
	}
	drift := &InstallDriftEvidence{
		Status:               strings.TrimSpace(summary["install_drift_status"]),
		ReasonCode:           strings.TrimSpace(summary["reason"]),
		ChartName:            strings.TrimSpace(summary["chart_name"]),
		CurrentChartVersion:  firstNonEmpty(summary["current_chart_version"], summary["chart_version"]),
		DesiredChartVersion:  strings.TrimSpace(summary["desired_chart_version"]),
		CurrentAppVersion:    firstNonEmpty(summary["current_app_version"], summary["chart_app_version"]),
		DesiredAppVersion:    strings.TrimSpace(summary["desired_app_version"]),
		CurrentImage:         strings.TrimSpace(summary["current_image"]),
		DesiredImage:         strings.TrimSpace(summary["desired_image"]),
		CurrentCSIImage:      strings.TrimSpace(summary["current_csi_image"]),
		DesiredCSIImage:      strings.TrimSpace(summary["desired_csi_image"]),
		CurrentOperatorImage: strings.TrimSpace(summary["current_operator_image"]),
		DesiredOperatorImage: strings.TrimSpace(summary["desired_operator_image"]),
		EvidenceRef:          evidenceRef,
	}
	if drift.Status == "" {
		drift.Status = deriveInstallDriftStatus(drift)
	}
	if drift.ReasonCode == "" {
		drift.ReasonCode = installDriftReason(drift.Status)
	}
	return drift
}

func installDriftCondition(drift *InstallDriftEvidence) *ObservationCondition {
	if drift == nil {
		return nil
	}
	switch drift.Status {
	case InstallDriftStatusOK:
		return &ObservationCondition{
			Type:         ConditionReady,
			Status:       "True",
			Reason:       installDriftReason(drift.Status),
			Severity:     "info",
			Message:      "install evidence is aligned",
			EvidenceRefs: installDriftEvidenceRefs(drift),
		}
	case InstallDriftStatusMismatch:
		return &ObservationCondition{
			Type:         ConditionBlocked,
			Status:       "True",
			Reason:       installDriftReason(drift.Status),
			Severity:     "warning",
			Message:      "install evidence does not match desired release identity",
			EvidenceRefs: installDriftEvidenceRefs(drift),
		}
	default:
		return &ObservationCondition{
			Type:         ConditionEvidenceStale,
			Status:       "True",
			Reason:       installDriftReason(drift.Status),
			Severity:     "warning",
			Message:      "install drift evidence is missing or incomplete",
			EvidenceRefs: installDriftEvidenceRefs(drift),
		}
	}
}

func installDriftEvidenceRefs(drift *InstallDriftEvidence) []string {
	if drift == nil || drift.EvidenceRef == "" {
		return nil
	}
	return []string{drift.EvidenceRef}
}

func installDriftReason(status string) string {
	switch status {
	case InstallDriftStatusOK:
		return ReasonInstallDriftAligned
	case InstallDriftStatusMismatch:
		return ReasonInstallDriftMismatch
	default:
		return ReasonInstallDriftEvidenceMissing
	}
}

func deriveInstallDriftStatus(drift *InstallDriftEvidence) string {
	pairs := [][2]string{
		{drift.CurrentChartVersion, drift.DesiredChartVersion},
		{drift.CurrentAppVersion, drift.DesiredAppVersion},
		{drift.CurrentImage, drift.DesiredImage},
		{drift.CurrentCSIImage, drift.DesiredCSIImage},
		{drift.CurrentOperatorImage, drift.DesiredOperatorImage},
	}
	observed := false
	for _, pair := range pairs {
		current := strings.TrimSpace(pair[0])
		desired := strings.TrimSpace(pair[1])
		if current == "" && desired == "" {
			continue
		}
		observed = true
		if current == "" || desired == "" || current != desired {
			return InstallDriftStatusMismatch
		}
	}
	if !observed {
		return InstallDriftStatusUnknown
	}
	return InstallDriftStatusOK
}
