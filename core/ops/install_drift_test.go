package ops

import "testing"

func TestInstallDriftEvidenceFromSummary_DerivesAlignedMismatchAndMissing(t *testing.T) {
	aligned := InstallDriftEvidenceFromSummary(map[string]string{
		"current_image":         "sw-block:sha-a",
		"desired_image":         "sw-block:sha-a",
		"current_csi_image":     "sw-block-csi:sha-a",
		"desired_csi_image":     "sw-block-csi:sha-a",
		"current_chart_version": "0.3.5",
		"desired_chart_version": "0.3.5",
	}, "install-drift-summary.txt")
	if aligned.Status != InstallDriftStatusOK || aligned.ReasonCode != ReasonInstallDriftAligned {
		t.Fatalf("aligned=%+v", aligned)
	}

	mismatch := InstallDriftEvidenceFromSummary(map[string]string{
		"current_image": "sw-block:old",
		"desired_image": "sw-block:new",
	}, "install-drift-summary.txt")
	if mismatch.Status != InstallDriftStatusMismatch || mismatch.ReasonCode != ReasonInstallDriftMismatch {
		t.Fatalf("mismatch=%+v", mismatch)
	}

	missing := InstallDriftEvidenceFromSummary(map[string]string{
		"install_drift_status": InstallDriftStatusUnknown,
	}, "install-drift-summary.txt")
	if missing.Status != InstallDriftStatusUnknown || missing.ReasonCode != ReasonInstallDriftEvidenceMissing {
		t.Fatalf("missing=%+v", missing)
	}
}
