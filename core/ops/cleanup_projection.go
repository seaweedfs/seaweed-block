package ops

const (
	ReasonCleanupVerified = "cleanup_verified"
	ReasonCleanupRequired = "cleanup_required"
)

func cleanupRequired(cleanup *CleanupEvidence) bool {
	if cleanup == nil {
		return false
	}
	return cleanup.Status != ObservationStatusOK ||
		cleanup.KubernetesResidueCount > 0 ||
		cleanup.ISCSIResidueCount > 0 ||
		cleanup.MultipathResidueCount > 0 ||
		cleanup.ProcessResidueCount > 0 ||
		cleanup.HostPathResidueCount > 0 ||
		cleanup.FailureCount > 0
}

func cleanupReason(cleanup *CleanupEvidence) string {
	if cleanup != nil && len(cleanup.ReasonCodes) > 0 && cleanup.ReasonCodes[0] != "" {
		return cleanup.ReasonCodes[0]
	}
	if cleanupRequired(cleanup) {
		return ReasonCleanupRequired
	}
	return ReasonCleanupVerified
}

func cleanupCondition(cleanup *CleanupEvidence) *ObservationCondition {
	if cleanup == nil {
		return nil
	}
	condition := ObservationCondition{
		Type:     ConditionCleanupRequired,
		Status:   "False",
		Reason:   ReasonCleanupVerified,
		Severity: "info",
		Message:  "cleanup verifier found no Seaweed Block residue",
	}
	if cleanupRequired(cleanup) {
		condition.Status = "True"
		condition.Reason = cleanupReason(cleanup)
		condition.Severity = "warning"
		condition.Message = "cleanup verifier found Seaweed Block residue; run the scripted verifier/cleanup path"
	}
	return &condition
}

func cleanupEvidenceRefs(cleanup *CleanupEvidence) []string {
	if cleanup == nil || cleanup.EvidenceRef == "" {
		return nil
	}
	return []string{cleanup.EvidenceRef}
}

func cleanupSafeNextStep(cleanup *CleanupEvidence) *SwBlockSafeNextStep {
	if !cleanupRequired(cleanup) {
		return nil
	}
	return &SwBlockSafeNextStep{
		Type:            ManagedVolumeActionVerifyCleanup,
		Mode:            ManagedVolumeActionModeScripted,
		Command:         `bash scripts/verify-helm-cleanup.sh "$PWD"`,
		ReasonCode:      cleanupReason(cleanup),
		MutationAllowed: false,
		EvidenceRefs:    cleanupEvidenceRefs(cleanup),
	}
}
