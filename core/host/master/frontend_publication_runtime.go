package master

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type frontendPublicationRuntimeRequest struct {
	VolumeID                          string   `json:"volumeID"`
	ReplicaID                         string   `json:"replicaID"`
	TargetDataAddr                    string   `json:"targetDataAddr,omitempty"`
	TargetCtrlAddr                    string   `json:"targetCtrlAddr,omitempty"`
	SourceFailbackName                string   `json:"sourceFailbackName,omitempty"`
	FailbackCompleted                 bool     `json:"failbackCompleted"`
	AuthorityEpochAdvanced            bool     `json:"authorityEpochAdvanced"`
	SinglePrimaryAfterFailback        bool     `json:"singlePrimaryAfterFailback"`
	PublishTargetSwappedAfterFailback bool     `json:"publishTargetSwappedAfterFailback"`
	NoCrossVolumeIdentityChange       bool     `json:"noCrossVolumeIdentityChange"`
	EvidenceRefs                      []string `json:"evidenceRefs,omitempty"`
}

type frontendPublicationRuntimeResult struct {
	FrontendPublished           bool     `json:"frontendPublished"`
	FailbackStarted             bool     `json:"failbackStarted"`
	NoStorageMutation           bool     `json:"noStorageMutation"`
	NoCrossVolumeIdentityChange bool     `json:"noCrossVolumeIdentityChange"`
	EvidenceRefs                []string `json:"evidenceRefs,omitempty"`
}

func (h *Host) frontendPublicationRuntimeHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/runtime/frontend-publication", h.handleFrontendPublicationRuntime)
	return mux
}

func (h *Host) handleFrontendPublicationRuntime(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req frontendPublicationRuntimeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("decode frontend publication request: %v", err), http.StatusBadRequest)
		return
	}
	if err := validateFrontendPublicationRuntimeRequest(h, req); err != nil {
		http.Error(w, err.Error(), http.StatusPreconditionFailed)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(frontendPublicationRuntimeResult{
		FrontendPublished:           true,
		FailbackStarted:             false,
		NoStorageMutation:           true,
		NoCrossVolumeIdentityChange: req.NoCrossVolumeIdentityChange,
		EvidenceRefs: append([]string{
			"frontend_publication_authority_line_verified",
		}, req.EvidenceRefs...),
	})
}

func validateFrontendPublicationRuntimeRequest(h *Host, req frontendPublicationRuntimeRequest) error {
	if req.VolumeID == "" || req.ReplicaID == "" {
		return fmt.Errorf("frontend publication runtime: volumeID and replicaID are required")
	}
	if req.TargetDataAddr == "" || req.TargetCtrlAddr == "" {
		return fmt.Errorf("frontend publication runtime: targetDataAddr and targetCtrlAddr are required")
	}
	if req.SourceFailbackName == "" ||
		!req.FailbackCompleted ||
		!req.AuthorityEpochAdvanced ||
		!req.SinglePrimaryAfterFailback ||
		!req.PublishTargetSwappedAfterFailback ||
		!req.NoCrossVolumeIdentityChange {
		return fmt.Errorf("frontend publication runtime: terminal failback evidence is required")
	}
	line, ok := h.Publisher().VolumeAuthorityLine(req.VolumeID)
	if !ok {
		return fmt.Errorf("frontend publication runtime: missing authority line for volume %s", req.VolumeID)
	}
	if line.ReplicaID != req.ReplicaID || line.DataAddr != req.TargetDataAddr || line.CtrlAddr != req.TargetCtrlAddr {
		return fmt.Errorf("frontend publication runtime: authority line mismatch got %s %s/%s want %s %s/%s",
			line.ReplicaID, line.DataAddr, line.CtrlAddr,
			req.ReplicaID, req.TargetDataAddr, req.TargetCtrlAddr)
	}
	return nil
}
