package ops

import (
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	hostvolume "github.com/seaweedfs/seaweed-block/core/host/volume"
	"github.com/seaweedfs/seaweed-block/core/replication"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

const (
	// VolumeStatusReportSchemaVersion is the first append-only ops status
	// report contract. The report is read-only evidence, not an admin command
	// input and not a block/data snapshot.
	VolumeStatusReportSchemaVersion = "1.0"
	Unavailable                     = "unavailable"
)

// VolumeStatusReportInput is the component-test seam for assembling the first
// operations-layer volume status report from existing product facts.
type VolumeStatusReportInput struct {
	CapturedAt      time.Time
	Source          ReportSource
	ProductRevision string
	RunnerRevision  string

	MasterStatus *control.StatusResponse
	LocalStatus  *hostvolume.StatusProjection
	Peers        []replication.ReplicaPeerStatus
	Durable      []durable.VolumeStatus
	Residue      ResidueReport
}

type ReportSource struct {
	Component string `json:"component"`
	Host      string `json:"host,omitempty"`
	Scenario  string `json:"scenario,omitempty"`
}

type VolumeStatusReport struct {
	SchemaVersion    string            `json:"schema_version"`
	CapturedAt       time.Time         `json:"captured_at"`
	Source           ReportSource      `json:"source"`
	ProductRevision  string            `json:"product_revision"`
	RunnerRevision   string            `json:"runner_revision,omitempty"`
	CollectionErrors []string          `json:"collection_errors,omitempty"`
	Volume           VolumeReport      `json:"volume"`
	Authority        AuthorityReport   `json:"authority"`
	Replication      ReplicationReport `json:"replication"`
	Durable          []DurableReport   `json:"durable"`
	Residue          ResidueReport     `json:"residue"`
}

type VolumeReport struct {
	VolumeID  string           `json:"volume_id"`
	ReplicaID string           `json:"replica_id"`
	Protocols []string         `json:"protocols"`
	Frontends []FrontendReport `json:"frontends"`
}

type FrontendReport struct {
	Protocol  string `json:"protocol"`
	Transport string `json:"transport,omitempty"`
	Addr      string `json:"addr,omitempty"`
	IQN       string `json:"iqn,omitempty"`
	NQN       string `json:"nqn,omitempty"`
	LUN       uint32 `json:"lun"`
	NSID      uint32 `json:"nsid"`
}

type AuthorityReport struct {
	Epoch                  uint64 `json:"epoch"`
	EndpointVersion        uint64 `json:"endpoint_version"`
	Assigned               bool   `json:"assigned"`
	AuthorityRole          string `json:"authority_role"`
	FrontendPrimaryReady   bool   `json:"frontend_primary_ready"`
	Healthy                bool   `json:"healthy"`
	LastUnsupportedReason  string `json:"last_unsupported_reason,omitempty"`
	LastConvergenceStuckAt string `json:"last_convergence_stuck_at,omitempty"`
}

type ReplicationReport struct {
	ReplicationRole string       `json:"replication_role"`
	Peers           []PeerReport `json:"peers"`
}

type PeerReport struct {
	ReplicaID       string `json:"replica_id"`
	State           string `json:"state"`
	DataAddr        string `json:"data_addr,omitempty"`
	CtrlAddr        string `json:"ctrl_addr,omitempty"`
	Healthy         bool   `json:"healthy"`
	Epoch           uint64 `json:"epoch"`
	EndpointVersion uint64 `json:"endpoint_version"`
	SessionID       uint64 `json:"session_id"`
	ProbeInFlight   bool   `json:"probe_in_flight"`
	Closed          bool   `json:"closed"`
	LastError       string `json:"last_error"`
}

type DurableReport struct {
	VolumeID        string `json:"volume_id"`
	Impl            string `json:"impl"`
	Path            string `json:"path"`
	ReplicaID       string `json:"replica_id"`
	Epoch           uint64 `json:"epoch"`
	EndpointVersion uint64 `json:"endpoint_version"`
	Latched         bool   `json:"latched"`
	Operational     bool   `json:"operational"`
	FrontierKnown   bool   `json:"frontier_known"`
	DurableLSN      uint64 `json:"durable_lsn"`
	RetainedLSN     uint64 `json:"retained_lsn"`
	HeadLSN         uint64 `json:"head_lsn"`
	Closed          bool   `json:"closed"`
	Evidence        string `json:"evidence,omitempty"`
}

type ResidueReport struct {
	HostInitiator HostInitiatorResidue `json:"host_initiator"`
	Processes     []string             `json:"processes"`
	Kubernetes    []string             `json:"kubernetes"`
	StoragePaths  []string             `json:"storage_paths"`
	Unchecked     []string             `json:"unchecked,omitempty"`
}

type HostInitiatorResidue struct {
	ISCSISessions  []string `json:"iscsi_sessions"`
	NVMESubsystems []string `json:"nvme_subsystems"`
}

// BuildVolumeStatusReport normalizes existing status surfaces into the first
// operations-layer JSON contract. It must not collect or mutate anything; all
// inputs are supplied by callers/tests.
func BuildVolumeStatusReport(in VolumeStatusReportInput) VolumeStatusReport {
	capturedAt := in.CapturedAt
	if capturedAt.IsZero() {
		capturedAt = time.Now().UTC()
	} else {
		capturedAt = capturedAt.UTC()
	}

	volumeID, replicaID := volumeIdentity(in)
	frontends := frontendSnapshots(in.MasterStatus.GetFrontends())
	protocols := frontendProtocols(frontends)
	source := in.Source
	source.Component = explicitUnavailable(source.Component)

	return VolumeStatusReport{
		SchemaVersion:   VolumeStatusReportSchemaVersion,
		CapturedAt:      capturedAt,
		Source:          source,
		ProductRevision: explicitUnavailable(in.ProductRevision),
		RunnerRevision:  in.RunnerRevision,
		Volume: VolumeReport{
			VolumeID:  volumeID,
			ReplicaID: replicaID,
			Protocols: protocols,
			Frontends: frontends,
		},
		Authority:   authoritySnapshot(in.MasterStatus, in.LocalStatus),
		Replication: replicationSnapshot(in.LocalStatus, in.Peers),
		Durable:     durableSnapshots(in.Durable),
		Residue:     copyResidue(in.Residue),
	}
}

func volumeIdentity(in VolumeStatusReportInput) (volumeID, replicaID string) {
	if in.LocalStatus != nil {
		volumeID = in.LocalStatus.VolumeID
		replicaID = in.LocalStatus.ReplicaID
	}
	if volumeID == "" && in.MasterStatus != nil {
		volumeID = in.MasterStatus.GetVolumeId()
	}
	if replicaID == "" && in.MasterStatus != nil {
		replicaID = in.MasterStatus.GetReplicaId()
	}
	if volumeID == "" && len(in.Durable) > 0 {
		volumeID = in.Durable[0].VolumeID
	}
	if replicaID == "" && len(in.Durable) > 0 {
		replicaID = in.Durable[0].ReplicaID
	}
	return explicitUnavailable(volumeID), explicitUnavailable(replicaID)
}

func frontendSnapshots(targets []*control.FrontendTarget) []FrontendReport {
	out := make([]FrontendReport, 0, len(targets))
	for _, t := range targets {
		if t == nil {
			continue
		}
		out = append(out, FrontendReport{
			Protocol:  t.GetProtocol(),
			Transport: t.GetTransport(),
			Addr:      t.GetAddr(),
			IQN:       t.GetIqn(),
			NQN:       t.GetNqn(),
			LUN:       t.GetLun(),
			NSID:      t.GetNsid(),
		})
	}
	return out
}

func frontendProtocols(frontends []FrontendReport) []string {
	seen := map[string]bool{}
	for _, f := range frontends {
		if f.Protocol == "" {
			continue
		}
		seen[f.Protocol] = true
	}
	out := make([]string, 0, len(seen))
	for protocol := range seen {
		out = append(out, protocol)
	}
	sort.Strings(out)
	return out
}

func authoritySnapshot(master *control.StatusResponse, local *hostvolume.StatusProjection) AuthorityReport {
	s := AuthorityReport{
		AuthorityRole: Unavailable,
	}
	if master != nil {
		s.Epoch = master.GetEpoch()
		s.EndpointVersion = master.GetEndpointVersion()
		s.Assigned = master.GetAssigned()
		s.LastUnsupportedReason = master.GetLastUnsupportedReason()
		s.LastConvergenceStuckAt = master.GetLastConvergenceStuckAt()
	}
	if local != nil {
		s.Epoch = local.Epoch
		s.EndpointVersion = local.EndpointVersion
		s.AuthorityRole = explicitUnavailable(local.AuthorityRole)
		s.FrontendPrimaryReady = local.FrontendPrimaryReady
		s.Healthy = local.Healthy
	}
	return s
}

func replicationSnapshot(local *hostvolume.StatusProjection, peers []replication.ReplicaPeerStatus) ReplicationReport {
	s := ReplicationReport{
		ReplicationRole: Unavailable,
		Peers:           make([]PeerReport, 0, len(peers)),
	}
	if local != nil {
		s.ReplicationRole = explicitUnavailable(local.ReplicationRole)
	}
	for _, p := range peers {
		s.Peers = append(s.Peers, PeerReport{
			ReplicaID:       p.ReplicaID,
			State:           p.State,
			DataAddr:        p.DataAddr,
			CtrlAddr:        p.CtrlAddr,
			Healthy:         strings.EqualFold(p.State, "healthy"),
			Epoch:           p.Epoch,
			EndpointVersion: p.EndpointVersion,
			SessionID:       p.SessionID,
			ProbeInFlight:   p.ProbeInFlight,
			Closed:          p.Closed,
			LastError:       Unavailable,
		})
	}
	return s
}

func durableSnapshots(statuses []durable.VolumeStatus) []DurableReport {
	out := make([]DurableReport, 0, len(statuses))
	for _, st := range statuses {
		out = append(out, DurableReport{
			VolumeID:        st.VolumeID,
			Impl:            st.Impl,
			Path:            st.Path,
			ReplicaID:       st.ReplicaID,
			Epoch:           st.Epoch,
			EndpointVersion: st.EndpointVersion,
			Latched:         st.Latched,
			Operational:     st.Operational,
			FrontierKnown:   st.FrontierKnown,
			DurableLSN:      st.DurableLSN,
			RetainedLSN:     st.RetainedLSN,
			HeadLSN:         st.HeadLSN,
			Closed:          st.Closed,
			Evidence:        st.Evidence,
		})
	}
	return out
}

func copyResidue(in ResidueReport) ResidueReport {
	return ResidueReport{
		HostInitiator: HostInitiatorResidue{
			ISCSISessions:  copyStringSlice(in.HostInitiator.ISCSISessions),
			NVMESubsystems: copyStringSlice(in.HostInitiator.NVMESubsystems),
		},
		Processes:    copyStringSlice(in.Processes),
		Kubernetes:   copyStringSlice(in.Kubernetes),
		StoragePaths: copyStringSlice(in.StoragePaths),
		Unchecked:    copyStringSlice(in.Unchecked),
	}
}

func copyStringSlice(in []string) []string {
	if in == nil {
		return []string{}
	}
	return append([]string(nil), in...)
}

func explicitUnavailable(v string) string {
	if v == "" {
		return Unavailable
	}
	return v
}
