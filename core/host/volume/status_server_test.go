// Ownership: sw regression test (architect finding 2026-04-21:
// status HTTP must reject non-loopback bind and non-loopback
// client to avoid shipping an unauthenticated diagnostic API).
package volume

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
)

func TestStatusServer_RejectsNonLoopbackBind(t *testing.T) {
	cases := []string{
		"0.0.0.0:0",
		"8.8.8.8:0",
		":0", // empty host = all interfaces
	}
	for _, addr := range cases {
		t.Run(addr, func(t *testing.T) {
			s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
			if _, err := s.Start(addr); err == nil {
				_ = s.Close(context.Background())
				t.Fatalf("bind %q must be rejected", addr)
			} else if !strings.Contains(err.Error(), "loopback") {
				t.Fatalf("bind %q rejected but error does not mention loopback: %v", addr, err)
			}
		})
	}
}

func TestStatusServer_AcceptsLoopbackBindAndServesJSON(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("loopback bind: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.Close(ctx)
		cancel()
	}()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("loopback client status: %d want 200", resp.StatusCode)
	}
}

func TestNodeLoss_StatusServer_ExternalBindPolicyRequiresConcreteHost(t *testing.T) {
	if err := enforceExplicitExternalBind("10.0.0.12:23260"); err != nil {
		t.Fatalf("concrete node address rejected: %v", err)
	}
	for _, addr := range []string{":23260", "0.0.0.0:23260", "[::]:23260", "127.0.0.1:23260", "localhost:23260"} {
		t.Run(addr, func(t *testing.T) {
			if err := enforceExplicitExternalBind(addr); err == nil {
				t.Fatalf("external status bind %q must be rejected", addr)
			}
		})
	}
}

func TestNodeLoss_StatusServer_AllowExternalAccessServesNonLoopbackClient(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy, Epoch: 5, EndpointVersion: 3}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))

	req := httptest.NewRequest(http.MethodGet, "/status?volume=v1", nil)
	req.RemoteAddr = "10.0.0.9:44100"
	w := httptest.NewRecorder()
	s.handleStatus(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("default non-loopback client status=%d want 403", w.Code)
	}

	s.AllowExternalAccess()
	w = httptest.NewRecorder()
	s.handleStatus(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("external non-loopback client status=%d want 200 body=%s", w.Code, w.Body.String())
	}
}

func TestG9A_StatusProjection_ReturnedOldPrimarySupersededNotReady(t *testing.T) {
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r1"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r2",
		Epoch:           2,
		EndpointVersion: 1,
	})

	s := NewStatusServer(h.view)
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var body StatusProjection
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Healthy {
		t.Fatalf("returned old primary must fail closed: %+v", body)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("returned old primary must not be frontend-primary-ready: %+v", body)
	}
	if body.AuthorityRole != AuthorityRoleSuperseded {
		t.Fatalf("authority role = %q want %q", body.AuthorityRole, AuthorityRoleSuperseded)
	}
	if body.ReplicationRole != ReplicationRoleNotReady {
		t.Fatalf("replication role = %q want %q", body.ReplicationRole, ReplicationRoleNotReady)
	}
}

func TestG9A_StatusProjection_EngineRecoveringMapsReplicationRole(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{
		Mode:             engine.ModeRecovering,
		RecoveryDecision: engine.DecisionCatchUp,
		SessionKind:      engine.SessionCatchUp,
		SessionPhase:     engine.PhaseRunning,
		Epoch:            3,
		EndpointVersion:  2,
	}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var body StatusProjection
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Healthy || body.FrontendPrimaryReady {
		t.Fatalf("recovering replica must not be frontend-primary-ready: %+v", body)
	}
	if body.ReplicationRole != ReplicationRoleRecovering {
		t.Fatalf("replication role = %q want %q", body.ReplicationRole, ReplicationRoleRecovering)
	}
}

func TestG9B_StatusProjection_GenesisObservedBeforeAssignment_NotFrontendReady(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var body StatusProjection
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.FrontendPrimaryReady || body.Healthy {
		t.Fatalf("observed process before assignment must not be frontend-ready: %+v", body)
	}
	if body.AuthorityRole != AuthorityRoleUnknown {
		t.Fatalf("authority role = %q want %q", body.AuthorityRole, AuthorityRoleUnknown)
	}
}

func TestG9B_StatusProjection_AssignmentConsumed_MakesFrontendPrimaryReady(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{
		Mode: engine.ModeHealthy, Epoch: 1, EndpointVersion: 1,
	}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", resp.StatusCode)
	}
	var body StatusProjection
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.FrontendPrimaryReady || !body.Healthy {
		t.Fatalf("assignment-consumed healthy projection must be frontend-ready: %+v", body)
	}
	if body.AuthorityRole != AuthorityRolePrimary {
		t.Fatalf("authority role = %q want %q", body.AuthorityRole, AuthorityRolePrimary)
	}
}

func TestG9B_StatusProjection_ReturnedReplicaLifecycle_NotReadyRecoveringReady(t *testing.T) {
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode:            engine.ModeDegraded,
		Epoch:           3,
		EndpointVersion: 2,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r2"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r2", h)
	// Current authority line names r1, so r2 is a returned/registered
	// replica candidate, not the frontend primary.
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r1",
		Epoch:           3,
		EndpointVersion: 2,
	})
	s := NewStatusServer(h.view)

	body := s.statusProjection()
	if body.AuthorityRole != AuthorityRoleUnknown {
		t.Fatalf("candidate authority role = %q want %q", body.AuthorityRole, AuthorityRoleUnknown)
	}
	if body.ReplicationRole != ReplicationRoleNotReady {
		t.Fatalf("candidate replication role = %q want %q", body.ReplicationRole, ReplicationRoleNotReady)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("candidate must not be frontend ready: %+v", body)
	}

	proj.p = engine.ReplicaProjection{
		Mode:             engine.ModeRecovering,
		RecoveryDecision: engine.DecisionCatchUp,
		SessionKind:      engine.SessionCatchUp,
		SessionPhase:     engine.PhaseRunning,
		Epoch:            3,
		EndpointVersion:  2,
	}
	body = s.statusProjection()
	if body.ReplicationRole != ReplicationRoleRecovering {
		t.Fatalf("recovering replication role = %q want %q", body.ReplicationRole, ReplicationRoleRecovering)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("recovering replica must not be frontend ready: %+v", body)
	}

	proj.p = engine.ReplicaProjection{
		Mode:            engine.ModeHealthy,
		Epoch:           3,
		EndpointVersion: 2,
	}
	body = s.statusProjection()
	if body.AuthorityRole != AuthorityRoleUnknown {
		t.Fatalf("ready supporting replica authority role = %q want %q", body.AuthorityRole, AuthorityRoleUnknown)
	}
	if body.ReplicationRole != ReplicationRoleReady {
		t.Fatalf("ready supporting replica role = %q want %q", body.ReplicationRole, ReplicationRoleReady)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("supporting replica_ready must not be frontend primary ready: %+v", body)
	}
}

func TestG9B_StatusProjection_ComponentAdapterLifecycle_NotReadyRecoveringReady(t *testing.T) {
	exec := newG9BStatusExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r2"}}
	h.view = NewAdapterProjectionView(a, "v1", "r2", h)
	// Current frontend authority line names r1. r2 may become a
	// supporting replica_ready after recovery, but never the frontend
	// primary for this line.
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r1",
		Epoch:           3,
		EndpointVersion: 2,
	})
	s := NewStatusServer(h.view)

	a.OnAssignment(adapter.AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 3, EndpointVersion: 2,
		DataAddr: "d2", CtrlAddr: "c2",
	})
	exec.waitProbe(t)
	body := s.statusProjection()
	if body.ReplicationRole != ReplicationRoleNotReady {
		t.Fatalf("post-assignment candidate role=%q want %q", body.ReplicationRole, ReplicationRoleNotReady)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("post-assignment candidate must not be frontend ready: %+v", body)
	}

	a.OnProbeResult(adapter.ProbeResult{
		ReplicaID: "r2", Success: true,
		EndpointVersion: 2, TransportEpoch: 3,
		ReplicaFlushedLSN: 4, PrimaryTailLSN: 1, PrimaryHeadLSN: 10,
	})
	body = s.statusProjection()
	if body.ReplicationRole != ReplicationRoleRecovering {
		t.Fatalf("after catch-up probe role=%q want %q", body.ReplicationRole, ReplicationRoleRecovering)
	}
	if exec.lastSessionID == 0 {
		t.Fatal("catch-up probe did not start a recovery session")
	}

	exec.completeSession(adapter.SessionCloseResult{
		ReplicaID: "r2", SessionID: exec.lastSessionID,
		Success: true, AchievedLSN: 10,
	})
	body = s.statusProjection()
	if body.ReplicationRole != ReplicationRoleRecovering {
		t.Fatalf("after completed recovery before durable ack role=%q want %q", body.ReplicationRole, ReplicationRoleRecovering)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("post-close recovered replica must not become frontend primary: %+v", body)
	}

	exec.durableAck(adapter.DurableAckResult{
		ReplicaID: "r2", SessionID: exec.lastSessionID,
		DurableLSN: 10, PrimaryTailLSN: 1, PrimaryHeadLSN: 10,
	})
	body = s.statusProjection()
	if body.ReplicationRole != ReplicationRoleReady {
		t.Fatalf("after post-close durable ack role=%q want %q", body.ReplicationRole, ReplicationRoleReady)
	}
	if body.FrontendPrimaryReady {
		t.Fatalf("supporting replica_ready must not become frontend primary: %+v", body)
	}
}

func TestStatusServer_MissingVolume_Returns400(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 {
		t.Fatalf("missing volume: got %d want 400", resp.StatusCode)
	}
}

func TestStatusServer_WrongVolume_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status?volume=otherVol")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("wrong volume: got %d want 404", resp.StatusCode)
	}
}

func TestIsLoopbackRemote(t *testing.T) {
	cases := map[string]bool{
		"127.0.0.1:34567": true,
		"[::1]:80":        true,
		"8.8.8.8:80":      false,
		"":                false,
		"garbage":         false,
	}
	for in, want := range cases {
		if got := isLoopbackRemote(in); got != want {
			t.Errorf("isLoopbackRemote(%q) = %v, want %v", in, got, want)
		}
	}
}

type g9bStatusExecutor struct {
	onSessionStart  adapter.OnSessionStart
	onSessionClose  adapter.OnSessionClose
	onDurableAck    adapter.OnDurableAck
	onFenceComplete adapter.OnFenceComplete
	lastSessionID   uint64
	probeCh         chan struct{}
}

func newG9BStatusExecutor() *g9bStatusExecutor {
	return &g9bStatusExecutor{probeCh: make(chan struct{}, 4)}
}

func (e *g9bStatusExecutor) SetOnSessionStart(fn adapter.OnSessionStart) {
	e.onSessionStart = fn
}

func (e *g9bStatusExecutor) SetOnSessionClose(fn adapter.OnSessionClose) {
	e.onSessionClose = fn
}

func (e *g9bStatusExecutor) SetOnDurableAck(fn adapter.OnDurableAck) {
	e.onDurableAck = fn
}

func (e *g9bStatusExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete) {
	e.onFenceComplete = fn
}

func (e *g9bStatusExecutor) Probe(replicaID, _dataAddr, _ctrlAddr string, _sessionID, epoch, endpointVersion uint64) adapter.ProbeResult {
	e.probeCh <- struct{}{}
	return adapter.ProbeResult{
		ReplicaID:         replicaID,
		Success:           true,
		TransportEpoch:    epoch,
		EndpointVersion:   endpointVersion,
		ReplicaFlushedLSN: 0,
		PrimaryTailLSN:    0,
		PrimaryHeadLSN:    0,
	}
}

func (e *g9bStatusExecutor) waitProbe(t *testing.T) {
	t.Helper()
	select {
	case <-e.probeCh:
	case <-time.After(time.Second):
		t.Fatal("assignment did not dispatch probe")
	}
}

func (e *g9bStatusExecutor) StartCatchUp(replicaID string, sessionID, _epoch, _endpointVersion, _fromLSN, _frontierHint uint64) error {
	e.lastSessionID = sessionID
	if e.onSessionStart != nil {
		e.onSessionStart(adapter.SessionStartResult{ReplicaID: replicaID, SessionID: sessionID})
	}
	return nil
}

func (e *g9bStatusExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, frontierHint uint64) error {
	return e.StartCatchUp(replicaID, sessionID, epoch, endpointVersion, 0, frontierHint)
}

func (e *g9bStatusExecutor) StartRecoverySession(replicaID string, sessionID, epoch, endpointVersion, frontierHint uint64, _ engine.RecoveryContentKind, _ engine.RecoveryRuntimePolicy) error {
	return e.StartCatchUp(replicaID, sessionID, epoch, endpointVersion, 0, frontierHint)
}

func (e *g9bStatusExecutor) InvalidateSession(string, uint64, string) {}

func (e *g9bStatusExecutor) PublishHealthy(string) {}

func (e *g9bStatusExecutor) PublishDegraded(string, string) {}

func (e *g9bStatusExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	if e.onFenceComplete != nil {
		e.onFenceComplete(adapter.FenceResult{
			ReplicaID:       replicaID,
			SessionID:       sessionID,
			Epoch:           epoch,
			EndpointVersion: endpointVersion,
			Success:         true,
		})
	}
	return nil
}

func (e *g9bStatusExecutor) completeSession(result adapter.SessionCloseResult) {
	if e.onSessionClose != nil {
		e.onSessionClose(result)
	}
}

func (e *g9bStatusExecutor) durableAck(result adapter.DurableAckResult) {
	if e.onDurableAck != nil {
		e.onDurableAck(result)
	}
}

// TestStatusServer_RecoveryEndpoint_Disabled_Returns404 — G5-5
// regression: /status/recovery is OPT-IN. Default config (no
// EnableRecoveryEndpoint call) must NOT route the path.
func TestStatusServer_RecoveryEndpoint_Disabled_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(
		stubProjector{p: engine.ReplicaProjection{Mode: engine.ModeHealthy}},
		"v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/recovery?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("disabled recovery endpoint: got %d want 404 (mux must not route)", resp.StatusCode)
	}
}

// TestStatusServer_RecoveryEndpoint_Enabled_ReturnsEngineProjection
// — pins that EnableRecoveryEndpoint exposes the engine projection
// over HTTP with R/S/H + Mode + RecoveryDecision visible (the fields
// G5-5 hardware-test catch-up verification reads).
func TestStatusServer_RecoveryEndpoint_Enabled_ReturnsEngineProjection(t *testing.T) {
	p := stubProjector{p: engine.ReplicaProjection{
		Mode:             engine.ModeRecovering,
		RecoveryDecision: "catch_up",
		R:                42,
		S:                10,
		H:                100,
		Epoch:            7,
		EndpointVersion:  3,
	}}
	s := NewStatusServer(NewAdapterProjectionView(p, "v1", "r1", nil))
	s.EnableRecoveryEndpoint()
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/recovery?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("enabled recovery: got %d want 200", resp.StatusCode)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, want := range []string{"R", "S", "H", "Mode", "RecoveryDecision", "Epoch", "EndpointVersion"} {
		if _, ok := body[want]; !ok {
			t.Errorf("recovery body missing field %q (need for G5-5 catch-up verifier); body=%v", want, body)
		}
	}
	if got, _ := body["R"].(float64); got != 42 {
		t.Errorf("R: got %v want 42", body["R"])
	}
	if got, _ := body["H"].(float64); got != 100 {
		t.Errorf("H: got %v want 100", body["H"])
	}
}

// TestStatusServer_RecoveryEndpoint_WrongVolume_Returns404 — pins
// that the wrong-volume guard fires the same way as /status.
func TestStatusServer_RecoveryEndpoint_WrongVolume_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	s.EnableRecoveryEndpoint()
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/recovery?volume=other")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("wrong volume: got %d want 404", resp.StatusCode)
	}
}

type stubPeerStatusSource struct {
	peers []replication.ReplicaPeerStatus
}

func (s stubPeerStatusSource) PeerStatuses() []replication.ReplicaPeerStatus {
	return append([]replication.ReplicaPeerStatus(nil), s.peers...)
}

func TestStatusServer_PeerStatusEndpoint_Disabled_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/peers?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("disabled peers endpoint: got %d want 404", resp.StatusCode)
	}
}

func TestStatusServer_PeerStatusEndpoint_EnabledAfterStart_ReturnsPeerStates(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r2", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	s.SetPeerStatusSource(stubPeerStatusSource{peers: []replication.ReplicaPeerStatus{{
		ReplicaID:       "r1",
		State:           "healthy",
		Epoch:           2,
		EndpointVersion: 1,
		DataAddr:        "127.0.0.1:19601",
		CtrlAddr:        "127.0.0.1:19602",
		SessionID:       5,
	}}})

	resp, err := http.Get("http://" + addr + "/status/peers?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("enabled peers: got %d want 200", resp.StatusCode)
	}
	var body struct {
		VolumeID  string
		ReplicaID string
		PeerCount int
		Peers     []replication.ReplicaPeerStatus
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.VolumeID != "v1" || body.ReplicaID != "r2" || body.PeerCount != 1 {
		t.Fatalf("unexpected peers body header: %+v", body)
	}
	if len(body.Peers) != 1 || body.Peers[0].ReplicaID != "r1" || body.Peers[0].State != "healthy" || body.Peers[0].Epoch != 2 {
		t.Fatalf("unexpected peer body: %+v", body.Peers)
	}
}

type stubDurableStatusSource struct {
	vols []durable.VolumeStatus
}

func (s stubDurableStatusSource) DurableStatuses() []durable.VolumeStatus {
	return append([]durable.VolumeStatus(nil), s.vols...)
}

func TestStatusServer_DurableStatusEndpoint_Disabled_Returns404(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	resp, err := http.Get("http://" + addr + "/status/durable?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("disabled durable endpoint: got %d want 404", resp.StatusCode)
	}
}

func TestStatusServer_DurableStatusEndpoint_EnabledAfterStart_ReturnsLineage(t *testing.T) {
	s := NewStatusServer(NewAdapterProjectionView(stubProjector{}, "v1", "r1", nil))
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{
		VolumeID:        "v1",
		Path:            "/tmp/v1.bin",
		Impl:            "smartwal",
		ReplicaID:       "r1",
		Epoch:           1,
		EndpointVersion: 1,
		Latched:         true,
		Operational:     true,
		Evidence:        "recovered LSN=0",
	}}})

	resp, err := http.Get("http://" + addr + "/status/durable?volume=v1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("enabled durable: got %d want 200", resp.StatusCode)
	}
	var body struct {
		VolumeID    string
		ReplicaID   string
		VolumeCount int
		Volumes     []durable.VolumeStatus
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.VolumeID != "v1" || body.ReplicaID != "r1" || body.VolumeCount != 1 {
		t.Fatalf("unexpected durable body header: %+v", body)
	}
	if len(body.Volumes) != 1 || !body.Volumes[0].Latched || !body.Volumes[0].Operational || body.Volumes[0].Epoch != 1 {
		t.Fatalf("unexpected durable volume body: %+v", body.Volumes)
	}
}

func TestG9B_StatusProjection_ReturnedReplicaCanBeDurableReadyButFrontendNotReady(t *testing.T) {
	proj := &mutableProjector{p: engine.ReplicaProjection{
		Mode:            engine.ModeHealthy,
		Epoch:           2,
		EndpointVersion: 1,
	}}
	h := &Host{cfg: Config{VolumeID: "v1", ReplicaID: "r1"}}
	h.view = NewAdapterProjectionView(proj, "v1", "r1", h)
	// Same lineage as this replica, but authority names r2. This is a
	// returned supporting replica: local durable state may be recovered and
	// usable by replication, but frontend I/O must remain fenced.
	h.recordOtherLine(&control.AssignmentFact{
		VolumeId:        "v1",
		ReplicaId:       "r2",
		Epoch:           2,
		EndpointVersion: 1,
	})
	s := NewStatusServer(h.view)
	s.SetDurableStatusSource(stubDurableStatusSource{vols: []durable.VolumeStatus{{
		VolumeID:        "v1",
		Path:            "/tmp/v1.bin",
		Impl:            "smartwal",
		ReplicaID:       "r1",
		Epoch:           2,
		EndpointVersion: 1,
		Latched:         true,
		Operational:     true,
		Evidence:        "recovered LSN=12",
	}}})
	addr, err := s.Start("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() { _ = s.Close(context.Background()) }()

	statusResp, err := http.Get("http://" + addr + "/status?volume=v1")
	if err != nil {
		t.Fatalf("get status: %v", err)
	}
	defer statusResp.Body.Close()
	if statusResp.StatusCode != 200 {
		t.Fatalf("status: got %d want 200", statusResp.StatusCode)
	}
	var statusBody StatusProjection
	if err := json.NewDecoder(statusResp.Body).Decode(&statusBody); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if statusBody.VolumeID != "v1" || statusBody.ReplicaID != "r1" || statusBody.Epoch != 2 || statusBody.EndpointVersion != 1 {
		t.Fatalf("returned replica status reported wrong lineage: %+v", statusBody)
	}
	if statusBody.Healthy || statusBody.FrontendPrimaryReady {
		t.Fatalf("returned replica must not be frontend-ready: %+v", statusBody)
	}
	if statusBody.AuthorityRole != AuthorityRoleUnknown {
		t.Fatalf("authority role=%q want %q", statusBody.AuthorityRole, AuthorityRoleUnknown)
	}
	if statusBody.ReplicationRole != ReplicationRoleReady {
		t.Fatalf("replication role=%q want %q", statusBody.ReplicationRole, ReplicationRoleReady)
	}

	durableResp, err := http.Get("http://" + addr + "/status/durable?volume=v1")
	if err != nil {
		t.Fatalf("get durable: %v", err)
	}
	defer durableResp.Body.Close()
	if durableResp.StatusCode != 200 {
		t.Fatalf("durable: got %d want 200", durableResp.StatusCode)
	}
	var durableBody struct {
		VolumeID    string
		ReplicaID   string
		VolumeCount int
		Volumes     []durable.VolumeStatus
	}
	if err := json.NewDecoder(durableResp.Body).Decode(&durableBody); err != nil {
		t.Fatalf("decode durable: %v", err)
	}
	if durableBody.VolumeID != "v1" || durableBody.ReplicaID != "r1" || durableBody.VolumeCount != 1 {
		t.Fatalf("returned replica durable header reported wrong identity: %+v", durableBody)
	}
	if len(durableBody.Volumes) != 1 {
		t.Fatalf("durable volume count=%d want 1: %+v", len(durableBody.Volumes), durableBody.Volumes)
	}
	gotDurable := durableBody.Volumes[0]
	if gotDurable.VolumeID != "v1" || gotDurable.ReplicaID != "r1" || gotDurable.Epoch != 2 || gotDurable.EndpointVersion != 1 {
		t.Fatalf("returned replica durable status reported wrong lineage: %+v", gotDurable)
	}
	if !gotDurable.Latched || !gotDurable.Operational {
		t.Fatalf("returned replica must expose local durable readiness separately: %+v", durableBody.Volumes)
	}
}
