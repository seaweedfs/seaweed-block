package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
	"github.com/seaweedfs/seaweed-block/core/rpc/wsclient"
)

const wsExitMovedTargetNotServing = 6

type plannedMove struct {
	master, volume, replica, sourceHost, sourceStatus, targetHost, targetStatus, requestID, claimID string
	epoch                                                                                           uint64
	timeout                                                                                         time.Duration
}

func runWorkspacePlannedMove(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sw-block ops workspace planned-move", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(stderr, "planned-move: RF1 workspace only. Quiesce the guest first. Start the target unit waiting for its grant; its binding must be visible at blockmaster before any change. Reconnect the guest only after COMPLETE. No local journal; rerun the SAME request id after interruption or UNKNOWN.")
		fs.PrintDefaults()
	}
	var p plannedMove
	fs.StringVar(&p.master, "master", "", "blockmaster gRPC address")
	fs.StringVar(&p.volume, "volume", "", "volume name (RF1 workspace only)")
	fs.StringVar(&p.replica, "replica", "", "expected source replica")
	fs.Uint64Var(&p.epoch, "epoch", 0, "expected source epoch")
	fs.StringVar(&p.sourceHost, "source-host", "", "expected source server id")
	fs.StringVar(&p.sourceStatus, "source-status", "", "source status/control URL")
	fs.StringVar(&p.targetHost, "target-host", "", "target server id, already started waiting for its grant")
	fs.StringVar(&p.targetStatus, "target-status", "", "target's own status URL")
	fs.StringVar(&p.requestID, "request-id", "", "stable move id; reuse it after interruption or UNKNOWN")
	fs.StringVar(&p.claimID, "claim-id", "", "expected source claim id, if known")
	fs.DurationVar(&p.timeout, "timeout", 5*time.Minute, "bound for this invocation; never proof that a step applied")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return wsExitOK
		}
		return wsExitRefusedLocally
	}
	if fs.NArg() != 0 || p.master == "" || p.volume == "" || p.replica == "" || p.epoch == 0 || p.sourceHost == "" || p.sourceStatus == "" || p.targetHost == "" || p.targetStatus == "" || p.requestID == "" || p.timeout <= 0 {
		fmt.Fprintln(stderr, "planned-move: require master, volume, replica, positive epoch/timeout, source-host/source-status, target-host/target-status and request-id; quiesce the guest first and reconnect it after COMPLETE")
		return wsExitRefusedLocally
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
	defer cancel()
	c, err := workspaceDial(p.master)
	if err != nil {
		return p.result(stdout, wsExitUnknown, err.Error())
	}
	defer c.Close()
	st, err := c.Show(ctx, p.volume)
	if err != nil {
		return p.result(stdout, wsExitUnknown, err.Error())
	}
	var target *control.WorkspaceBinding
	for _, b := range st.GetBindings() {
		if b.GetVolumeId() == p.volume && b.GetServerId() == p.targetHost && !b.GetRetired() {
			target = b
			break
		}
	}
	if target == nil {
		return p.result(stdout, wsExitRefused, "target host has no live volume binding; start its unit waiting for a grant first")
	}
	// Master-owned volume metadata establishes RF1; a workspace binding alone
	// is not evidence that a NET volume is supported by this driver.
	conn, err := link.DialMasterFromEnv(p.master)
	if err != nil {
		return p.result(stdout, wsExitUnknown, err.Error())
	}
	defer conn.Close()
	vol, err := control.NewLifecycleServiceClient(conn).GetVolume(ctx, &control.GetLifecycleVolumeRequest{VolumeId: p.volume})
	if err != nil {
		return p.result(stdout, wsExitUnknown, err.Error())
	}
	if vol.GetVolumeId() != p.volume || vol.GetReplicationFactor() != 1 {
		return p.result(stdout, wsExitRefused, "only RF1 workspace volumes are supported")
	}
	for _, g := range st.GetMoves() {
		if g.GetRequestId() == p.requestID {
			if !p.matchesGrant(g) {
				return p.result(stdout, wsExitRefused, "request id is bound to another move")
			}
			return p.waitTarget(ctx, g, stdout)
		}
	}
	if l := st.GetLease(); l.GetVolumeId() != p.volume || l.GetReplicaId() != p.replica || l.GetEpoch() != p.epoch {
		return p.result(stdout, wsExitRefused, "source holder changed")
	}
	var sealedGen uint64
	if st.GetRevocationAck() == nil && !st.GetLease().GetRevoking() {
		fmt.Fprintln(stdout, "waiting: source conditional seal")
		seal := struct {
			VolumeID       string `json:"volumeID"`
			HeadGen        uint64 `json:"headGen"`
			HeadEpoch      uint64 `json:"headEpoch"`
			Holder         string `json:"holder"`
			CutLSN         uint64 `json:"cutLSN"`
			LastWrittenLSN uint64 `json:"lastWrittenLSN"`
		}{}
		timeout := time.Until(deadlineOf(ctx)).Milliseconds()
		if timeout < 1 {
			return p.result(stdout, wsExitUnknown, "seal deadline reached")
		}
		body := map[string]any{"volumeID": p.volume, "plain": true, "timeoutMs": timeout, "expectedHolder": map[string]any{"replicaID": p.replica, "epoch": p.epoch, "claimID": p.claimID}}
		status, err := plannedHTTP(ctx, http.MethodPost, p.sourceStatus, "/chunk/detach", body, &seal)
		if err != nil {
			code := wsExitUnknown
			if status >= 400 && status < 500 && status != 408 && status != 429 {
				code = wsExitRefused
			}
			return p.result(stdout, code, "seal: "+err.Error())
		}
		if seal.VolumeID != p.volume || seal.HeadEpoch != p.epoch || seal.Holder != p.sourceHost+"/"+p.replica || seal.HeadGen == 0 || seal.CutLSN != seal.LastWrittenLSN {
			return p.result(stdout, wsExitRefused, "seal reply does not match the expected holder and durable frontier; no revoke sent")
		}
		sealedGen = seal.HeadGen
		fmt.Fprintln(stdout, "waiting: source revocation acknowledgement")
		_, err = c.Revoke(ctx, &control.WorkspaceIdentity{VolumeId: p.volume, ReplicaId: p.replica, Epoch: p.epoch})
		if err != nil {
			return p.result(stdout, plannedRPCExit(err), "revoke: "+err.Error())
		}
	}
	if st.GetRevocationAck() == nil {
		fmt.Fprintln(stdout, "waiting: recorded ACK for the sealed source")
	}
	for {
		ack := st.GetRevocationAck()
		if ack != nil {
			id := ack.GetIdentity()
			if id.GetVolumeId() != p.volume || id.GetReplicaId() != p.replica || id.GetEpoch() != p.epoch || id.GetServerId() != p.sourceHost || ack.GetHeadGen() == 0 || (sealedGen != 0 && ack.GetHeadGen() != sealedGen) {
				return p.result(stdout, wsExitRefused, "recorded ACK does not match the sealed source identity")
			}
			break
		}
		if err := plannedWait(ctx); err != nil {
			return p.result(stdout, wsExitUnknown, "recorded ACK not observed: "+err.Error())
		}
		st, err = c.Show(ctx, p.volume)
		if err != nil {
			return p.result(stdout, plannedRPCExit(err), "read ACK: "+err.Error())
		}
	}
	g, err := c.CleanMove(ctx, &control.WorkspaceMove{RequestId: p.requestID, VolumeId: p.volume, ExpectedReplicaId: p.replica, ExpectedEpoch: p.epoch, TargetReplicaId: p.replica, TargetServerId: p.targetHost, TargetDataAddr: target.GetDataAddr(), TargetCtrlAddr: target.GetCtrlAddr(), Kind: "clean"})
	if err != nil {
		return p.result(stdout, plannedRPCExit(err), "clean move: "+err.Error())
	}
	if !p.matchesGrant(g) {
		return p.result(stdout, wsExitUnknown, "move reply did not match the requested identity")
	}
	return p.waitTarget(ctx, g, stdout)
}

func (p plannedMove) result(out io.Writer, code int, detail string) int {
	label := map[int]string{wsExitOK: "COMPLETE", wsExitRefused: "REFUSED", wsExitUnknown: "UNKNOWN", wsExitMovedTargetNotServing: "MOVED-TARGET-NOT-SERVING"}[code]
	fmt.Fprintf(out, "%s volume=%s request=%s: %s\n", label, p.volume, p.requestID, detail)
	if code == wsExitUnknown || code == wsExitMovedTargetNotServing {
		fmt.Fprintln(out, "next: inspect the named wait and rerun with the SAME --request-id; keep the guest quiesced")
	}
	return code
}

func deadlineOf(ctx context.Context) time.Time { d, _ := ctx.Deadline(); return d }
func plannedRPCExit(err error) int {
	if wsclient.OutcomeOf(err) == wsclient.OutcomeRefused || wsclient.OutcomeOf(err) == wsclient.OutcomeNotAuthorized || wsclient.OutcomeOf(err) == wsclient.OutcomeRefusedLocally {
		return wsExitRefused
	}
	return wsExitUnknown
}
func (p plannedMove) matchesGrant(g *control.WorkspaceGrant) bool {
	return g.GetIssued() && g.GetKind() == "clean" && g.GetRequestId() == p.requestID && g.GetVolumeId() == p.volume && g.GetOldReplicaId() == p.replica && g.GetNewReplicaId() == p.replica && g.GetOldEpoch() == p.epoch && g.GetNewEpoch() > p.epoch && g.GetOldHost() == p.sourceHost && g.GetNewHost() == p.targetHost
}
func plannedWait(ctx context.Context) error {
	t := time.NewTimer(100 * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (p plannedMove) waitTarget(ctx context.Context, g *control.WorkspaceGrant, out io.Writer) int {
	fmt.Fprintf(out, "waiting: target %s serves %s@%d (move receipt recorded)\n", p.targetHost, p.replica, g.GetNewEpoch())
	detail := "target has not reported readiness; keep its unit started and waiting for the grant"
	for {
		var target struct {
			VolumeID             string
			ReplicaID            string
			Epoch                uint64
			FrontendPrimaryReady bool
		}
		_, err := plannedHTTP(ctx, http.MethodGet, p.targetStatus, "/status?volume="+url.QueryEscape(p.volume), nil, &target)
		if err == nil && target.VolumeID == p.volume && target.ReplicaID == p.replica && target.Epoch == g.GetNewEpoch() && target.FrontendPrimaryReady {
			return p.result(out, wsExitOK, fmt.Sprintf("target %s serves %s@%d; reconnect the guest", p.targetHost, p.replica, g.GetNewEpoch()))
		}
		if err != nil {
			detail = "target status: " + err.Error()
		}
		if err := plannedWait(ctx); err != nil {
			return p.result(out, wsExitMovedTargetNotServing, detail)
		}
	}
}

// The caller configures each host's status URL. It is an observation/control
// endpoint, not a master-issued host identity or a remote target-start API.
func plannedHTTP(ctx context.Context, method, addr, path string, body any, out any) (int, error) {
	base := addr
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	var data []byte
	var err error
	if body != nil {
		data, err = json.Marshal(body)
		if err != nil {
			return 0, err
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, bytes.NewReader(data))
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return res.StatusCode, err
	}
	if res.StatusCode != http.StatusOK {
		return res.StatusCode, fmt.Errorf("HTTP %d: %s", res.StatusCode, strings.TrimSpace(string(raw)))
	}
	if len(raw) == 1<<20 {
		return res.StatusCode, errors.New("response exceeds size bound")
	}
	if err = json.Unmarshal(raw, out); err != nil {
		return res.StatusCode, err
	}
	return res.StatusCode, nil
}
