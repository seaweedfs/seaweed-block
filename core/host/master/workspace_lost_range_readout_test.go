package master

import (
	"context"
	"io/fs"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/authority"
	"github.com/seaweedfs/seaweed-block/core/lifecycle"
	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// lostRangeFiles lists the lost range side files under dir.
func lostRangeFiles(t *testing.T, dir string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(p, ".lostrange.json") {
			out = append(out, p)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// startStoppableMaster starts a master and registers its close as cleanup
// immediately; the returned stop closes it at most once (an explicit stop
// before a restart, then the cleanup, never closes twice).
func startStoppableMaster(t *testing.T, cfg Config) (*Host, func()) {
	t.Helper()
	h, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h.Start()
	var once sync.Once
	stop := func() { once.Do(func() { closeTestMaster(t, h) }) }
	t.Cleanup(stop)
	return h, stop
}

// forcedMoveHost starts a master whose volume vol-f is held by r1 on node-a
// (epoch 1, lease renewed once) with a prepared target binding on node-b.
// The returned stop closes it once; it is also registered as cleanup as soon
// as the master starts, so a failing setup step never leaks it.
func forcedMoveHost(t *testing.T, cfg Config) (*Host, func(), control.WorkspaceServiceClient, *control.WorkspaceMove) {
	t.Helper()
	h, stop := startStoppableMaster(t, cfg)
	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := control.NewWorkspaceServiceClient(conn)
	for _, server := range []string{"node-a", "node-b"} {
		if _, err := h.Lifecycle().Nodes.RegisterNode(lifecycle.NodeRegistration{ServerID: server, DataAddr: "127.0.0.1:9200", CtrlAddr: "127.0.0.1:9100"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := control.NewLifecycleServiceClient(conn).CreateVolume(ctx, &control.CreateVolumeRequest{VolumeId: "vol-f", SizeBytes: 1 << 20, ReplicationFactor: 1}); err != nil {
		t.Fatal(err)
	}
	src := &control.WorkspaceBinding{VolumeId: "vol-f", ServerId: "node-a", DataAddr: "127.0.0.1:19200", CtrlAddr: "127.0.0.1:19100"}
	target := &control.WorkspaceBinding{VolumeId: "vol-f", ServerId: "node-b", DataAddr: "127.0.0.1:29200", CtrlAddr: "127.0.0.1:29100"}
	for _, b := range []*control.WorkspaceBinding{src, target} {
		if _, err := client.RegisterWorkspaceBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.PlaceWorkspace(ctx, &control.WorkspacePlacement{VolumeId: "vol-f", ServerId: "node-a", ReplicaId: "r1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := control.NewObservationServiceClient(conn).ReportHeartbeat(ctx, &control.HeartbeatReport{ServerId: "node-a", SentAt: timestamppb.Now(), Reachable: true, Eligible: true,
		Slots: []*control.HeartbeatSlot{{VolumeId: "vol-f", ReplicaId: "r1", DataAddr: src.DataAddr, CtrlAddr: src.CtrlAddr}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.RunLifecycleProductTick(); err != nil {
		t.Fatal(err)
	}
	line := waitAuthorityLine(t, h.Publisher(), "vol-f")
	if _, err := client.RenewWorkspaceLease(ctx, &control.WorkspaceIdentity{VolumeId: "vol-f", ReplicaId: "r1", Epoch: line.Epoch}); err != nil {
		t.Fatal(err)
	}
	return h, stop, client, &control.WorkspaceMove{RequestId: "force-1", VolumeId: "vol-f", ExpectedReplicaId: "r1", ExpectedEpoch: line.Epoch, TargetReplicaId: "r1",
		TargetServerId: "node-b", TargetDataAddr: target.DataAddr, TargetCtrlAddr: target.CtrlAddr, Kind: "forced", Force: true}
}

// INV-WS-LOST-RANGE-READOUT-001 over the real master: the preview writes
// nothing and says when the move becomes admissible; the issued forced
// grant carries the stored range (with the HEAD's filer time read with HEAD)
// and is logged; a replay returns the same grant; the lost range RPC returns
// the same stored range; after a restart with the range file damaged, a
// replay still returns the issued grant but without a range and with the
// reason, and the lost range RPC fails with DataLoss, never "no loss".
func TestWorkspaceForcedMoveLostRangeOverRPC(t *testing.T) {
	logs := &syncBuffer{}
	headTime := time.Date(2026, 11, 21, 1, 2, 3, 0, time.UTC)
	authDir := t.TempDir()
	cfg := Config{WorkspaceMaxClockSkew: 100 * time.Millisecond, Listen: "127.0.0.1:0", AuthorityStoreDir: authDir, LifecycleStoreDir: t.TempDir(), WorkspaceRPC: true,
		Logger: log.New(logs, "", 0),
		WorkspaceHeadReader: func(context.Context, string) (authority.WorkspaceHead, error) {
			return authority.WorkspaceHead{Gen: 9, Epoch: 1, Holder: "node-a/r1", RemoteLSN: 42, FilerMtimeNS: headTime.UnixNano()}, nil
		}}
	h, stop, client, move := forcedMoveHost(t, cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pv, err := client.PreviewWorkspaceMove(ctx, move)
	if err != nil {
		t.Fatal(err)
	}
	if pv.Admitted || pv.LostUntilNs <= pv.MasterTimeNs || pv.HeadRead || !strings.Contains(pv.Reason, "has not elapsed") {
		t.Fatalf("preview before the horizon: %+v", pv)
	}
	if _, err := client.MoveWorkspace(ctx, move); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("move before the horizon: %v; want FailedPrecondition", err)
	}
	select {
	case <-time.After(time.Until(time.Unix(0, pv.LostUntilNs)) + 50*time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	pv2, err := client.PreviewWorkspaceMove(ctx, move)
	if err != nil {
		t.Fatal(err)
	}
	if !pv2.Admitted || !pv2.HeadRead || pv2.HeadGen != 9 || pv2.HeadRemoteLsn != 42 || !pv2.HeadTimeKnown || pv2.HeadTimeNs != headTime.UnixNano() || pv2.LostUntilNs != pv.LostUntilNs {
		t.Fatalf("preview after the horizon: %+v", pv2)
	}
	if st := h.Publisher().WorkspaceState("vol-f"); len(st.Moves) != 0 {
		t.Fatalf("preview wrote a journal record: %+v", st.Moves)
	}
	if files := lostRangeFiles(t, authDir); len(files) != 0 {
		t.Fatalf("preview wrote a lost range: %v", files)
	}

	g, err := client.MoveWorkspace(ctx, move)
	if err != nil {
		t.Fatal(err)
	}
	want := &control.WorkspaceLostRange{RequestId: "force-1", VolumeId: "vol-f", NewEpoch: g.NewEpoch, OldReplicaId: "r1", OldEpoch: move.ExpectedEpoch,
		LostAfterLsn: 42, LostUntilNs: pv.LostUntilNs, HeadGen: 9, HeadTimeNs: headTime.UnixNano(), HeadTimeKnown: true}
	if !g.Issued || g.LostRangeError != "" || !proto.Equal(g.LostRange, want) {
		t.Fatalf("forced grant %+v; want lost range %+v", g, want)
	}
	for _, s := range []string{`forced move issued volume="vol-f" request="force-1"`, "old=node-a/r1@1", "new=node-b/r1@2", "replay=false", "lost_after_lsn=42", "head_gen=9", "head_filer_time=2026-11-21T01:02:03Z"} {
		if !strings.Contains(logs.String(), s) {
			t.Fatalf("master log lacks %q:\n%s", s, logs.String())
		}
	}
	g2, err := client.MoveWorkspace(ctx, move)
	if err != nil || !proto.Equal(g, g2) {
		t.Fatalf("replay %+v %v; want the stored grant %+v", g2, err, g)
	}
	if !strings.Contains(logs.String(), "replay=true") {
		t.Fatalf("replayed forced grant not logged:\n%s", logs.String())
	}
	for _, id := range []string{"", "force-1"} {
		lr, err := client.QueryWorkspaceLostRange(ctx, &control.WorkspaceLostRangeQuery{VolumeId: "vol-f", RequestId: id})
		if err != nil || len(lr.Ranges) != 1 || !proto.Equal(lr.Ranges[0], want) {
			t.Fatalf("lost range query %q: %+v %v", id, lr, err)
		}
	}
	if _, err := client.QueryWorkspaceLostRange(ctx, &control.WorkspaceLostRangeQuery{VolumeId: "vol-f", RequestId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("unknown request id: %v; want NotFound", err)
	}
	if st, err := client.QueryWorkspace(ctx, &control.WorkspaceVolume{VolumeId: "vol-f"}); err != nil || len(st.Moves) != 1 || !proto.Equal(st.Moves[0].LostRange, want) {
		t.Fatalf("QueryWorkspace moves %+v %v", st.GetMoves(), err)
	}
	if pv3, err := client.PreviewWorkspaceMove(ctx, move); err != nil || !pv3.Admitted || pv3.HeadRead || pv3.LostUntilNs != pv.LostUntilNs {
		t.Fatalf("preview of the issued request: %+v %v", pv3, err)
	}

	stop()
	files := lostRangeFiles(t, authDir)
	if len(files) != 1 {
		t.Fatalf("lost range files %v", files)
	}
	raw, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files[0], []byte(strings.Replace(string(raw), `"LostAfterLSN":42`, `"LostAfterLSN":41`, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	h2, _ := startStoppableMaster(t, cfg)
	conn, err := grpc.NewClient(h2.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c2 := control.NewWorkspaceServiceClient(conn)
	g3, err := c2.MoveWorkspace(ctx, move)
	if err != nil || !g3.Issued || g3.NewEpoch != g.NewEpoch || g3.LostRange != nil || !strings.Contains(g3.LostRangeError, "missing or unreadable") {
		t.Fatalf("replay with a damaged range: %+v %v; want the issued grant without a range, with the reason", g3, err)
	}
	if !strings.Contains(logs.String(), "lost_range=UNPROVEN") {
		t.Fatalf("unproven range not logged:\n%s", logs.String())
	}
	if _, err := c2.QueryWorkspaceLostRange(ctx, &control.WorkspaceLostRangeQuery{VolumeId: "vol-f"}); status.Code(err) != codes.DataLoss {
		t.Fatalf("lost range query with a damaged record: %v; want DataLoss", err)
	}
}

// The HEAD reader takes the HEAD entry's mtime from the filer's
// Last-Modified header of the same response; without a usable header the
// time is unknown (0), never invented.
func TestWorkspaceHEADReadsFilerMtime(t *testing.T) {
	mtime := time.Date(2026, 11, 21, 1, 2, 3, 0, time.UTC)
	for _, tc := range []struct {
		name, header string
		want         int64
	}{
		{"last-modified", mtime.Format(http.TimeFormat), mtime.UnixNano()},
		{"absent", "", 0},
		{"unparseable", "yesterday", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tc.header != "" {
					w.Header().Set("Last-Modified", tc.header)
				}
				_, _ = w.Write([]byte(`{"format":1,"gen":9,"epoch":3,"holder":"old","remote_lsn":42}`))
			}))
			defer s.Close()
			read, err := NewWorkspaceHEADReader(s.URL)
			if err != nil {
				t.Fatal(err)
			}
			h, err := read(context.Background(), "v")
			if err != nil || h.FilerMtimeNS != tc.want || h.Gen != 9 || h.RemoteLSN != 42 {
				t.Fatalf("HEAD %+v %v; want filer mtime %d", h, err, tc.want)
			}
		})
	}
}
