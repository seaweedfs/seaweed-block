package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/wsclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeWorkspace is a workspace service with canned answers that records
// every call.
type fakeWorkspace struct {
	control.UnimplementedWorkspaceServiceServer
	mu       sync.Mutex
	calls    map[string]int
	moves    []*control.WorkspaceMove
	status   *control.WorkspaceStatus
	move     func(*control.WorkspaceMove) (*control.WorkspaceGrant, error)
	preview  *control.WorkspaceMovePreview
	ranges   []*control.WorkspaceLostRange
	rangeErr error
	lastLR   *control.WorkspaceLostRangeQuery
}

func (f *fakeWorkspace) count(m string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.calls == nil {
		f.calls = map[string]int{}
	}
	f.calls[m]++
}

func (f *fakeWorkspace) n(m string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[m]
}

func (f *fakeWorkspace) total() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		n += c
	}
	return n
}

func (f *fakeWorkspace) QueryWorkspace(context.Context, *control.WorkspaceVolume) (*control.WorkspaceStatus, error) {
	f.count("QueryWorkspace")
	return f.status, nil
}

func (f *fakeWorkspace) MoveWorkspace(_ context.Context, q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
	f.count("MoveWorkspace")
	f.mu.Lock()
	f.moves = append(f.moves, q)
	f.mu.Unlock()
	return f.move(q)
}

func (f *fakeWorkspace) PreviewWorkspaceMove(context.Context, *control.WorkspaceMove) (*control.WorkspaceMovePreview, error) {
	f.count("PreviewWorkspaceMove")
	if f.preview == nil {
		return nil, status.Error(codes.Unavailable, "no preview")
	}
	return f.preview, nil
}

func (f *fakeWorkspace) QueryWorkspaceLostRange(_ context.Context, q *control.WorkspaceLostRangeQuery) (*control.WorkspaceLostRanges, error) {
	f.count("QueryWorkspaceLostRange")
	f.mu.Lock()
	f.lastLR = q
	f.mu.Unlock()
	if f.rangeErr != nil {
		return nil, f.rangeErr
	}
	return &control.WorkspaceLostRanges{Ranges: f.ranges}, nil
}

// startFakeWorkspace serves f; dials counts the CLI's connections.
func startFakeWorkspace(t *testing.T, f *fakeWorkspace) (addr string, dials *int) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	control.RegisterWorkspaceServiceServer(srv, f)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	n := 0
	prev := workspaceDial
	workspaceDial = func(a string) (*wsclient.Client, error) { n++; return wsclient.Dial(a) }
	t.Cleanup(func() { workspaceDial = prev })
	return lis.Addr().String(), &n
}

var (
	wsHeadTime  = time.Date(2026, 11, 21, 1, 2, 3, 0, time.UTC)
	wsLostUntil = time.Date(2026, 11, 21, 1, 5, 0, 0, time.UTC)
)

func wsHolder() *control.WorkspaceStatus {
	return &control.WorkspaceStatus{Lease: &control.WorkspaceLeaseFact{VolumeId: "vol", ReplicaId: "r1", Epoch: 3}}
}

func wsGrant(q *control.WorkspaceMove) *control.WorkspaceGrant {
	return &control.WorkspaceGrant{RequestId: q.RequestId, VolumeId: q.VolumeId, OldReplicaId: q.ExpectedReplicaId, NewReplicaId: q.TargetReplicaId, Kind: "forced",
		OldEpoch: q.ExpectedEpoch, NewEpoch: q.ExpectedEpoch + 1, HeadGen: 9, RemoteLsn: 42, Issued: true, OldHost: "node-a", NewHost: q.TargetServerId,
		LostRange: &control.WorkspaceLostRange{RequestId: q.RequestId, VolumeId: q.VolumeId, NewEpoch: q.ExpectedEpoch + 1, OldReplicaId: q.ExpectedReplicaId, OldEpoch: q.ExpectedEpoch,
			LostAfterLsn: 42, LostUntilNs: wsLostUntil.UnixNano(), HeadGen: 9, HeadTimeNs: wsHeadTime.UnixNano(), HeadTimeKnown: true}}
}

const wsReadout = "volume vol restored to the last published image at 2026-11-21T01:02:03Z (publishing host clock as recorded in the filer, 1 s resolution) (gen 9, LSN 42): writes with LSN <= 42 are preserved; " +
	"writes acknowledged by r1@3 after that and before 2026-11-21T01:05:00Z, master clock, may be lost; the upper bound is a time, not an LSN."

func forceMoveArgs(addr string, extra ...string) []string {
	return append([]string{"ops", "workspace", "force-move", "--master", addr, "--volume", "vol", "--target-host", "node-b", "--target-data", "10.0.0.2:3260",
		"--target-ctrl", "10.0.0.2:9100", "--request-id", "req-1"}, extra...)
}

func runCLI(args []string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

// Row Parse -> Refused (INV-WS-FORCED-CONFIRM-001): a confirmation that is
// absent or not exactly <volume>@<expected epoch> exits 2 with no
// connection and no RPC.
func TestWorkspaceForceMove_Row1_ConfirmationRefusedLocally(t *testing.T) {
	f := &fakeWorkspace{status: wsHolder(), move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) { return wsGrant(q), nil }}
	addr, dials := startFakeWorkspace(t, f)
	for _, extra := range [][]string{nil, {"--confirm-forced", "other@3"}, {"--confirm-forced", "vol"}, {"--confirm-forced", "vol@x"}, {"--confirm-forced", "vol@0"},
		{"--confirm-forced", "vol@3", "--force"}, {"--dry-run"}} {
		code, out, errOut := runCLI(forceMoveArgs(addr, extra...))
		if code != wsExitRefusedLocally || out != "" {
			t.Fatalf("%v: exit %d stdout %q; want 2 and nothing", extra, code, out)
		}
		if len(extra) > 0 && extra[len(extra)-1] != "--force" && !strings.Contains(errOut, "refused locally") {
			t.Fatalf("%v: stderr %q", extra, errOut)
		}
	}
	if *dials != 0 || f.total() != 0 {
		t.Fatalf("refused locally after %d dials and %d RPCs", *dials, f.total())
	}
}

// Row Parse -> Preview: --dry-run calls only the preview, writes nothing,
// labels the answer advisory, says when a refused move becomes admissible,
// and exits 0 whatever the answer.
func TestWorkspaceForceMove_Row2_DryRunPreview(t *testing.T) {
	master := time.Date(2026, 11, 21, 1, 4, 0, 0, time.UTC)
	f := &fakeWorkspace{status: wsHolder(), move: func(*control.WorkspaceMove) (*control.WorkspaceGrant, error) { panic("dry run moved") },
		preview: &control.WorkspaceMovePreview{Reason: "workspace forced move lease expiry plus clock-skew margin has not elapsed", LostUntilNs: wsLostUntil.UnixNano(), MasterTimeNs: master.UnixNano()}}
	addr, _ := startFakeWorkspace(t, f)
	code, out, errOut := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3", "--dry-run"))
	if code != wsExitOK || f.n("MoveWorkspace") != 0 || f.n("PreviewWorkspaceMove") != 1 {
		t.Fatalf("exit %d, %v; stderr %s", code, f.calls, errOut)
	}
	for _, s := range []string{"preview (advisory): at master time 2026-11-21T01:04:00Z, may change; admission is re-checked at the move", "this is not a confirmation",
		"forced move not admitted: workspace forced move lease expiry", "admissible at 2026-11-21T01:05:00Z, master clock"} {
		if !strings.Contains(out, s) {
			t.Fatalf("stdout lacks %q:\n%s", s, out)
		}
	}
	f.preview = &control.WorkspaceMovePreview{Admitted: true, LostUntilNs: wsLostUntil.UnixNano(), MasterTimeNs: wsLostUntil.Add(time.Minute).UnixNano(),
		HeadRead: true, HeadGen: 9, HeadEpoch: 3, HeadHolder: "node-a/r1", HeadRemoteLsn: 42, HeadTimeNs: wsHeadTime.UnixNano(), HeadTimeKnown: true}
	code, out, _ = runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3", "--dry-run"))
	if code != wsExitOK || f.n("MoveWorkspace") != 0 || strings.Contains(out, "admissible at") ||
		!strings.Contains(out, "forced move admitted") || !strings.Contains(out, "HEAD now: gen 9, epoch 3, holder node-a/r1, remote LSN 42, published at 2026-11-21T01:02:03Z (publishing host clock as recorded in the filer, 1 s resolution)") {
		t.Fatalf("admitted preview: exit %d\n%s", code, out)
	}
}

// Rows Parse -> Sent and Sent -> Granted: the move is sent as kind forced,
// force true, at the confirmed epoch with the request id; the grant and the
// readout from its stored range are printed, with the next step.
func TestWorkspaceForceMove_Row3and4_SentAndGranted(t *testing.T) {
	f := &fakeWorkspace{status: wsHolder(), move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) { return wsGrant(q), nil }}
	addr, _ := startFakeWorkspace(t, f)
	code, out, errOut := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3"))
	if code != wsExitOK || errOut != "" {
		t.Fatalf("exit %d stderr %s", code, errOut)
	}
	q := f.moves[0]
	if len(f.moves) != 1 || q.Kind != "forced" || !q.Force || q.ExpectedEpoch != 3 || q.ExpectedReplicaId != "r1" || q.TargetReplicaId != "r1" || q.RequestId != "req-1" ||
		q.VolumeId != "vol" || q.TargetServerId != "node-b" || q.TargetDataAddr != "10.0.0.2:3260" || q.TargetCtrlAddr != "10.0.0.2:9100" {
		t.Fatalf("sent %+v", f.moves)
	}
	want := "forced move issued: volume vol request req-1: r1@3 on node-a -> r1@4 on node-b (kept HEAD gen 9, remote LSN 42)\n" + wsReadout + "\n" +
		"next: start volume vol on node-b (this command moved authority only)\n"
	if out != want {
		t.Fatalf("stdout\n got %q\nwant %q", out, want)
	}
}

// Row Sent -> Refused: a FailedPrecondition prints the master's reason and,
// before the horizon, when the move becomes admissible; exit 1.
func TestWorkspaceForceMove_Row5_MasterRefusal(t *testing.T) {
	f := &fakeWorkspace{status: wsHolder(), move: func(*control.WorkspaceMove) (*control.WorkspaceGrant, error) {
		return nil, status.Error(codes.FailedPrecondition, "workspace forced move lease expiry plus clock-skew margin has not elapsed")
	}, preview: &control.WorkspaceMovePreview{LostUntilNs: wsLostUntil.UnixNano(), MasterTimeNs: wsLostUntil.Add(-time.Minute).UnixNano()}}
	addr, _ := startFakeWorkspace(t, f)
	code, out, errOut := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3"))
	if code != wsExitRefused || out != "" || !strings.Contains(errOut, "refused by the master (nothing moved)") || !strings.Contains(errOut, "has not elapsed") ||
		!strings.Contains(errOut, "admissible at 2026-11-21T01:05:00Z, master clock") || !strings.Contains(errOut, "there is no bypass") {
		t.Fatalf("exit %d stdout %q stderr %s", code, out, errOut)
	}
	// Past the horizon (refused for another reason) no time is promised.
	f.preview = &control.WorkspaceMovePreview{Admitted: true, LostUntilNs: wsLostUntil.UnixNano(), MasterTimeNs: wsLostUntil.Add(time.Minute).UnixNano()}
	if code, _, errOut = runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3")); code != wsExitRefused || strings.Contains(errOut, "admissible at") {
		t.Fatalf("exit %d stderr %s", code, errOut)
	}
	// A confirmation at an epoch the master never had is refused before the move.
	moves := f.n("MoveWorkspace")
	if code, _, errOut = runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@7")); code != wsExitRefused || f.n("MoveWorkspace") != moves || !strings.Contains(errOut, "no holder of volume") {
		t.Fatalf("stale epoch: exit %d stderr %s", code, errOut)
	}
}

// Row Sent -> Unknown: a transport error or the deadline leaves the outcome
// unknown; exit 3 with "rerun with the SAME --request-id".
func TestWorkspaceForceMove_Row6_OutcomeUnknown(t *testing.T) {
	for name, move := range map[string]func(*control.WorkspaceMove) (*control.WorkspaceGrant, error){
		"unavailable": func(*control.WorkspaceMove) (*control.WorkspaceGrant, error) {
			return nil, status.Error(codes.Unavailable, "connection reset")
		},
		"deadline": func(*control.WorkspaceMove) (*control.WorkspaceGrant, error) {
			time.Sleep(2 * time.Second)
			return nil, status.Error(codes.Unavailable, "late")
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeWorkspace{status: wsHolder(), move: move}
			addr, _ := startFakeWorkspace(t, f)
			code, out, errOut := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3", "--timeout", "500ms"))
			if code != wsExitUnknown || out != "" || !strings.Contains(errOut, "outcome unknown: rerun with the SAME --request-id req-1") {
				t.Fatalf("exit %d stdout %q stderr %s", code, out, errOut)
			}
		})
	}
}

// Row Granted -> LostMissing: an issued forced grant without its stored
// range (or with another move's) exits 4: the move is issued, the readout is
// not proven, and "no loss" is never printed. lost-range with a missing or
// corrupt record (DataLoss) exits 4 too.
func TestWorkspaceForceMove_Row7_ReadoutUnproven(t *testing.T) {
	f := &fakeWorkspace{status: wsHolder(), move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
		g := wsGrant(q)
		g.LostRange, g.LostRangeError = nil, "workspace lost range missing or unreadable: forced move \"req-1\" has no loaded lost range record"
		return g, nil
	}, rangeErr: status.Error(codes.DataLoss, "workspace lost range missing or unreadable")}
	addr, _ := startFakeWorkspace(t, f)
	code, out, errOut := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3"))
	if code != wsExitReadoutUnproven || !strings.HasPrefix(out, "forced move issued: volume vol request req-1") || strings.Contains(out, "restored to") ||
		!strings.Contains(errOut, "lost range is not proven") || !strings.Contains(errOut, "the move IS issued; do not assume no loss") {
		t.Fatalf("exit %d stdout %q stderr %s", code, out, errOut)
	}
	code, out, errOut = runCLI([]string{"ops", "workspace", "lost-range", "--master", addr, "--volume", "vol"})
	if code != wsExitReadoutUnproven || out != "" || !strings.Contains(errOut, "NOT proven") {
		t.Fatalf("lost-range: exit %d stdout %q stderr %s", code, out, errOut)
	}
}

// splitShellWords splits a POSIX shell line of plain words and
// single-quoted parts and backslash escapes (the forms shellCommand prints).
func splitShellWords(t *testing.T, line string) []string {
	t.Helper()
	var words []string
	var cur strings.Builder
	in, quoted, inQuote, escaped := false, false, false, false
	for _, r := range line {
		switch {
		case escaped:
			cur.WriteRune(r)
			escaped, in = false, true
		case inQuote && r == '\'':
			inQuote = false
		case inQuote:
			cur.WriteRune(r)
		case r == '\'':
			inQuote, in, quoted = true, true, true
		case r == '\\':
			escaped = true
		case r == ' ':
			if in {
				words = append(words, cur.String())
				cur.Reset()
				in, quoted = false, false
			}
		default:
			cur.WriteRune(r)
			in = true
		}
	}
	if inQuote {
		t.Fatalf("unterminated quote in %q", line)
	}
	if in || quoted {
		words = append(words, cur.String())
	}
	return words
}

// The exit-4 recovery command carries the master address and survives
// arguments that need quoting: the printed words parse back to the same
// values and run as a lost-range command against the same master.
func TestWorkspaceForceMoveRecoveryCommandRuns(t *testing.T) {
	const reqID = "req 1'$x"
	f := &fakeWorkspace{status: wsHolder(), move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
		g := wsGrant(q)
		g.LostRange, g.LostRangeError = nil, "missing"
		return g, nil
	}, ranges: nil}
	addr, _ := startFakeWorkspace(t, f)
	args := forceMoveArgs(addr, "--confirm-forced", "vol@3")
	args[len(args)-3] = reqID // --request-id value
	code, _, errOut := runCLI(args)
	if code != wsExitReadoutUnproven {
		t.Fatalf("exit %d %s", code, errOut)
	}
	var line string
	for _, l := range strings.Split(errOut, "\n") {
		if strings.HasPrefix(l, workspaceRecoveryPrefix+"sw-block ") {
			line = strings.TrimPrefix(l, workspaceRecoveryPrefix)
		}
	}
	words := splitShellWords(t, line)
	want := []string{"sw-block", "ops", "workspace", "lost-range", "--master", addr, "--volume", "vol", "--request-id", reqID}
	if strings.Join(words, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("recovery command %q parses to %q; want %q", line, words, want)
	}
	f.ranges = []*control.WorkspaceLostRange{{RequestId: reqID, VolumeId: "vol", NewEpoch: 4, OldReplicaId: "r1", OldEpoch: 3, LostAfterLsn: 42, LostUntilNs: wsLostUntil.UnixNano(), HeadGen: 9}}
	if code, out, errOut := runCLI(words[1:]); code != wsExitOK || f.lastLR.RequestId != reqID || f.lastLR.VolumeId != "vol" || !strings.Contains(out, "range 1 of 1") {
		t.Fatalf("recovery command run: exit %d %+v stdout %q stderr %s", code, f.lastLR, out, errOut)
	}
}

// A successful answer that does not match the request (here the target)
// is an unknown outcome, and one whose range disagrees with the kept HEAD
// is unproven: neither prints the preservation readout.
func TestWorkspaceForceMoveRejectsAMismatchedAnswer(t *testing.T) {
	for name, tc := range map[string]struct {
		fault func(*control.WorkspaceGrant)
		exit  int
	}{
		"grant target":   {func(g *control.WorkspaceGrant) { g.NewHost = "node-c" }, wsExitUnknown},
		"grant epoch":    {func(g *control.WorkspaceGrant) { g.OldEpoch = 2 }, wsExitUnknown},
		"range volume":   {func(g *control.WorkspaceGrant) { g.LostRange.VolumeId = "vol2" }, wsExitReadoutUnproven},
		"range frontier": {func(g *control.WorkspaceGrant) { g.LostRange.LostAfterLsn = 41 }, wsExitReadoutUnproven},
		"range head gen": {func(g *control.WorkspaceGrant) { g.LostRange.HeadGen = 8 }, wsExitReadoutUnproven},
	} {
		t.Run(name, func(t *testing.T) {
			f := &fakeWorkspace{status: wsHolder(), move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
				g := wsGrant(q)
				tc.fault(g)
				return g, nil
			}}
			addr, _ := startFakeWorkspace(t, f)
			code, out, errOut := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3"))
			if code != tc.exit || strings.Contains(out, "restored to") || strings.Contains(out, "next: start") {
				t.Fatalf("exit %d (want %d) stdout %q stderr %s", code, tc.exit, out, errOut)
			}
			if tc.exit == wsExitUnknown && !strings.Contains(errOut, "rerun with the SAME --request-id req-1") {
				t.Fatalf("no same-id recovery: %s", errOut)
			}
		})
	}
}

// Row Rerun same id -> Granted: the master replays the stored grant, and
// the output is the same, from the stored range.
func TestWorkspaceForceMove_Row8_RerunSameRequestID(t *testing.T) {
	stored := map[string]*control.WorkspaceGrant{}
	f := &fakeWorkspace{status: wsHolder(), move: func(q *control.WorkspaceMove) (*control.WorkspaceGrant, error) {
		if g, ok := stored[q.RequestId]; ok {
			return g, nil
		}
		stored[q.RequestId] = wsGrant(q)
		return nil, status.Error(codes.Unavailable, "reply lost")
	}}
	addr, _ := startFakeWorkspace(t, f)
	if code, _, _ := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3")); code != wsExitUnknown {
		t.Fatalf("first run: exit %d; want unknown", code)
	}
	code, out, _ := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3"))
	code2, out2, _ := runCLI(forceMoveArgs(addr, "--confirm-forced", "vol@3"))
	if code != wsExitOK || code2 != wsExitOK || out != out2 || !strings.Contains(out, wsReadout) {
		t.Fatalf("reruns: exit %d/%d\n%s\n%s", code, code2, out, out2)
	}
	if len(f.moves) != 3 || f.moves[0].RequestId != f.moves[2].RequestId {
		t.Fatalf("moves %+v", f.moves)
	}
}

// lost-range lists every stored range in issue order (or the one of
// --request-id), each with its readout; a volume without forced moves says
// so.
func TestWorkspaceLostRangeListsAllInOrder(t *testing.T) {
	q := &control.WorkspaceMove{RequestId: "req-1", VolumeId: "vol", ExpectedReplicaId: "r1", TargetReplicaId: "r1", ExpectedEpoch: 3}
	first := wsGrant(q).LostRange
	second := &control.WorkspaceLostRange{RequestId: "req-2", VolumeId: "vol", NewEpoch: 5, OldReplicaId: "r1", OldEpoch: 4, LostAfterLsn: 42,
		LostUntilNs: wsLostUntil.Add(time.Minute).UnixNano(), HeadGen: 9, HeadTimeNs: wsHeadTime.UnixNano(), HeadTimeKnown: true}
	f := &fakeWorkspace{ranges: []*control.WorkspaceLostRange{first, second}}
	addr, _ := startFakeWorkspace(t, f)
	code, out, _ := runCLI([]string{"ops", "workspace", "lost-range", "--master", addr, "--volume", "vol"})
	i1, i2 := strings.Index(out, "range 1 of 2: request req-1, r1@3 -> epoch 4"), strings.Index(out, "range 2 of 2: request req-2, r1@4 -> epoch 5")
	if code != wsExitOK || i1 < 0 || i2 < i1 || !strings.Contains(out, "  "+wsReadout+"\n") || !strings.Contains(out, "acknowledged by r1@4 after that and before 2026-11-21T01:06:00Z") {
		t.Fatalf("exit %d\n%s", code, out)
	}
	if f.lastLR.RequestId != "" {
		t.Fatalf("listed with request id %q", f.lastLR.RequestId)
	}
	f.ranges = []*control.WorkspaceLostRange{second}
	if code, out, _ = runCLI([]string{"ops", "workspace", "lost-range", "--master", addr, "--volume", "vol", "--request-id", "req-2"}); code != wsExitOK || f.lastLR.RequestId != "req-2" || !strings.Contains(out, "range 1 of 1: request req-2") {
		t.Fatalf("by request id: exit %d %+v\n%s", code, f.lastLR, out)
	}
	f.ranges = nil
	if code, out, _ = runCLI([]string{"ops", "workspace", "lost-range", "--master", addr, "--volume", "vol"}); code != wsExitOK || out != "volume vol has no issued forced move\n" {
		t.Fatalf("none: exit %d %q", code, out)
	}
	f.rangeErr = status.Error(codes.NotFound, "no issued forced move with that request id")
	if code, _, _ = runCLI([]string{"ops", "workspace", "lost-range", "--master", addr, "--volume", "vol", "--request-id", "x"}); code != wsExitRefused {
		t.Fatalf("unknown request id: exit %d", code)
	}
}

// The help says authority only, the next step, and that no bypass exists.
func TestWorkspaceHelpText(t *testing.T) {
	code, out, _ := runCLI([]string{"ops", "workspace", "help"})
	out = strings.Join(strings.Fields(out), " ")
	for _, s := range []string{"AUTHORITY only", "starting the volume on the target host is the next step", "There is NO bypass", "never a confirmation", "<volume>@<expected epoch>"} {
		if code != wsExitOK || !strings.Contains(out, s) {
			t.Fatalf("help lacks %q:\n%s", s, out)
		}
	}
}
