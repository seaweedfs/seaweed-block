package authority

import (
	"bufio"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/engine"
)

// ============================================================
// Unit tests — authoring rules
// ============================================================

func TestPublisher_Bind_MintsEpochAndEndpointVersion(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	got, ok := pub.LastPublished("v1", "r1")
	if !ok {
		t.Fatal("expected LastPublished after Bind")
	}
	if got.Epoch != 1 {
		t.Fatalf("Bind Epoch: got %d want 1", got.Epoch)
	}
	if got.EndpointVersion != 1 {
		t.Fatalf("Bind EndpointVersion: got %d want 1", got.EndpointVersion)
	}
}

func TestPublisher_Bind_RejectsDoubleBind(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	ask := AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}
	if err := pub.apply(ask); err != nil {
		t.Fatalf("first Bind: %v", err)
	}
	if err := pub.apply(ask); !errors.Is(err, ErrBindAlreadyBound) {
		t.Fatalf("second Bind: want ErrBindAlreadyBound, got %v", err)
	}
}

func TestPublisher_RefreshEndpoint_BumpsOnlyEndpointVersion(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("RefreshEndpoint: %v", err)
	}
	got, _ := pub.LastPublished("v1", "r1")
	if got.Epoch != 1 {
		t.Fatalf("Refresh must keep Epoch=1, got %d", got.Epoch)
	}
	if got.EndpointVersion != 2 {
		t.Fatalf("Refresh must bump EndpointVersion to 2, got %d", got.EndpointVersion)
	}
	if got.DataAddr != "d2" || got.CtrlAddr != "c2" {
		t.Fatalf("Refresh must update addrs, got %q/%q", got.DataAddr, got.CtrlAddr)
	}
}

func TestPublisher_RefreshEndpoint_IdempotentOnSameAddrs(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	ask := AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}
	if err := pub.apply(ask); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	refresh := ask
	refresh.Intent = IntentRefreshEndpoint
	if err := pub.apply(refresh); err != nil {
		t.Fatalf("RefreshEndpoint same-addrs: %v", err)
	}
	got, _ := pub.LastPublished("v1", "r1")
	if got.Epoch != 1 || got.EndpointVersion != 1 {
		t.Fatalf("same-addrs Refresh must be no-op, got epoch=%d ev=%d",
			got.Epoch, got.EndpointVersion)
	}
}

func TestPublisher_RefreshEndpoint_RejectsUnboundKey(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentRefreshEndpoint,
	})
	if !errors.Is(err, ErrRefreshNotBound) {
		t.Fatalf("want ErrRefreshNotBound, got %v", err)
	}
}

func TestPublisher_Reassign_BumpsEpochAndResetsEndpointVersion(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d1", CtrlAddr: "c1",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentRefreshEndpoint,
	}); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// Prior state: epoch=1, ev=2.
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d2", CtrlAddr: "c2",
		Intent: IntentReassign,
	}); err != nil {
		t.Fatalf("Reassign: %v", err)
	}
	got, _ := pub.LastPublished("v1", "r1")
	if got.Epoch != 2 {
		t.Fatalf("Reassign Epoch: got %d want 2", got.Epoch)
	}
	if got.EndpointVersion != 1 {
		t.Fatalf("Reassign must reset EndpointVersion to 1, got %d", got.EndpointVersion)
	}
}

func TestPublisher_Reassign_RejectsUnboundKey(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentReassign,
	})
	if !errors.Is(err, ErrReassignNotBound) {
		t.Fatalf("want ErrReassignNotBound, got %v", err)
	}
}

func TestPublisher_Validate_RejectsMissingFields(t *testing.T) {
	cases := []struct {
		name string
		ask  AssignmentAsk
		want error
	}{
		{"no volume", AssignmentAsk{ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}, ErrMissingVolumeID},
		{"no replica", AssignmentAsk{VolumeID: "v", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind}, ErrMissingReplicaID},
		{"no data", AssignmentAsk{VolumeID: "v", ReplicaID: "r", CtrlAddr: "c", Intent: IntentBind}, ErrMissingDataAddr},
		{"no ctrl", AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", Intent: IntentBind}, ErrMissingCtrlAddr},
		{"no intent", AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c"}, ErrMissingIntent},
		{"bad intent", AssignmentAsk{VolumeID: "v", ReplicaID: "r", DataAddr: "d", CtrlAddr: "c", Intent: AskIntent(99)}, ErrUnknownIntent},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pub := NewPublisher(NewStaticDirective(nil))
			err := pub.apply(c.ask)
			if !errors.Is(err, c.want) {
				t.Fatalf("want %v, got %v", c.want, err)
			}
		})
	}
}

// ============================================================
// Fan-out keyed by (VolumeID, ReplicaID)
// ============================================================

func TestPublisher_FanOut_TwoSubscribersOnSameKey(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	chA := pub.Subscribe("v1", "r1")
	chB := pub.Subscribe("v1", "r1")

	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}

	got1 := receiveOrFail(t, chA, "subA")
	got2 := receiveOrFail(t, chB, "subB")
	if got1.Epoch != 1 || got2.Epoch != 1 {
		t.Fatalf("both subscribers must receive epoch=1, got %d / %d", got1.Epoch, got2.Epoch)
	}
}

func TestPublisher_FanOut_SubscriberOnOtherVolumeDoesNotReceive(t *testing.T) {
	// Same replicaID across different volumes must NOT cross-deliver.
	// This is the multi-volume correctness test the architect called
	// out when we were keying only by ReplicaID.
	pub := NewPublisher(NewStaticDirective(nil))
	chV1 := pub.Subscribe("vol1", "r1")
	chV2 := pub.Subscribe("vol2", "r1")

	if err := pub.apply(AssignmentAsk{
		VolumeID: "vol1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind vol1: %v", err)
	}

	got := receiveOrFail(t, chV1, "vol1 subscriber")
	if got.VolumeID != "vol1" {
		t.Fatalf("vol1 subscriber got %q", got.VolumeID)
	}

	select {
	case info := <-chV2:
		t.Fatalf("vol2 subscriber must not receive vol1 publication, got %+v", info)
	case <-time.After(20 * time.Millisecond):
		// expected: nothing
	}
}

func TestPublisher_LateSubscriber_ReceivesLastPublished(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	if err := pub.apply(AssignmentAsk{
		VolumeID: "v1", ReplicaID: "r1",
		DataAddr: "d", CtrlAddr: "c",
		Intent: IntentBind,
	}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	ch := pub.Subscribe("v1", "r1")
	got := receiveOrFail(t, ch, "late subscriber")
	if got.Epoch != 1 {
		t.Fatalf("late subscriber must receive last published, got epoch=%d", got.Epoch)
	}
}

func TestPublisher_Unsubscribe_ClosesChannel(t *testing.T) {
	pub := NewPublisher(NewStaticDirective(nil))
	ch := pub.Subscribe("v1", "r1")
	pub.Unsubscribe("v1", "r1")
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("expected channel closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel not closed after Unsubscribe")
	}
}

// ============================================================
// Run loop — integration with Directive
// ============================================================

func TestPublisher_Run_DrivesDirectiveUntilCtxCancel(t *testing.T) {
	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind},
	})
	pub := NewPublisher(dir)
	ch := pub.Subscribe("v1", "r1")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pub.Run(ctx) }()

	info := receiveOrFail(t, ch, "Run consumer")
	if info.Epoch != 1 {
		t.Fatalf("Run must drive Bind, got epoch=%d", info.Epoch)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run exit: want context.Canceled, got %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Run did not exit on ctx cancel")
	}
}

func TestPublisher_Run_LogsAndContinuesOnRejectedAsk(t *testing.T) {
	// Reassign on an unbound key must be rejected, then the next
	// ask must still be processed. This is the "continue on reject"
	// Run semantic.
	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentReassign}, // rejected
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind},    // accepted
	})
	pub := NewPublisher(dir)
	ch := pub.Subscribe("v1", "r1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)

	info := receiveOrFail(t, ch, "post-reject subscriber")
	if info.Epoch != 1 {
		t.Fatalf("expected accepted Bind after rejected Reassign, got epoch=%d", info.Epoch)
	}
}

// ============================================================
// Bridge — forwards to AssignmentConsumer
// ============================================================

type recordingConsumer struct {
	mu   sync.Mutex
	seen []adapter.AssignmentInfo
}

func (r *recordingConsumer) OnAssignment(info adapter.AssignmentInfo) adapter.ApplyLog {
	r.mu.Lock()
	r.seen = append(r.seen, info)
	r.mu.Unlock()
	return adapter.ApplyLog{}
}

func (r *recordingConsumer) snapshot() []adapter.AssignmentInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]adapter.AssignmentInfo, len(r.seen))
	copy(cp, r.seen)
	return cp
}

func TestBridge_ForwardsToConsumer(t *testing.T) {
	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "v1", ReplicaID: "r1", DataAddr: "d", CtrlAddr: "c", Intent: IntentBind},
	})
	pub := NewPublisher(dir)
	cons := &recordingConsumer{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	go Bridge(ctx, pub, cons, "v1", "r1")

	// Wait for Bridge → consumer delivery.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(cons.snapshot()) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	seen := cons.snapshot()
	if len(seen) != 1 {
		t.Fatalf("Bridge did not forward: got %d deliveries", len(seen))
	}
	if seen[0].Epoch != 1 || seen[0].EndpointVersion != 1 {
		t.Fatalf("Bridge forwarded wrong fact: %+v", seen[0])
	}
}

// ============================================================
// Closure target: end-to-end authority → adapter → engine → Healthy
// ============================================================

// closureExecutor is a test executor that satisfies
// adapter.CommandExecutor and drives the adapter to ModeHealthy on
// a caught-up replica. It responds to ProbeReplica synchronously
// with a caught-up result (R == H) and auto-fires fence callbacks.
//
// We intentionally build a test executor rather than reuse the
// adapter-internal mockExecutor — the closure test must live in the
// authority package and use only adapter's exported surface.
type closureExecutor struct {
	mu              sync.Mutex
	onStart         adapter.OnSessionStart
	onClose         adapter.OnSessionClose
	onFenceComplete adapter.OnFenceComplete
	nextSession     atomic.Uint64
}

func newClosureExecutor() *closureExecutor {
	ce := &closureExecutor{}
	ce.nextSession.Store(1000)
	return ce
}

func (e *closureExecutor) SetOnSessionStart(fn adapter.OnSessionStart)     { e.onStart = fn }
func (e *closureExecutor) SetOnSessionClose(fn adapter.OnSessionClose)     { e.onClose = fn }
func (e *closureExecutor) SetOnFenceComplete(fn adapter.OnFenceComplete)   { e.onFenceComplete = fn }

func (e *closureExecutor) Probe(replicaID, dataAddr, ctrlAddr string, epoch, endpointVersion uint64) adapter.ProbeResult {
	return adapter.ProbeResult{
		ReplicaID:       replicaID,
		Success:         true,
		EndpointVersion: endpointVersion,
		TransportEpoch:  epoch,
		// R >= H → caught-up; engine will emit FenceAtEpoch.
		ReplicaFlushedLSN: 100,
		PrimaryTailLSN:    10,
		PrimaryHeadLSN:    100,
	}
}

func (e *closureExecutor) StartCatchUp(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return nil
}
func (e *closureExecutor) StartRebuild(replicaID string, sessionID, epoch, endpointVersion, targetLSN uint64) error {
	return nil
}
func (e *closureExecutor) InvalidateSession(replicaID string, sessionID uint64, reason string) {}
func (e *closureExecutor) PublishHealthy(replicaID string)                                     {}
func (e *closureExecutor) PublishDegraded(replicaID string, reason string)                     {}

func (e *closureExecutor) Fence(replicaID string, sessionID, epoch, endpointVersion uint64) error {
	// Fire success inline — matches the ack-gated contract from P14 S1.
	cb := e.onFenceComplete
	if cb != nil {
		cb(adapter.FenceResult{
			ReplicaID:       replicaID,
			SessionID:       sessionID,
			Epoch:           epoch,
			EndpointVersion: endpointVersion,
			Success:         true,
		})
	}
	return nil
}

// TestClosureTarget_SparrowReachesHealthyViaAuthorityRoute is the
// S2 acceptance test: wire Publisher → Bridge → VolumeReplicaAdapter
// with a StaticDirective carrying one Bind, and observe the adapter
// reach Mode == ModeHealthy — without any harness.assign call or
// any test-side adapter.OnAssignment call.
func TestClosureTarget_SparrowReachesHealthyViaAuthorityRoute(t *testing.T) {
	exec := newClosureExecutor()
	a := adapter.NewVolumeReplicaAdapter(exec)

	dir := NewStaticDirective([]AssignmentAsk{
		{VolumeID: "vol1", ReplicaID: "r1",
			DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334",
			Intent: IntentBind},
	})
	pub := NewPublisher(dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go pub.Run(ctx)
	go Bridge(ctx, pub, a, "vol1", "r1")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a.Projection().Mode == engine.ModeHealthy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sparrow did not reach Healthy via authority route; final mode=%s", a.Projection().Mode)
}

// ============================================================
// Non-forgeability — structural test
// ============================================================

// nonForgeabilityAllowedPaths are production-compiled test
// infrastructure packages that are explicitly allowed to construct
// AssignmentInfo with a non-zero Epoch. They are not operator or
// admin paths; they exist to drive scenario tests from the
// calibration harness and the YAML conformance runner. Adding a
// package here is an explicit, reviewable act.
var nonForgeabilityAllowedPaths = []string{
	string(filepath.Separator) + "authority" + string(filepath.Separator),   // the minter
	string(filepath.Separator) + "calibration" + string(filepath.Separator), // scenario harness
	string(filepath.Separator) + "conformance" + string(filepath.Separator), // YAML test runner
	string(filepath.Separator) + "schema" + string(filepath.Separator),      // event wire conversion
}

// TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority walks
// every production .go file under core/ (excluding _test.go files
// and explicit allowlisted packages) and fails if anything
// constructs an adapter.AssignmentInfo with an Epoch field set to
// anything other than a literal 0. This enforces at build-tree
// scope that the publisher is the sole authoritative minter for
// real system paths.
//
// The allowlist exists because calibration/conformance/schema are
// test-infrastructure packages that happen to live in the
// production tree. They drive scenario tests and YAML replays;
// they are not operator or admin paths. Any new package added to
// the allowlist must carry the same rationale — "this is test
// infrastructure, not a system-owned path".
//
// The pattern is strict: it matches ANY `Epoch:` assignment inside
// an `AssignmentInfo{...}` literal (whether the RHS is a literal
// digit, a variable name, a function call, or anything else) — and
// only allows `Epoch: 0` as a written-out zero. This catches the
// bypass where someone would set `Epoch: userInput` via a CLI or
// admin surface.
func TestNonForgeability_NoAssignmentInfoMintingOutsideAuthority(t *testing.T) {
	coreDir, err := findCoreDir()
	if err != nil {
		t.Fatalf("find core dir: %v", err)
	}

	// Matches `AssignmentInfo{ ... Epoch: <expr>` where <expr> is the
	// first non-space, non-comma, non-closing-brace token.
	pattern := regexp.MustCompile(`AssignmentInfo\s*\{[\s\S]*?Epoch\s*:\s*([^\s,}]+)`)

	var bad []string
	err = filepath.WalkDir(coreDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		for _, allowed := range nonForgeabilityAllowedPaths {
			if strings.Contains(path, allowed) {
				return nil
			}
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		var sb strings.Builder
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1<<20), 1<<20)
		for scanner.Scan() {
			sb.WriteString(scanner.Text())
			sb.WriteString("\n")
		}
		if err := scanner.Err(); err != nil {
			return err
		}

		matches := pattern.FindAllStringSubmatch(sb.String(), -1)
		for _, m := range matches {
			if len(m) < 2 {
				continue
			}
			if m[1] == "0" {
				continue // explicit zero is fine
			}
			bad = append(bad, path+": Epoch="+m[1])
			break
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(bad) > 0 {
		t.Fatalf("non-forgeability: production code outside the authority/calibration/conformance/schema allowlist mints AssignmentInfo with a non-zero Epoch:\n  %s",
			strings.Join(bad, "\n  "))
	}
}

// ============================================================
// Helpers
// ============================================================

func receiveOrFail(t *testing.T, ch <-chan adapter.AssignmentInfo, who string) adapter.AssignmentInfo {
	t.Helper()
	select {
	case info, ok := <-ch:
		if !ok {
			t.Fatalf("%s: channel closed before delivery", who)
		}
		return info
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("%s: no delivery within 500ms", who)
		return adapter.AssignmentInfo{}
	}
}

// findCoreDir locates the repository's core/ directory by walking up
// from the current test file's working directory. Used by the
// non-forgeability test.
func findCoreDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	// When tests run, cwd is the package dir. Walk up until we find
	// a sibling "adapter" directory (proxy for "we're in core/").
	dir := cwd
	for i := 0; i < 6; i++ {
		if info, err := os.Stat(filepath.Join(dir, "adapter")); err == nil && info.IsDir() {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("could not locate core/ directory")
}
