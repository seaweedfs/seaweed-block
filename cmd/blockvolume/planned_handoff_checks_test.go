package main_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// The checks below read the product's own output for each step of the
// planned hand-over (docs/volume-lifecycle.md). The binary test applies
// them to live output; TestPlannedHandoffChecksReadCapturedProductOutput
// applies them to output captured from the real binaries
// (testdata/planned-handoff), so they run in every default test run.

func checkShowBeforeStart(out string) error {
	return wantLines(out, "volume: v1 (query snapshot, not authority to serve)", "replica: none", "recorded ACK: none")
}

func checkShowGranted(out string, e uint64) error {
	return wantLines(out, "replica: r1", fmt.Sprintf("epoch: %d", e), "lease state: granted",
		fmt.Sprintf("holder: r1@%d (logical replica)", e), "recorded ACK: none")
}

// adoptedEpoch reads E from blockvolume's adoption line.
func adoptedEpoch(line string) (uint64, error) { return adoptedEpochOf("v1", line) }

// adoptedEpochOf reads E from blockvolume's adoption line for volume vol.
func adoptedEpochOf(vol, line string) (uint64, error) {
	e, err := matchUint(regexp.MustCompile(`blockvolume: volume `+regexp.QuoteMeta(vol)+` adopted as PRIMARY at r1@(\d+) `), line)
	if err == nil && e == 0 {
		err = fmt.Errorf("adopted at epoch 0: %q", line)
	}
	return e, err
}

// sealedGen reads G from a plain detach: one JSON object on stdout and
// exactly the sealed-generation line on stderr.
func sealedGen(stdout, stderr string, e uint64) (uint64, error) {
	return sealedGenOf(stdout, stderr, "v1", e)
}

// sealedGenOf reads G from a plain detach of volume vol at epoch e.
func sealedGenOf(stdout, stderr, vol string, e uint64) (uint64, error) {
	var a struct {
		VolumeID       string `json:"volumeID"`
		HeadGen        uint64 `json:"headGen"`
		HeadEpoch      uint64 `json:"headEpoch"`
		CutLSN         uint64 `json:"cutLSN"`
		LastWrittenLSN uint64 `json:"lastWrittenLSN"`
	}
	if err := json.Unmarshal([]byte(stdout), &a); err != nil {
		return 0, fmt.Errorf("plain detach stdout is not the JSON answer: %v: %q", err, stdout)
	}
	if a.VolumeID != vol || a.HeadGen == 0 || a.HeadEpoch != e || a.CutLSN == 0 || a.LastWrittenLSN == 0 {
		return 0, fmt.Errorf("plain detach answer %+v; want %s, a generation, epoch %d and both LSNs after a write", a, vol, e)
	}
	if strings.TrimSpace(stderr) != fmt.Sprintf("sealed HEAD generation: %d", a.HeadGen) {
		return 0, fmt.Errorf("plain detach stderr %q; want exactly the sealed generation line", stderr)
	}
	return a.HeadGen, nil
}

type detachedBy struct {
	Holder string `json:"holder"`
	Epoch  uint64 `json:"epoch"`
	Gen    uint64 `json:"gen"`
}

type headRecord struct {
	Gen        uint64      `json:"gen"`
	Epoch      uint64      `json:"epoch"`
	Holder     string      `json:"holder"`
	State      string      `json:"state"`
	RemoteLSN  uint64      `json:"remoteLSN"`
	DetachedBy *detachedBy `json:"detachedBy"`
}

func parseHead(out string) (headRecord, error) {
	var shown struct {
		Head *headRecord `json:"head"`
	}
	if err := json.Unmarshal([]byte(out), &shown); err != nil || shown.Head == nil {
		return headRecord{}, fmt.Errorf("chunk show stdout has no head: %v: %q", err, out)
	}
	return *shown.Head, nil
}

// sealedHeadLSN checks HEAD after the seal and returns its remote LSN (L).
func sealedHeadLSN(out string, e, g uint64) (uint64, error) {
	h, err := parseHead(out)
	if err != nil {
		return 0, err
	}
	if h.Gen != g || h.Epoch != e || h.Holder != "s1/r1" || h.State != "detached" ||
		h.DetachedBy == nil || *h.DetachedBy != (detachedBy{Holder: "s1/r1", Epoch: e, Gen: g}) {
		return 0, fmt.Errorf("HEAD after the seal %+v; want gen %d epoch %d holder s1/r1 detached by itself", h, g, e)
	}
	return h.RemoteLSN, nil
}

// The layout HEAD exposes the committed position and the master-minted
// holder identity. The old storage HEAD's DetachedBy proof is not a field of
// this format; a detached layout HEAD itself is the committed stop image.
type layoutHeadRecord struct {
	Gen      uint64          `json:"gen"`
	Epoch    uint64          `json:"epoch"`
	Holder   string          `json:"holder"`
	ClaimID  string          `json:"claimID"`
	Position uint64          `json:"position"`
	State    string          `json:"state"`
	Current  json.RawMessage `json:"current"`
}

func parseLayoutHead(out string) (layoutHeadRecord, error) {
	var shown struct {
		Head *layoutHeadRecord `json:"head"`
	}
	if err := json.Unmarshal([]byte(out), &shown); err != nil || shown.Head == nil {
		return layoutHeadRecord{}, fmt.Errorf("layout show stdout has no head: %v: %q", err, out)
	}
	return *shown.Head, nil
}

func sealedLayoutHeadPosition(out string, epoch, gen uint64) (uint64, error) {
	h, err := parseLayoutHead(out)
	if err != nil {
		return 0, err
	}
	if h.Gen != gen || h.Epoch != epoch || h.Holder != "s1/r1" || h.ClaimID == "" || h.State != "detached" || len(h.Current) == 0 || string(h.Current) == "null" {
		return 0, fmt.Errorf("layout HEAD after seal %+v; want committed detached image at gen %d epoch %d holder s1/r1", h, gen, epoch)
	}
	return h.Position, nil
}

func checkLayoutHeadReopened(out string, epoch, sealedGen, position uint64) error {
	return checkLayoutHeadReopenedBy(out, epoch, sealedGen, position, "s1")
}

func checkLayoutHeadReopenedBy(out string, epoch, sealedGen, position uint64, host string) error {
	h, err := parseLayoutHead(out)
	if err != nil {
		return err
	}
	if h.Gen <= sealedGen || h.Epoch != epoch || h.Holder != host+"/r1" || h.ClaimID == "" || h.State != "" || h.Position != position || len(h.Current) == 0 || string(h.Current) == "null" {
		return fmt.Errorf("layout HEAD after claim %+v; want live image above gen %d at epoch %d position %d holder %s/r1", h, sealedGen, epoch, position, host)
	}
	return nil
}

func checkRevoke(out string, e, g uint64) error {
	if !strings.HasPrefix(out, fmt.Sprintf("revocation requested: volume v1 holder r1@%d expiry_ns ", e)) {
		return fmt.Errorf("revoke stdout %q", out)
	}
	return wantLines(out, fmt.Sprintf("operator-stated sealed HEAD generation: %d (not verified); workspace show must report ACK at this generation before move --clean", g))
}

func showsAck(out string, e, g uint64) bool {
	return hasLine(out, fmt.Sprintf("recorded ACK: r1@%d HEAD generation %d", e, g))
}

// movedEpoch reads E2 from the clean move's answer to s1.
func movedEpoch(out string, e, g, l uint64) (uint64, error) { return movedEpochTo(out, e, g, l, "s1") }

// movedEpochTo reads E2 from the clean move's answer to the target host.
func movedEpochTo(out string, e, g, l uint64, target string) (uint64, error) {
	issued := regexp.MustCompile(fmt.Sprintf(`^clean move issued: volume v1 request move-v1-1: r1@%d -> r1@(\d+) on %s \(sealed HEAD gen %d, remote LSN %d\)$`, e, regexp.QuoteMeta(target), g, l))
	first, _, _ := strings.Cut(out, "\n")
	e2, err := matchUint(issued, strings.TrimRight(first, "\r"))
	if err != nil {
		return 0, err
	}
	if e2 <= e {
		return 0, fmt.Errorf("clean move issued epoch %d; want above %d", e2, e)
	}
	return e2, wantLines(out, fmt.Sprintf("next: start volume v1 on %s (this command moved authority only)", target))
}

func checkClaimDone(line string, e2 uint64) error { return checkClaimDoneBy(line, e2, "s1") }

func checkClaimDoneBy(line string, e2 uint64, host string) error {
	if !strings.Contains(line, "durable: HEAD claim for volume=v1 done in") || !strings.Contains(line, fmt.Sprintf(`epoch=%d holder="%s/r1"`, e2, host)) {
		return fmt.Errorf("HEAD claim %q; want done at epoch %d holder %s/r1", line, e2, host)
	}
	return nil
}

func checkShowMoved(out string, e, e2, g, l uint64) error {
	return wantLines(out, fmt.Sprintf("epoch: %d", e2), "lease state: granted",
		fmt.Sprintf("holder: r1@%d (logical replica)", e2), "recorded ACK: none",
		fmt.Sprintf("move: request=move-v1-1 kind=clean issued=true s1/r1@%d -> s1/r1@%d HEAD generation %d remote LSN %d", e, e2, g, l))
}

func checkHeadReopened(out string, e2, g, l uint64) error {
	return checkHeadReopenedBy(out, e2, g, l, "s1")
}

func checkHeadReopenedBy(out string, e2, g, l uint64, host string) error {
	h, err := parseHead(out)
	if err != nil {
		return err
	}
	if h.State != "active" || h.Epoch != e2 || h.Holder != host+"/r1" || h.Gen <= g || h.RemoteLSN != l || h.DetachedBy != nil {
		return fmt.Errorf("HEAD after the reopen %+v; want active, epoch %d, holder %s/r1, gen above %d, remote LSN %d, not detached", h, e2, host, g, l)
	}
	return nil
}

// forcedEpoch reads E2 from a forced move's answer (`forced move issued:
// ... r1@E on FROM -> r1@E2 on TO (kept HEAD gen N, remote LSN L)`).
func forcedEpoch(out string, e uint64, from, to string) (uint64, error) {
	return forcedEpochOf(out, "v1", e, from, to)
}

// forcedEpochOf reads E2 from a forced move of volume vol from host from to
// host to.
func forcedEpochOf(out, vol string, e uint64, from, to string) (uint64, error) {
	re := regexp.MustCompile(fmt.Sprintf(`(?m)^forced move issued: volume %s request \S+: r1@%d on %s -> r1@(\d+) on %s \(kept HEAD gen \d+, remote LSN \d+\)$`, regexp.QuoteMeta(vol), e, regexp.QuoteMeta(from), regexp.QuoteMeta(to)))
	e2, err := matchUint(re, out)
	if err == nil && e2 <= e {
		err = fmt.Errorf("forced move issued epoch %d; want above %d", e2, e)
	}
	return e2, err
}

// forcedKeptLSN reads the remote LSN a forced move kept.
func forcedKeptLSN(out string) (uint64, error) {
	return matchUint(regexp.MustCompile(`(?m)^forced move issued: .*\(kept HEAD gen \d+, remote LSN (\d+)\)$`), out)
}

// previewAdmitted reports whether a force-move --dry-run says admitted.
func previewAdmitted(out string) bool {
	return regexp.MustCompile(`(?m)^forced move admitted(: .*)?$`).MatchString(out)
}

// frontendReady reports whether a /status?volume=v1 body says the frontend
// is ready to serve as primary.
func frontendReady(body []byte) bool {
	var st struct{ FrontendPrimaryReady bool }
	return json.Unmarshal(body, &st) == nil && st.FrontendPrimaryReady
}

// checkCleanMoveRefused checks a clean move refused for a missing ACK: exit
// 1 (a failed precondition), nothing on stdout, the documented error.
func checkCleanMoveRefused(stdout, stderr string, code int) error {
	if code != 1 || stdout != "" || !strings.Contains(stderr, "workspace clean move needs detached HEAD and matching revocation ACK") {
		return fmt.Errorf("clean move without the ACK: exit %d, stdout %q, stderr %q; want exit 1 and the documented refusal", code, stdout, stderr)
	}
	return nil
}

// statusPrimary reports whether a /status?volume=v1 body shows a healthy
// primary at epoch.
func statusPrimary(body []byte, epoch uint64) bool {
	var st struct {
		Epoch         uint64
		Healthy       bool
		AuthorityRole string
	}
	return json.Unmarshal(body, &st) == nil && st.Epoch == epoch && st.Healthy && st.AuthorityRole == "primary"
}

func hasLine(out, want string) bool {
	for _, l := range strings.Split(out, "\n") {
		if strings.TrimRight(l, "\r") == want {
			return true
		}
	}
	return false
}

func wantLines(out string, want ...string) error {
	for _, w := range want {
		if !hasLine(out, w) {
			return fmt.Errorf("no line %q in:\n%s", w, out)
		}
	}
	return nil
}

func matchUint(re *regexp.Regexp, s string) (uint64, error) {
	m := re.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("%q does not match %s", s, re)
	}
	return strconv.ParseUint(m[1], 10, 64)
}

// TestPlannedHandoffChecksReadCapturedProductOutput runs every check on the
// output the real binaries printed in one passing hand-over (E=1, G=3, L=1,
// E2=2), in the refused clean move without an ACK (move-refused,
// show-revoked), and in the procedure gate's move to s2 and forced move
// (move-clean-s2, head-reopened-s2, force-move*, lost-range), and each check
// on an output of another step, which it must refuse.
func TestPlannedHandoffChecksReadCapturedProductOutput(t *testing.T) {
	read := func(name string) string {
		b, err := os.ReadFile(filepath.Join("testdata", "planned-handoff", name))
		if err != nil {
			t.Fatal(err)
		}
		return strings.ReplaceAll(string(b), "\r\n", "\n")
	}
	logLine := func(s string) string {
		for _, l := range strings.Split(read("logs.txt"), "\n") {
			if strings.Contains(l, s) {
				return l
			}
		}
		t.Fatalf("logs.txt has no line with %q", s)
		return ""
	}
	ok := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Errorf("%s: %v", what, err)
		}
	}
	refused := func(what string, err error) {
		t.Helper()
		if err == nil {
			t.Errorf("%s: accepted the output of another step", what)
		}
	}
	const e, g, l, e2 = 1, 3, 1, 2

	ok("show before start", checkShowBeforeStart(read("show-before-start.stdout")))
	refused("show before start", checkShowBeforeStart(read("show-granted.stdout")))
	if got, err := adoptedEpoch(logLine("adopted as PRIMARY at r1@1 ")); err != nil || got != e {
		t.Errorf("adopted epoch %d, %v; want %d", got, err, e)
	}
	ok("show granted", checkShowGranted(read("show-granted.stdout"), e))
	refused("show granted", checkShowGranted(read("show-acked.stdout"), e))
	if got, err := sealedGen(read("detach-plain.stdout"), read("detach-plain.stderr"), e); err != nil || got != g {
		t.Errorf("sealed gen %d, %v; want %d", got, err, g)
	}
	_, err := sealedGen(read("detach-plain.stdout"), "", e)
	refused("sealed gen without its stderr line", err)
	if got, err := sealedHeadLSN(read("head-sealed.stdout"), e, g); err != nil || got != l {
		t.Errorf("sealed HEAD LSN %d, %v; want %d", got, err, l)
	}
	_, err = sealedHeadLSN(read("head-reopened.stdout"), e, g)
	refused("sealed HEAD", err)
	ok("revoke", checkRevoke(read("revoke.stdout"), e, g))
	refused("revoke", checkRevoke(read("move-clean.stdout"), e, g))
	if !showsAck(read("show-acked.stdout"), e, g) || showsAck(read("show-granted.stdout"), e, g) {
		t.Error("the ACK line is not told apart from a show without it")
	}
	if got, err := movedEpoch(read("move-clean.stdout"), e, g, l); err != nil || got != e2 {
		t.Errorf("moved epoch %d, %v; want %d", got, err, e2)
	}
	_, err = movedEpoch(read("move-clean.stdout"), e, g+1, l)
	refused("clean move at another generation", err)
	ok("claim done", checkClaimDone(logLine(`gen=4 epoch=2 holder="s1/r1"`), e2))
	refused("claim done", checkClaimDone(logLine(`gen=2 epoch=1 holder="s1/r1"`), e2))
	ok("show moved", checkShowMoved(read("show-moved.stdout"), e, e2, g, l))
	refused("show moved", checkShowMoved(read("show-granted.stdout"), e, e2, g, l))
	ok("HEAD reopened", checkHeadReopened(read("head-reopened.stdout"), e2, g, l))
	refused("HEAD reopened", checkHeadReopened(read("head-sealed.stdout"), e2, g, l))
	ok("clean move refused", checkCleanMoveRefused("", read("move-refused.stderr"), 1))
	refused("clean move refused", checkCleanMoveRefused("", read("move-refused.stderr"), 0))
	refused("clean move refused", checkCleanMoveRefused(read("move-clean.stdout"), "", 0))
	if r := read("show-revoked.stdout"); showsAck(r, e, g) || !hasLine(r, "recorded ACK: none") {
		t.Error("the revoked show without an ACK is read as acknowledged")
	}
	// The procedure gate's two-host outputs (a planned move to s2, a forced
	// move s1 -> s2 at E=1 -> E2=2 keeping gen 3, LSN 1).
	if got, err := movedEpochTo(read("move-clean-s2.stdout"), e, g, l, "s2"); err != nil || got != e2 {
		t.Errorf("moved epoch to s2 %d, %v; want %d", got, err, e2)
	}
	_, err = movedEpochTo(read("move-clean-s2.stdout"), e, g, l, "s1")
	refused("clean move to s2 read as a move to s1", err)
	ok("HEAD reopened by s2", checkHeadReopenedBy(read("head-reopened-s2.stdout"), e2, g, l, "s2"))
	refused("HEAD reopened by s2 read as s1's", checkHeadReopenedBy(read("head-reopened-s2.stdout"), e2, g, l, "s1"))
	if got, err := forcedEpoch(read("force-move.stdout"), e, "s1", "s2"); err != nil || got != e2 {
		t.Errorf("forced epoch %d, %v; want %d", got, err, e2)
	}
	_, err = forcedEpoch(read("force-move.stdout"), e, "s2", "s1")
	refused("forced move read the other way round", err)
	_, err = forcedEpoch(read("move-clean-s2.stdout"), e, "s1", "s2")
	refused("forced epoch", err)
	if got, err := forcedKeptLSN(read("force-move.stdout")); err != nil || got != l {
		t.Errorf("forced kept LSN %d, %v; want %d", got, err, l)
	}
	if !previewAdmitted(read("force-move-dry-run-admitted.stdout")) || previewAdmitted(read("force-move-dry-run-refused.stdout")) {
		t.Error("the dry-run preview is not told admitted from not admitted")
	}
	if !strings.Contains(read("force-move-dry-run-refused.stdout"), "admissible at ") {
		t.Error("the refused preview has no admissible time")
	}
	if !strings.Contains(read("lost-range.stdout"), "range 1 of 1: request force-v1-1, r1@") {
		t.Error("lost-range does not show the forced move")
	}
	if !frontendReady([]byte(read("status-primary.json"))) || frontendReady([]byte(`{"FrontendPrimaryReady":false,"Healthy":true}`)) {
		t.Error("the frontend-ready check does not read FrontendPrimaryReady")
	}
	if !statusPrimary([]byte(read("status-primary.json")), e2) || statusPrimary([]byte(read("status-primary.json")), e) {
		t.Error("status check does not read the epoch of a healthy primary")
	}
}

// lastCutUploadedChunks reads Chunk.Stats.LastCut.UploadedChunks of vol from a
// /status/durable body.
func lastCutUploadedChunks(body []byte, vol string) (int, error) {
	var st struct {
		Volumes []struct {
			VolumeID string
			Chunk    *struct {
				Stats struct {
					LastCut *struct{ UploadedChunks int }
				}
			}
		}
	}
	if err := json.Unmarshal(body, &st); err != nil {
		return 0, fmt.Errorf("durable status is not JSON: %v", err)
	}
	for _, v := range st.Volumes {
		if v.VolumeID != vol {
			continue
		}
		if v.Chunk == nil || v.Chunk.Stats.LastCut == nil {
			return 0, fmt.Errorf("durable status of %s has no last cut", vol)
		}
		return v.Chunk.Stats.LastCut.UploadedChunks, nil
	}
	return 0, fmt.Errorf("durable status has no volume %s", vol)
}
