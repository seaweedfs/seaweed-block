// Ownership: sw unit tests for the login negotiator. T2 ckpt 9
// scope: parameter negotiation, digest=None, multi-round
// SecurityNeg → LoginOp → FullFeature, target resolution,
// missing-required-field rejections.
package iscsi_test

import (
	"crypto/md5"
	"encoding/hex"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/iscsi"
)

type fakeResolver struct{ names map[string]bool }

func (r *fakeResolver) HasTarget(name string) bool { return r.names[name] }

func mkLoginReq(csg, nsg uint8, transit bool, params *iscsi.Params) *iscsi.PDU {
	p := &iscsi.PDU{}
	p.SetOpcode(iscsi.OpLoginReq)
	p.SetLoginStages(csg, nsg)
	p.SetLoginTransit(transit)
	p.SetISID([6]byte{0x02, 0x3d, 0, 0, 0, 1})
	if params != nil {
		p.DataSegment = params.Encode()
	}
	return p
}

func chapResponse(id byte, secret string, challenge []byte) string {
	h := md5.New()
	h.Write([]byte{id})
	h.Write([]byte(secret))
	h.Write(challenge)
	return "0x" + hex.EncodeToString(h.Sum(nil))
}

// Single-round LoginOp → FullFeature, no SecurityNeg. Mirrors
// the V2 "skip security" path the in-process Go test client uses.
func TestLoginNegotiator_DirectLoginOpToFullFeature(t *testing.T) {
	cfg := iscsi.DefaultNegotiableConfig()
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:t1")
	params.Set("HeaderDigest", "None")
	params.Set("DataDigest", "None")
	params.Set("MaxRecvDataSegmentLength", "8192")
	req := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, params)

	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("status class=0x%02x detail=0x%02x want Success",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	if !neg.Done() {
		t.Fatalf("Done()=false after Transit→FullFeature; phase=%v", neg.Phase())
	}
	got := neg.Result()
	if got.InitiatorName != "iqn.example.host:1" || got.TargetName != "iqn.example:t1" {
		t.Fatalf("captured identity wrong: %+v", got)
	}
	if got.SessionType != iscsi.SessionTypeNormal {
		t.Fatalf("SessionType=%q want Normal", got.SessionType)
	}
	// Digest negotiation echoed back as None.
	respParams, err := iscsi.ParseParams(resp.DataSegment)
	if err != nil {
		t.Fatalf("ParseParams resp: %v", err)
	}
	for _, k := range []string{"HeaderDigest", "DataDigest"} {
		if v, ok := respParams.Get(k); !ok || v != "None" {
			t.Fatalf("%s=%q,%v want None", k, v, ok)
		}
	}
	if v, _ := respParams.Get("TargetPortalGroupTag"); v == "" {
		t.Fatal("TargetPortalGroupTag missing in response (RFC 7143 §13.9 violation)")
	}
}

// Two-round: SecurityNeg (no transit) → SecurityNeg→LoginOp transit →
// LoginOp→FullFeature transit. iscsiadm's default flow.
func TestLoginNegotiator_MultiRound_SecurityThenLoginOp(t *testing.T) {
	neg := iscsi.NewLoginNegotiator(iscsi.DefaultNegotiableConfig())
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	// Round 1: SecurityNeg → SecurityNeg (no transit), declare identity.
	r1Params := iscsi.NewParams()
	r1Params.Set("InitiatorName", "iqn.example.host:1")
	r1Params.Set("SessionType", "Normal")
	r1Params.Set("TargetName", "iqn.example:t1")
	r1Params.Set("AuthMethod", "None")
	r1 := mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageSecurityNeg, false, r1Params)
	resp1 := neg.HandleLoginPDU(r1, resolver)
	if resp1.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("round 1 status=0x%02x", resp1.LoginStatusClass())
	}
	if neg.Done() {
		t.Fatal("Done() too early after round 1")
	}

	// Round 2: SecurityNeg → LoginOp transit.
	r2 := mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageLoginOp, true, iscsi.NewParams())
	resp2 := neg.HandleLoginPDU(r2, resolver)
	if resp2.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("round 2 status=0x%02x", resp2.LoginStatusClass())
	}
	if neg.Phase() != iscsi.LoginPhaseOperational {
		t.Fatalf("phase=%v want Operational", neg.Phase())
	}

	// Round 3: LoginOp → FullFeature transit, with op params.
	r3Params := iscsi.NewParams()
	r3Params.Set("HeaderDigest", "None")
	r3Params.Set("DataDigest", "None")
	r3Params.Set("MaxRecvDataSegmentLength", "16384")
	r3 := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, r3Params)
	resp3 := neg.HandleLoginPDU(r3, resolver)
	if resp3.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("round 3 status=0x%02x", resp3.LoginStatusClass())
	}
	if !neg.Done() {
		t.Fatalf("Done()=false after FullFeature transit")
	}
}

func TestLoginNegotiator_CHAP_DirectLoginOpRejected(t *testing.T) {
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.CHAP = iscsi.CHAPConfig{Username: "user1", Secret: "secret1"}
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:t1")
	req := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, params)

	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusInitiatorErr ||
		resp.LoginStatusDetail() != iscsi.LoginDetailAuthFailed {
		t.Fatalf("status=0x%02x detail=0x%02x want InitiatorErr/AuthFailed",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

func TestLoginNegotiator_CHAP_DiscoverySessionDoesNotRequireAuth(t *testing.T) {
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.CHAP = iscsi.CHAPConfig{Username: "user1", Secret: "secret1", Challenge: []byte("1234567890abcdef")}
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", iscsi.SessionTypeDiscovery)
	params.Set("AuthMethod", "None")
	req := mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageFullFeature, true, params)

	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("status=0x%02x detail=0x%02x want Success",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	if !neg.Done() {
		t.Fatalf("Done()=false after discovery login")
	}
}

func TestLoginNegotiator_CHAP_RejectsAuthMethodNone(t *testing.T) {
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.CHAP = iscsi.CHAPConfig{Username: "user1", Secret: "secret1", Challenge: []byte("1234567890abcdef")}
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:t1")
	params.Set("AuthMethod", "None")
	req := mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageSecurityNeg, false, params)

	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusInitiatorErr ||
		resp.LoginStatusDetail() != iscsi.LoginDetailAuthFailed {
		t.Fatalf("status=0x%02x detail=0x%02x want InitiatorErr/AuthFailed",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

func TestLoginNegotiator_CHAP_ChallengeThenLoginOp(t *testing.T) {
	challenge := []byte("1234567890abcdef")
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.CHAP = iscsi.CHAPConfig{Username: "user1", Secret: "secret1", Challenge: challenge}
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	r1Params := iscsi.NewParams()
	r1Params.Set("InitiatorName", "iqn.example.host:1")
	r1Params.Set("SessionType", "Normal")
	r1Params.Set("TargetName", "iqn.example:t1")
	r1Params.Set("AuthMethod", "CHAP")
	r1 := mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageSecurityNeg, false, r1Params)
	resp1 := neg.HandleLoginPDU(r1, resolver)
	if resp1.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("round 1 status=0x%02x detail=0x%02x",
			resp1.LoginStatusClass(), resp1.LoginStatusDetail())
	}
	resp1Params, err := iscsi.ParseParams(resp1.DataSegment)
	if err != nil {
		t.Fatalf("ParseParams round 1: %v", err)
	}
	if v, _ := resp1Params.Get("AuthMethod"); v != "CHAP" {
		t.Fatalf("AuthMethod=%q want CHAP", v)
	}
	if v, _ := resp1Params.Get("CHAP_A"); v != "5" {
		t.Fatalf("CHAP_A=%q want 5", v)
	}
	if v, _ := resp1Params.Get("CHAP_I"); v != "1" {
		t.Fatalf("CHAP_I=%q want 1", v)
	}
	if v, _ := resp1Params.Get("CHAP_C"); v != "0x"+hex.EncodeToString(challenge) {
		t.Fatalf("CHAP_C=%q want deterministic challenge", v)
	}

	r2Params := iscsi.NewParams()
	r2Params.Set("CHAP_N", "user1")
	r2Params.Set("CHAP_R", chapResponse(1, "secret1", challenge))
	r2 := mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageLoginOp, true, r2Params)
	resp2 := neg.HandleLoginPDU(r2, resolver)
	if resp2.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("round 2 status=0x%02x detail=0x%02x",
			resp2.LoginStatusClass(), resp2.LoginStatusDetail())
	}
	resp2Params, err := iscsi.ParseParams(resp2.DataSegment)
	if err != nil {
		t.Fatalf("ParseParams round 2: %v", err)
	}
	if v, _ := resp2Params.Get("AuthMethod"); v != "CHAP" {
		t.Fatalf("round 2 AuthMethod=%q want CHAP", v)
	}
	if neg.Phase() != iscsi.LoginPhaseOperational {
		t.Fatalf("phase=%v want Operational", neg.Phase())
	}

	r3Params := iscsi.NewParams()
	r3Params.Set("HeaderDigest", "None")
	r3Params.Set("DataDigest", "None")
	r3 := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, r3Params)
	resp3 := neg.HandleLoginPDU(r3, resolver)
	if resp3.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("round 3 status=0x%02x detail=0x%02x",
			resp3.LoginStatusClass(), resp3.LoginStatusDetail())
	}
	if !neg.Done() {
		t.Fatalf("Done()=false after CHAP + LoginOp")
	}
}

func TestLoginNegotiator_CHAP_ChallengeIgnoresPrematureTransit(t *testing.T) {
	challenge := []byte("1234567890abcdef")
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.CHAP = iscsi.CHAPConfig{Username: "user1", Secret: "secret1", Challenge: challenge}
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:t1")
	params.Set("AuthMethod", "CHAP")
	req := mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageLoginOp, true, params)

	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("status=0x%02x detail=0x%02x want Success",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	if resp.LoginTransit() {
		t.Fatal("response should not transit before CHAP_N/CHAP_R is verified")
	}
	if resp.LoginNSG() != iscsi.StageSecurityNeg {
		t.Fatalf("NSG=%d want SecurityNeg while challenge is outstanding", resp.LoginNSG())
	}
	if neg.Phase() != iscsi.LoginPhaseSecurity {
		t.Fatalf("phase=%v want Security", neg.Phase())
	}
	respParams, err := iscsi.ParseParams(resp.DataSegment)
	if err != nil {
		t.Fatalf("ParseParams: %v", err)
	}
	if v, _ := respParams.Get("CHAP_C"); v != "0x"+hex.EncodeToString(challenge) {
		t.Fatalf("CHAP_C=%q want deterministic challenge", v)
	}
}

func TestLoginNegotiator_CHAP_MissingResponseRejected(t *testing.T) {
	challenge := []byte("1234567890abcdef")
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.CHAP = iscsi.CHAPConfig{Username: "user1", Secret: "secret1", Challenge: challenge}
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	r1Params := iscsi.NewParams()
	r1Params.Set("InitiatorName", "iqn.example.host:1")
	r1Params.Set("SessionType", "Normal")
	r1Params.Set("TargetName", "iqn.example:t1")
	r1Params.Set("AuthMethod", "CHAP")
	neg.HandleLoginPDU(mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageSecurityNeg, false, r1Params), resolver)

	r2Params := iscsi.NewParams()
	r2Params.Set("CHAP_N", "user1")
	resp := neg.HandleLoginPDU(mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageLoginOp, true, r2Params), resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusInitiatorErr ||
		resp.LoginStatusDetail() != iscsi.LoginDetailMissingParam {
		t.Fatalf("status=0x%02x detail=0x%02x want InitiatorErr/MissingParam",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

func TestLoginNegotiator_CHAP_BadResponseRejected(t *testing.T) {
	challenge := []byte("1234567890abcdef")
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.CHAP = iscsi.CHAPConfig{Username: "user1", Secret: "secret1", Challenge: challenge}
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	r1Params := iscsi.NewParams()
	r1Params.Set("InitiatorName", "iqn.example.host:1")
	r1Params.Set("SessionType", "Normal")
	r1Params.Set("TargetName", "iqn.example:t1")
	r1Params.Set("AuthMethod", "CHAP")
	neg.HandleLoginPDU(mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageSecurityNeg, false, r1Params), resolver)

	r2Params := iscsi.NewParams()
	r2Params.Set("CHAP_N", "user1")
	r2Params.Set("CHAP_R", chapResponse(1, "wrong-secret", challenge))
	resp := neg.HandleLoginPDU(mkLoginReq(iscsi.StageSecurityNeg, iscsi.StageLoginOp, true, r2Params), resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusInitiatorErr ||
		resp.LoginStatusDetail() != iscsi.LoginDetailAuthFailed {
		t.Fatalf("status=0x%02x detail=0x%02x want InitiatorErr/AuthFailed",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
}

// Discovery sessions skip the TargetName resolver check.
func TestLoginNegotiator_DiscoverySession_NoTargetCheck(t *testing.T) {
	neg := iscsi.NewLoginNegotiator(iscsi.DefaultNegotiableConfig())
	// Resolver is empty — Normal session would reject. Discovery
	// must NOT consult the resolver.
	resolver := &fakeResolver{names: map[string]bool{}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", iscsi.SessionTypeDiscovery)
	req := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, params)
	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusSuccess {
		t.Fatalf("Discovery login status=0x%02x detail=0x%02x",
			resp.LoginStatusClass(), resp.LoginStatusDetail())
	}
	if neg.Result().SessionType != iscsi.SessionTypeDiscovery {
		t.Fatalf("SessionType=%q", neg.Result().SessionType)
	}
}

// Missing InitiatorName → reject with MissingParam.
func TestLoginNegotiator_RejectMissingInitiatorName(t *testing.T) {
	neg := iscsi.NewLoginNegotiator(iscsi.DefaultNegotiableConfig())
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	params := iscsi.NewParams()
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:t1")
	req := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, params)
	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusInitiatorErr {
		t.Fatalf("status=0x%02x want InitiatorErr", resp.LoginStatusClass())
	}
	if resp.LoginStatusDetail() != iscsi.LoginDetailMissingParam {
		t.Fatalf("detail=0x%02x want MissingParam (0x07)", resp.LoginStatusDetail())
	}
}

// Normal session with unknown TargetName → reject with NotFound.
func TestLoginNegotiator_RejectUnknownTargetName(t *testing.T) {
	neg := iscsi.NewLoginNegotiator(iscsi.DefaultNegotiableConfig())
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:wrong")
	req := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, params)
	resp := neg.HandleLoginPDU(req, resolver)
	if resp.LoginStatusClass() != iscsi.LoginStatusInitiatorErr {
		t.Fatalf("status=0x%02x want InitiatorErr", resp.LoginStatusClass())
	}
	if resp.LoginStatusDetail() != iscsi.LoginDetailNotFound {
		t.Fatalf("detail=0x%02x want NotFound (0x03)", resp.LoginStatusDetail())
	}
}

// Operational params get negotiated values back. MaxBurst /
// FirstBurst use min(); InitialR2T uses OR.
func TestLoginNegotiator_OperationalNegotiation(t *testing.T) {
	cfg := iscsi.DefaultNegotiableConfig()
	cfg.MaxBurstLength = 65536
	cfg.FirstBurstLength = 32768
	cfg.InitialR2T = false // ours says No
	neg := iscsi.NewLoginNegotiator(cfg)
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}

	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:t1")
	params.Set("MaxBurstLength", "131072")  // bigger than ours
	params.Set("FirstBurstLength", "16384") // smaller than ours
	params.Set("InitialR2T", "Yes")         // initiator says Yes → OR semantics
	req := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, params)
	resp := neg.HandleLoginPDU(req, resolver)

	respParams, err := iscsi.ParseParams(resp.DataSegment)
	if err != nil {
		t.Fatalf("ParseParams: %v", err)
	}
	if v, _ := respParams.Get("MaxBurstLength"); v != "65536" {
		t.Fatalf("MaxBurstLength=%q want 65536 (min(ours=65536, theirs=131072))", v)
	}
	if v, _ := respParams.Get("FirstBurstLength"); v != "16384" {
		t.Fatalf("FirstBurstLength=%q want 16384 (min)", v)
	}
	if v, _ := respParams.Get("InitialR2T"); v != "Yes" {
		t.Fatalf("InitialR2T=%q want Yes (OR semantics: theirs=Yes)", v)
	}
}

// Unknown operational keys produce NotUnderstood per RFC.
func TestLoginNegotiator_UnknownKey_RespondsNotUnderstood(t *testing.T) {
	neg := iscsi.NewLoginNegotiator(iscsi.DefaultNegotiableConfig())
	resolver := &fakeResolver{names: map[string]bool{"iqn.example:t1": true}}
	params := iscsi.NewParams()
	params.Set("InitiatorName", "iqn.example.host:1")
	params.Set("SessionType", "Normal")
	params.Set("TargetName", "iqn.example:t1")
	params.Set("X-Vendor-Extension", "shenanigans")
	req := mkLoginReq(iscsi.StageLoginOp, iscsi.StageFullFeature, true, params)
	resp := neg.HandleLoginPDU(req, resolver)
	respParams, _ := iscsi.ParseParams(resp.DataSegment)
	if v, _ := respParams.Get("X-Vendor-Extension"); v != "NotUnderstood" {
		t.Fatalf("X-Vendor-Extension=%q want NotUnderstood", v)
	}
}
