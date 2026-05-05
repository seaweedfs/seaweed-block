package iscsi

// iSCSI login negotiator — T2 ckpt 9 mechanism port.
//
// Adapted from weed/storage/blockvol/iscsi/login.go per
// assignment §4.3 ckpt 9 allowlist:
//   - parameter negotiation (MaxRecv, MaxBurst, FirstBurst,
//     InitialR2T, ImmediateData, MaxConnections, etc.)
//   - digest negotiation to None
//   - SecurityNeg → LoginOp → FullFeature stage transitions
//
// Intentionally NOT ported from V2:
//   - target redirects (multi-target operator surface, later)
//
// Production code path remains "consumer of authority truth":
// no V2 storage / authority bindings appear in this file.

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Login status class/detail (RFC 7143 §11.13).
const (
	LoginStatusSuccess      uint8 = 0x00
	LoginStatusInitiatorErr uint8 = 0x02
	LoginStatusTargetErr    uint8 = 0x03

	LoginDetailSuccess        uint8 = 0x00
	LoginDetailAuthFailed     uint8 = 0x01
	LoginDetailInitiatorError uint8 = 0x00
	LoginDetailNotFound       uint8 = 0x03
	LoginDetailMissingParam   uint8 = 0x07
	LoginDetailTargetError    uint8 = 0x00
)

// Errors surfaced during login.
var (
	ErrLoginInvalidStage   = errors.New("iscsi: invalid login stage transition")
	ErrLoginInvalidRequest = errors.New("iscsi: invalid login request")
	ErrLoginInvalidCHAP    = errors.New("iscsi: invalid CHAP configuration")
)

// LoginPhase tracks the negotiator's stage.
type LoginPhase int

const (
	LoginPhaseStart LoginPhase = iota
	LoginPhaseSecurity
	LoginPhaseOperational
	LoginPhaseDone
)

// SessionType constants — mirror what iscsiadm sets in the
// SessionType key during login.
const (
	SessionTypeNormal    = "Normal"
	SessionTypeDiscovery = "Discovery"
)

// NegotiableConfig is the per-target negotiation profile. Sane
// defaults match common Linux initiator expectations.
type NegotiableConfig struct {
	MaxRecvDataSegmentLength int
	MaxBurstLength           int
	FirstBurstLength         int
	MaxConnections           int
	MaxOutstandingR2T        int
	DefaultTime2Wait         int
	DefaultTime2Retain       int
	InitialR2T               bool
	ImmediateData            bool
	DataPDUInOrder           bool
	DataSequenceInOrder      bool
	ErrorRecoveryLevel       int
	TargetPortalGroupTag     int
	TargetAlias              string
	CHAP                     CHAPConfig
}

// CHAPConfig enables target-side CHAP authentication when Secret is
// non-empty. Username is required when CHAP is enabled. Challenge is optional
// and exists to make tests deterministic; production uses random challenge
// bytes when it is empty.
type CHAPConfig struct {
	Username  string
	Secret    string
	Challenge []byte
}

func (c NegotiableConfig) isZeroExceptCHAP() bool {
	return c.MaxRecvDataSegmentLength == 0 &&
		c.MaxBurstLength == 0 &&
		c.FirstBurstLength == 0 &&
		c.MaxConnections == 0 &&
		c.MaxOutstandingR2T == 0 &&
		c.DefaultTime2Wait == 0 &&
		c.DefaultTime2Retain == 0 &&
		!c.InitialR2T &&
		!c.ImmediateData &&
		!c.DataPDUInOrder &&
		!c.DataSequenceInOrder &&
		c.ErrorRecoveryLevel == 0 &&
		c.TargetPortalGroupTag == 0 &&
		c.TargetAlias == ""
}

// DefaultNegotiableConfig returns a profile that interoperates
// with typical Linux open-iscsi initiators. Values match V2
// DefaultTargetConfig where the field exists.
func DefaultNegotiableConfig() NegotiableConfig {
	return NegotiableConfig{
		MaxRecvDataSegmentLength: 262144, // 256 KiB
		MaxBurstLength:           262144,
		FirstBurstLength:         65536,
		MaxConnections:           1,
		MaxOutstandingR2T:        1,
		DefaultTime2Wait:         2,
		DefaultTime2Retain:       0,
		InitialR2T:               true,
		ImmediateData:            true,
		DataPDUInOrder:           true,
		DataSequenceInOrder:      true,
		ErrorRecoveryLevel:       0,
		TargetPortalGroupTag:     1,
	}
}

// TargetResolver lets the negotiator validate target names
// against the set this server hosts. Discovery sessions skip
// the check; Normal sessions require a known TargetName.
type TargetResolver interface {
	HasTarget(name string) bool
}

// LoginNegotiator drives the target side of login negotiation
// across one or many request/response rounds.
type LoginNegotiator struct {
	config NegotiableConfig

	phase    LoginPhase
	isid     [6]byte
	tsih     uint16
	targetOK bool

	// Negotiated outputs (updated as keys are processed).
	NegMaxRecvDataSegLen int
	NegMaxBurstLength    int
	NegFirstBurstLength  int
	NegInitialR2T        bool
	NegImmediateData     bool

	InitiatorName string
	TargetName    string
	SessionType   string

	chapID        byte
	chapChallenge []byte
	chapMethodOK  bool
	chapOK        bool
}

// NewLoginNegotiator constructs a fresh negotiator using the
// given negotiation profile.
func NewLoginNegotiator(cfg NegotiableConfig) *LoginNegotiator {
	return &LoginNegotiator{
		config:               cfg,
		phase:                LoginPhaseStart,
		NegMaxRecvDataSegLen: cfg.MaxRecvDataSegmentLength,
		NegMaxBurstLength:    cfg.MaxBurstLength,
		NegFirstBurstLength:  cfg.FirstBurstLength,
		NegInitialR2T:        cfg.InitialR2T,
		NegImmediateData:     cfg.ImmediateData,
	}
}

// Done returns true if negotiation has reached FullFeature.
func (ln *LoginNegotiator) Done() bool { return ln.phase == LoginPhaseDone }

// Phase returns the current login phase (for diagnostics).
func (ln *LoginNegotiator) Phase() LoginPhase { return ln.phase }

// HandleLoginPDU processes one login request and returns the
// response PDU. Caller writes the response and reads the next
// request until Done() returns true (or response carries a
// reject status).
func (ln *LoginNegotiator) HandleLoginPDU(req *PDU, resolver TargetResolver) *PDU {
	resp := &PDU{}
	resp.SetOpcode(OpLoginResp)
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	resp.SetISID(req.ISID())
	resp.SetTSIH(req.TSIH())

	if req.Opcode() != OpLoginReq {
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
		return resp
	}
	csg := req.LoginCSG()
	nsg := req.LoginNSG()
	transit := req.LoginTransit()

	params, err := ParseParams(req.DataSegment)
	if err != nil {
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
		return resp
	}

	respParams := NewParams()

	switch csg {
	case StageSecurityNeg:
		if ln.phase != LoginPhaseStart && ln.phase != LoginPhaseSecurity {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
			return resp
		}
		ln.phase = LoginPhaseSecurity
		if !ln.captureSessionIdentity(params, resolver, resp) {
			return resp
		}
		ln.isid = req.ISID()
		if ln.chapRequiredForSession() {
			if !ln.handleCHAPSecurity(params, resp, respParams) {
				return resp
			}
		} else {
			respParams.Set("AuthMethod", "None")
		}
		if transit {
			if ln.chapRequiredForSession() && !ln.chapOK {
				if authInProgress(respParams) {
					transit = false
					nsg = StageSecurityNeg
				} else {
					setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailAuthFailed)
					return resp
				}
			}
			if transit {
				if nsg == StageLoginOp {
					ln.phase = LoginPhaseOperational
				} else if nsg == StageFullFeature {
					ln.phase = LoginPhaseDone
				}
			}
		}
	case StageLoginOp:
		// V2 allows direct jump to LoginOp when SecurityNeg was
		// skipped. Same here: capture identity in this PDU if not
		// done yet.
		if ln.phase == LoginPhaseStart {
			if !ln.captureSessionIdentity(params, resolver, resp) {
				return resp
			}
			if ln.chapRequiredForSession() {
				setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailAuthFailed)
				return resp
			}
			ln.isid = req.ISID()
		} else if ln.phase != LoginPhaseSecurity && ln.phase != LoginPhaseOperational {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
			return resp
		}
		ln.phase = LoginPhaseOperational
		ln.negotiateOperational(params, respParams)
		if transit && nsg == StageFullFeature {
			ln.phase = LoginPhaseDone
		}
	default:
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailInitiatorError)
		return resp
	}

	resp.SetLoginStages(csg, nsg)
	if transit {
		resp.SetLoginTransit(true)
	}
	resp.SetLoginStatus(LoginStatusSuccess, LoginDetailSuccess)

	if ln.tsih == 0 {
		ln.tsih = 1
	}
	resp.SetTSIH(ln.tsih)

	tpgt := ln.config.TargetPortalGroupTag
	if tpgt <= 0 {
		tpgt = 1
	}
	respParams.Set("TargetPortalGroupTag", strconv.Itoa(tpgt))

	if respParams.Len() > 0 {
		resp.DataSegment = respParams.Encode()
	}
	return resp
}

// captureSessionIdentity extracts InitiatorName, SessionType,
// and (for Normal sessions) TargetName from the request params.
// Writes a reject into resp and returns false on missing
// required fields.
func (ln *LoginNegotiator) captureSessionIdentity(params *Params, resolver TargetResolver, resp *PDU) bool {
	if name, ok := params.Get("InitiatorName"); ok {
		ln.InitiatorName = name
	} else if ln.InitiatorName == "" {
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailMissingParam)
		return false
	}
	if st, ok := params.Get("SessionType"); ok {
		ln.SessionType = st
	}
	if ln.SessionType == "" {
		ln.SessionType = SessionTypeNormal
	}
	if tn, ok := params.Get("TargetName"); ok {
		// Normal sessions must name a target this server hosts.
		// Discovery sessions skip the check (TargetName is "All"
		// or absent for SendTargets).
		if ln.SessionType == SessionTypeNormal {
			if resolver == nil || !resolver.HasTarget(tn) {
				setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailNotFound)
				return false
			}
		}
		ln.TargetName = tn
		ln.targetOK = true
	} else if ln.SessionType == SessionTypeNormal && !ln.targetOK {
		setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailMissingParam)
		return false
	}
	return true
}

// negotiateOperational walks the request's operational params
// and writes responses into respParams.
func (ln *LoginNegotiator) negotiateOperational(req, resp *Params) {
	req.Each(func(key, value string) {
		switch key {
		case "MaxRecvDataSegmentLength":
			if v, err := NegotiateNumber(value, ln.config.MaxRecvDataSegmentLength, 512, 16777215); err == nil {
				ln.NegMaxRecvDataSegLen = v
				// V2 quirk preserved: declare OUR max in response,
				// not the negotiated value, so the initiator knows
				// our send-side limit. The negotiated value bounds
				// what we can RECEIVE.
				resp.Set(key, strconv.Itoa(ln.config.MaxRecvDataSegmentLength))
			}
		case "MaxBurstLength":
			if v, err := NegotiateNumber(value, ln.config.MaxBurstLength, 512, 16777215); err == nil {
				ln.NegMaxBurstLength = v
				resp.Set(key, strconv.Itoa(v))
			}
		case "FirstBurstLength":
			if v, err := NegotiateNumber(value, ln.config.FirstBurstLength, 512, 16777215); err == nil {
				ln.NegFirstBurstLength = v
				resp.Set(key, strconv.Itoa(v))
			}
		case "InitialR2T":
			// OR semantics per RFC 7143: result is Yes if either
			// party says Yes.
			if value == "Yes" || ln.config.InitialR2T {
				ln.NegInitialR2T = true
			} else {
				ln.NegInitialR2T = false
			}
			resp.Set(key, BoolStr(ln.NegInitialR2T))
		case "ImmediateData":
			if v, err := NegotiateBool(value, ln.config.ImmediateData); err == nil {
				ln.NegImmediateData = v
				resp.Set(key, BoolStr(v))
			}
		case "MaxConnections":
			resp.Set(key, strconv.Itoa(ln.config.MaxConnections))
		case "DataPDUInOrder":
			resp.Set(key, BoolStr(ln.config.DataPDUInOrder))
		case "DataSequenceInOrder":
			resp.Set(key, BoolStr(ln.config.DataSequenceInOrder))
		case "DefaultTime2Wait":
			resp.Set(key, strconv.Itoa(ln.config.DefaultTime2Wait))
		case "DefaultTime2Retain":
			resp.Set(key, strconv.Itoa(ln.config.DefaultTime2Retain))
		case "MaxOutstandingR2T":
			resp.Set(key, strconv.Itoa(ln.config.MaxOutstandingR2T))
		case "ErrorRecoveryLevel":
			resp.Set(key, strconv.Itoa(ln.config.ErrorRecoveryLevel))
		case "HeaderDigest":
			// Always negotiate to None in T2 scope; digest
			// verification is T8 security.
			resp.Set(key, "None")
		case "DataDigest":
			resp.Set(key, "None")
		case "TargetName", "InitiatorName", "SessionType", "AuthMethod":
			// Already captured / declarative.
		case "TargetAlias", "InitiatorAlias":
			// Informational.
		default:
			resp.Set(key, "NotUnderstood")
		}
	})
	if ln.config.TargetAlias != "" {
		resp.Set("TargetAlias", ln.config.TargetAlias)
	}
}

func (ln *LoginNegotiator) chapRequired() bool {
	return ln.config.CHAP.Secret != ""
}

func (ln *LoginNegotiator) chapRequiredForSession() bool {
	return ln.chapRequired() && ln.SessionType != SessionTypeDiscovery
}

func (ln *LoginNegotiator) handleCHAPSecurity(req *Params, resp *PDU, respParams *Params) bool {
	if ln.config.CHAP.Username == "" {
		setLoginReject(resp, LoginStatusTargetErr, LoginDetailTargetError)
		return false
	}
	if method, ok := req.Get("AuthMethod"); ok {
		if !valueListIncludes(method, "CHAP") {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailAuthFailed)
			return false
		}
		ln.chapMethodOK = true
	}
	if name, hasName := req.Get("CHAP_N"); hasName {
		gotResp, hasResp := req.Get("CHAP_R")
		if !hasResp {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailMissingParam)
			return false
		}
		if !ln.verifyCHAP(name, gotResp) {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailAuthFailed)
			return false
		}
		ln.chapOK = true
		return true
	}
	if alg, hasAlg := req.Get("CHAP_A"); hasAlg {
		if !ln.chapMethodOK || !valueListIncludes(alg, "5") {
			setLoginReject(resp, LoginStatusInitiatorErr, LoginDetailAuthFailed)
			return false
		}
		if ln.chapChallenge == nil {
			challenge, err := ln.newCHAPChallenge()
			if err != nil {
				setLoginReject(resp, LoginStatusTargetErr, LoginDetailTargetError)
				return false
			}
			ln.chapChallenge = challenge
		}
		if ln.chapID == 0 {
			ln.chapID = 1
		}
		respParams.Set("CHAP_A", "5")
		respParams.Set("CHAP_I", strconv.Itoa(int(ln.chapID)))
		respParams.Set("CHAP_C", "0x"+hex.EncodeToString(ln.chapChallenge))
		return true
	}

	if !ln.chapMethodOK {
		respParams.Set("AuthMethod", "CHAP")
		return true
	}
	respParams.Set("AuthMethod", "CHAP")
	return true
}

func (ln *LoginNegotiator) newCHAPChallenge() ([]byte, error) {
	if len(ln.config.CHAP.Challenge) > 0 {
		challenge := append([]byte(nil), ln.config.CHAP.Challenge...)
		return challenge, nil
	}
	challenge := make([]byte, 16)
	if _, err := rand.Read(challenge); err != nil {
		return nil, fmt.Errorf("iscsi: generate CHAP challenge: %w", err)
	}
	return challenge, nil
}

func (ln *LoginNegotiator) verifyCHAP(username, response string) bool {
	if !ln.chapRequired() || ln.chapChallenge == nil {
		return false
	}
	if username != ln.config.CHAP.Username {
		return false
	}
	want := chapMD5Response(ln.chapID, ln.config.CHAP.Secret, ln.chapChallenge)
	got, ok := parseCHAPHex(response)
	return ok && strings.EqualFold(got, want)
}

func authInProgress(params *Params) bool {
	_, hasMethod := params.Get("AuthMethod")
	_, hasChallenge := params.Get("CHAP_C")
	return hasMethod || hasChallenge
}

func valueListIncludes(value, want string) bool {
	for _, part := range strings.Split(value, ",") {
		if strings.TrimSpace(part) == want {
			return true
		}
	}
	return false
}

func chapMD5Response(id byte, secret string, challenge []byte) string {
	h := md5.New()
	h.Write([]byte{id})
	h.Write([]byte(secret))
	h.Write(challenge)
	return "0x" + hex.EncodeToString(h.Sum(nil))
}

func parseCHAPHex(value string) (string, bool) {
	if !strings.HasPrefix(value, "0x") && !strings.HasPrefix(value, "0X") {
		return "", false
	}
	raw := value[2:]
	if _, err := hex.DecodeString(raw); err != nil {
		return "", false
	}
	return "0x" + strings.ToLower(raw), true
}

func setLoginReject(resp *PDU, class, detail uint8) {
	resp.SetLoginStatus(class, detail)
	resp.SetLoginTransit(false)
}

// LoginResult is the final negotiated state after Done()=true.
type LoginResult struct {
	InitiatorName     string
	TargetName        string
	SessionType       string
	ISID              [6]byte
	TSIH              uint16
	MaxRecvDataSegLen int
	MaxBurstLength    int
	FirstBurstLength  int
	InitialR2T        bool
	ImmediateData     bool
}

// Result returns the final negotiated values.
func (ln *LoginNegotiator) Result() LoginResult {
	return LoginResult{
		InitiatorName:     ln.InitiatorName,
		TargetName:        ln.TargetName,
		SessionType:       ln.SessionType,
		ISID:              ln.isid,
		TSIH:              ln.tsih,
		MaxRecvDataSegLen: ln.NegMaxRecvDataSegLen,
		MaxBurstLength:    ln.NegMaxBurstLength,
		FirstBurstLength:  ln.NegFirstBurstLength,
		InitialR2T:        ln.NegInitialR2T,
		ImmediateData:     ln.NegImmediateData,
	}
}
