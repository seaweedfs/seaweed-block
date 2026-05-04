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
//   - CHAP authentication (T8 security)
//   - target redirects (multi-target operator surface, later)
//   - CHAP method negotiation
//
// Production code path remains "consumer of authority truth":
// no V2 storage / authority bindings appear in this file.

import (
	"errors"
	"strconv"
)

// Login status class/detail (RFC 7143 §11.13).
const (
	LoginStatusSuccess      uint8 = 0x00
	LoginStatusInitiatorErr uint8 = 0x02
	LoginStatusTargetErr    uint8 = 0x03

	LoginDetailSuccess        uint8 = 0x00
	LoginDetailInitiatorError uint8 = 0x00
	LoginDetailNotFound       uint8 = 0x03
	LoginDetailMissingParam   uint8 = 0x07
	LoginDetailTargetError    uint8 = 0x00
)

// Errors surfaced during login.
var (
	ErrLoginInvalidStage   = errors.New("iscsi: invalid login stage transition")
	ErrLoginInvalidRequest = errors.New("iscsi: invalid login request")
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
		// No CHAP — accept AuthMethod=None.
		respParams.Set("AuthMethod", "None")
		if transit {
			if nsg == StageLoginOp {
				ln.phase = LoginPhaseOperational
			} else if nsg == StageFullFeature {
				ln.phase = LoginPhaseDone
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
	resp.BHS[1] |= FlagF
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

func setLoginReject(resp *PDU, class, detail uint8) {
	resp.SetLoginStatus(class, detail)
	resp.SetLoginTransit(false)
}

// LoginResult is the final negotiated state after Done()=true.
type LoginResult struct {
	InitiatorName        string
	TargetName           string
	SessionType          string
	ISID                 [6]byte
	TSIH                 uint16
	MaxRecvDataSegLen    int
	MaxBurstLength       int
	FirstBurstLength     int
	InitialR2T           bool
	ImmediateData        bool
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
