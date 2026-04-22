package iscsi

// Login negotiation — minimal T2 scope.
//
// The V2 implementation does full parameter negotiation
// (MaxRecvDataSegmentLength, HeaderDigest, DataDigest, CHAP,
// etc.) because it needs to interop with OS initiators. For T2
// L1 (in-process component route), we only need a login that
// establishes a session so SCSI commands can flow. T2 L2-OS
// (real initiator) will extend this when the time comes; until
// then, we accept ANY login request with Transit=true and
// respond Success + NSG=FullFeature.
//
// The wire format is still real iSCSI login PDUs — just with
// negotiation collapsed into "accept and move on".

import "errors"

// Login status class/detail (RFC 7143 §11.13).
const (
	LoginStatusSuccess      uint8 = 0x00
	LoginStatusInitiatorErr uint8 = 0x02

	LoginDetailSuccess        uint8 = 0x00
	LoginDetailInitiatorError uint8 = 0x00
)

// Errors surfaced during login.
var (
	ErrLoginInvalidStage   = errors.New("iscsi: invalid login stage transition")
	ErrLoginInvalidRequest = errors.New("iscsi: invalid login request")
)

// buildLoginResponse constructs a Login-Response PDU matching
// the request's ITT / CmdSN fields and granting success. If the
// request has Transit=true and its NSG is FullFeature, the
// response grants the transition; otherwise the response keeps
// the connection in login phase. Data segment is empty (no
// text params) — sufficient for the in-process L1 client.
func buildLoginResponse(req *PDU, tsih uint16) *PDU {
	resp := &PDU{}
	resp.SetOpcode(OpLoginResp)
	// Copy stages + transit back: target grants the transition
	// the initiator requested (sketch §: minimal login).
	csg := req.LoginCSG()
	nsg := req.LoginNSG()
	resp.SetLoginStages(csg, nsg)
	resp.SetLoginTransit(req.LoginTransit())
	// Set final bit (last Login-Response for this round).
	resp.BHS[1] |= FlagF
	// Copy ISID + ITT so the initiator can match.
	resp.SetISID(req.ISID())
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	// TSIH: for a new session the target assigns it now.
	resp.SetTSIH(tsih)
	// StatSN (byte 24-27), ExpCmdSN (28-31), MaxCmdSN (32-35):
	// target advances StatSN per response; ExpCmdSN/MaxCmdSN
	// echo initiator CmdSN for now (no command queuing pre-FFP).
	resp.SetStatSN(0)
	resp.SetExpCmdSN(req.CmdSN() + 1)
	resp.SetMaxCmdSN(req.CmdSN() + 32)
	// Version-min / Version-max bytes 2-3 are zero (iSCSI v0).
	// Status class / detail (byte 36 / 37): success.
	resp.SetLoginStatus(LoginStatusSuccess, LoginDetailSuccess)
	return resp
}

// loginGrantsFullFeature reports whether a Login-Request asks
// (and our minimal negotiator accepts) to transit into Full
// Feature Phase. T2 scope: accept any request with Transit=true
// AND NSG=FullFeature.
func loginGrantsFullFeature(req *PDU) bool {
	return req.LoginTransit() && req.LoginNSG() == StageFullFeature
}
