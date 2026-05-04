package iscsi

// SendTargets discovery (RFC 7143 §12.3) — T2 ckpt 9 mechanism
// port. Adapted from weed/storage/blockvol/iscsi/discovery.go.
//
// Discovery is what `iscsiadm -m discovery -t sendtargets -p ADDR`
// uses: open a Discovery session, issue a Text Request with
// SendTargets=All, receive a list of TargetName=... +
// TargetAddress=... pairs, then logout. The volume host
// advertises only the target(s) it serves; multi-target /
// portal-group routing is later operator surface (T8+).

// DiscoveryTarget is one entry in a SendTargets response.
type DiscoveryTarget struct {
	Name    string // IQN, e.g. "iqn.2026-04.example.v3:v1"
	Address string // "ip:port,portal-group", e.g. "127.0.0.1:3260,1"
}

// HandleTextRequest processes one Text Request PDU. Currently
// supports SendTargets only; other text-key requests return an
// empty response (RFC 7143 §6 allows this).
func HandleTextRequest(req *PDU, targets []DiscoveryTarget) *PDU {
	resp := &PDU{}
	resp.SetOpcode(OpTextResp)
	resp.SetOpSpecific1(FlagF) // Final
	resp.SetInitiatorTaskTag(req.InitiatorTaskTag())
	resp.SetTargetTransferTag(0xFFFFFFFF) // no continuation

	params, err := ParseParams(req.DataSegment)
	if err != nil {
		return resp
	}
	val, ok := params.Get("SendTargets")
	if !ok {
		return resp
	}

	// Discovery responses use multi-value encoding (TargetName
	// + TargetAddress repeats per target), which our normal
	// Params encoder rejects (duplicate keys). Use a dedicated
	// emitter.
	var matched []DiscoveryTarget
	switch val {
	case "All":
		matched = targets
	default:
		for _, tgt := range targets {
			if tgt.Name == val {
				matched = append(matched, tgt)
				break
			}
		}
	}
	if data := EncodeDiscoveryTargets(matched); len(data) > 0 {
		resp.DataSegment = data
	}
	return resp
}

// EncodeDiscoveryTargets serializes a target list into
// SendTargets text format. Each target produces:
//
//	TargetName=<iqn>\0TargetAddress=<addr>\0
//
// Repeated TargetName / TargetAddress keys are SPECIFIC to
// discovery responses (RFC 7143 §6.1 exception); normal
// negotiation rejects duplicates.
func EncodeDiscoveryTargets(targets []DiscoveryTarget) []byte {
	if len(targets) == 0 {
		return nil
	}
	var buf []byte
	for _, tgt := range targets {
		buf = append(buf, "TargetName="+tgt.Name+"\x00"...)
		if tgt.Address != "" {
			buf = append(buf, "TargetAddress="+tgt.Address+"\x00"...)
		}
	}
	return buf
}

// TargetLister is the seam Target implements so the session
// layer can resolve the discovery list at request time. Returning
// the live list (rather than capturing it at session start) lets
// future multi-target hosts add/remove targets without
// reshuffling sessions.
type TargetLister interface {
	ListTargets() []DiscoveryTarget
}
