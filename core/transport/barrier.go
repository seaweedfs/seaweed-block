package transport

import (
	"encoding/binary"
	"fmt"
	"net"
	"time"
)

// BarrierResponse is the typed reply to MsgBarrierReq and to the
// terminal MsgRebuildDone. AchievedLSN is the replica's actual
// synced frontier after applying every pending write — NOT the
// primary's intent target. Lineage is the full authority tuple
// echoed from the request's registered session so the primary can
// validate the ack against its expected session without relying
// on cached primary-side assumptions (architect round-21 uniform
// rule, INV-REPL-LINEAGE-BORNE-ON-BARRIER-ACK).
//
// Wire (post-T4b-1): [32B lineage][8B achievedLSN] — 40 bytes.
// Lineage field order inside the 32B slab is strictly
// SessionID, Epoch, EndpointVersion, TargetLSN (matches
// EncodeLineage). Any deviation MUST be rejected at decode.
type BarrierResponse struct {
	Lineage     RecoveryLineage
	AchievedLSN uint64
}

// barrierRespSize is the exact byte length of a valid encoded
// BarrierResponse. Short, over-long, or field-misordered payloads
// are rejected by DecodeBarrierResp and MUST NOT contribute to
// correctness decisions (round-21 rule).
const barrierRespSize = 40

// EncodeBarrierResp serializes a barrier response.
// Wire: [32B lineage (SessionID, Epoch, EndpointVersion, TargetLSN)]
//       [8B achievedLSN]
func EncodeBarrierResp(r BarrierResponse) []byte {
	buf := make([]byte, barrierRespSize)
	copy(buf[0:32], EncodeLineage(r.Lineage))
	binary.BigEndian.PutUint64(buf[32:40], r.AchievedLSN)
	return buf
}

// DecodeBarrierResp deserializes a barrier response. Strict and
// fail-closed per round-21 uniform rule:
//   - payloads shorter than 40 bytes are invalid
//   - zero-valued or malformed lineage fields are invalid
//     (catches partially-initialized-struct drift)
//   - exact field order inside the lineage slab must be
//     SessionID, Epoch, EndpointVersion, TargetLSN
//
// Callers MUST treat any error return as "no valid ack" and MUST NOT
// fabricate a frontier or count the ack toward durability.
func DecodeBarrierResp(buf []byte) (BarrierResponse, error) {
	if len(buf) < barrierRespSize {
		return BarrierResponse{}, fmt.Errorf("transport: short barrier response: %d bytes, want %d",
			len(buf), barrierRespSize)
	}
	lineage, err := DecodeLineage(buf[0:32])
	if err != nil {
		return BarrierResponse{}, fmt.Errorf("transport: barrier response lineage decode: %w", err)
	}
	// Reject all-zero lineage. acceptMutationLineage already rejects
	// zero values on the request path (replica.go:186-188); round-21
	// applies the same rule to the ack path so a silently-zeroed or
	// stale-heap-reuse response cannot sneak through as a valid ack.
	if lineage.SessionID == 0 || lineage.Epoch == 0 ||
		lineage.EndpointVersion == 0 || lineage.TargetLSN == 0 {
		return BarrierResponse{}, fmt.Errorf("transport: barrier response has zero-valued lineage field (sessionID=%d epoch=%d endpointVersion=%d targetLSN=%d)",
			lineage.SessionID, lineage.Epoch, lineage.EndpointVersion, lineage.TargetLSN)
	}
	return BarrierResponse{
		Lineage:     lineage,
		AchievedLSN: binary.BigEndian.Uint64(buf[32:40]),
	}, nil
}

// sendBarrierReq writes a MsgBarrierReq frame on conn carrying the
// session lineage. Sets the conn deadline so a silent replica cannot
// park the sender indefinitely.
func sendBarrierReq(conn net.Conn, lineage RecoveryLineage, deadline time.Duration) error {
	if err := conn.SetDeadline(time.Now().Add(deadline)); err != nil {
		return fmt.Errorf("barrier: set deadline: %w", err)
	}
	return WriteMsg(conn, MsgBarrierReq, EncodeLineage(lineage))
}

// recvBarrierResp reads and decodes the next message as a barrier
// response. Returns an error if the next message is not a
// MsgBarrierResp or the payload is malformed. The primary uses the
// returned AchievedLSN as the engine-facing completion fact.
func recvBarrierResp(conn net.Conn, deadline time.Duration) (BarrierResponse, error) {
	if err := conn.SetDeadline(time.Now().Add(deadline)); err != nil {
		return BarrierResponse{}, fmt.Errorf("barrier: set deadline: %w", err)
	}
	msgType, payload, err := ReadMsg(conn)
	if err != nil {
		return BarrierResponse{}, fmt.Errorf("barrier: read resp: %w", err)
	}
	if msgType != MsgBarrierResp {
		return BarrierResponse{}, fmt.Errorf("barrier: unexpected msg type 0x%02x", msgType)
	}
	return DecodeBarrierResp(payload)
}
