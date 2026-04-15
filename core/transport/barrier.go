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
// primary's intent target. The primary must return this value
// verbatim when reporting completion to the engine; widening it
// would let the executor silently redefine what "complete" means,
// which the engine contract forbids.
type BarrierResponse struct {
	AchievedLSN uint64
}

const barrierRespSize = 8

// EncodeBarrierResp serializes a barrier response.
// Wire: [8B achievedLSN]
func EncodeBarrierResp(r BarrierResponse) []byte {
	buf := make([]byte, barrierRespSize)
	binary.BigEndian.PutUint64(buf, r.AchievedLSN)
	return buf
}

// DecodeBarrierResp deserializes a barrier response. Returns an
// error on a short payload — callers must fail closed, never
// fabricate a frontier.
func DecodeBarrierResp(buf []byte) (BarrierResponse, error) {
	if len(buf) < barrierRespSize {
		return BarrierResponse{}, fmt.Errorf("transport: short barrier response")
	}
	return BarrierResponse{
		AchievedLSN: binary.BigEndian.Uint64(buf[:barrierRespSize]),
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
