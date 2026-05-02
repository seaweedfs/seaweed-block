// Package transport provides minimal TCP block replication for the sparrow.
// Three operations: Ship (WAL entry), Probe (handshake with R/S/H), Rebuild (full base copy).
//
// Wire format — V3 clean-break replication envelope (12 bytes):
//
//	[4B magic "SWRP"][1B version][1B type][1B flags][1B reserved][4B length][payload]
//
// Magic + version let replicas refuse foreign / incompatible traffic at the
// first byte. Flags and reserved are zero in V1 and carved out for future
// protocol extensions without reshuffling the preamble.
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

// Envelope preamble (V3 T4a-1 clean-break layout).
//
// MagicV3Repl is ASCII "SWRP" (SeaWeed Replication Protocol). Present on every
// frame. Replicas reject frames whose first 4 bytes do not match.
//
// ProtocolVersion = 1 is V1 of the V3 replication envelope. Unknown versions
// are rejected — there is no silent fallback. Future versions may change
// flags/reserved semantics or payload encoding.
//
// preambleLen is the fixed preamble byte count (4+1+1+1+1+4 = 12).
const (
	MagicV3Repl     uint32 = 0x53575250 // "SWRP"
	ProtocolVersion byte   = 1
	preambleLen            = 12
)

// Message types
const (
	MsgShipEntry    byte = 0x01 // primary → replica: one block write
	MsgProbeReq     byte = 0x02 // primary → replica: request boundaries
	MsgProbeResp    byte = 0x03 // replica → primary: R/S/H response
	MsgRebuildBlock byte = 0x04 // primary → replica: one base block
	MsgRebuildDone  byte = 0x05 // primary → replica: rebuild complete
	MsgBarrierReq   byte = 0x06 // primary → replica: sync request
	MsgBarrierResp  byte = 0x07 // replica → primary: sync response
	// MsgRecoveryLaneStart marks the current legacy SWRP connection as
	// recovery-lane before any MsgShipEntry frames arrive. It replaces
	// payload-derived lane guessing from RecoveryLineage.TargetLSN.
	MsgRecoveryLaneStart byte = 0x08
)

// RecoveryLineage binds a mutating recovery stream to one accepted recovery
// contract. The tuple is ordered by semantic authority:
// epoch -> endpointVersion -> sessionID, with a frontier hint frozen by the
// engine. The fourth wire slot is still named TargetLSN for compatibility,
// but it is no longer a completion predicate or lane signal.
// All four wire fields MUST be >= 1; zero-valued identity lineage is rejected
// at engine event ingestion before any recovery command is emitted.
type RecoveryLineage struct {
	SessionID       uint64
	Epoch           uint64
	EndpointVersion uint64
	FrontierHint    uint64
	TargetLSN       uint64 // legacy wire alias for FrontierHint
}

func (l RecoveryLineage) EffectiveFrontierHint() uint64 {
	if l.FrontierHint != 0 {
		return l.FrontierHint
	}
	return l.TargetLSN
}

func (l RecoveryLineage) WireLineage() RecoveryLineage {
	hint := l.EffectiveFrontierHint()
	l.FrontierHint = 0
	l.TargetLSN = hint
	return l
}

func (l RecoveryLineage) Equivalent(other RecoveryLineage) bool {
	return l.SessionID == other.SessionID &&
		l.Epoch == other.Epoch &&
		l.EndpointVersion == other.EndpointVersion &&
		l.EffectiveFrontierHint() == other.EffectiveFrontierHint()
}

// ShipEntry is one replicated block write.
type ShipEntry struct {
	Lineage RecoveryLineage
	LBA     uint32
	LSN     uint64
	Data    []byte
}

// ProbeRequest is the typed body of `MsgProbeReq` post-T4c-1. Carries the
// full `RecoveryLineage` of the probe attempt so the replica can validate
// who is asking. Per architect round-26 (Item C.3) the wire MUST carry
// every authority-bearing field — this is the request-side half of the
// symmetric pair (`ProbeResponse` is the response-side half).
//
// Probe lineage is **transient** — minted by the adapter (parallel to
// `FenceAtEpoch`), NOT registered in `executor.sessions`, NOT represented
// in engine session truth. See `feedback_t4c_probe_lineage_option_d.md`
// (architect Option D resolution).
//
// Wire (T4c-1): [32B lineage (SessionID, Epoch, EndpointVersion, frontierHint)] — 32 bytes.
type ProbeRequest struct {
	Lineage RecoveryLineage
}

// ProbeResponse carries the replica's recovery boundaries plus the echoed
// request lineage (T4c-1; per architect round-26 Item C.3).
//
// Wire (T4c-1):
//
//	[32B echoed lineage][8B SyncedLSN][8B WalTail][8B WalHead] — 56 bytes.
//
// Strict decode is fail-closed (round-21 uniform rule, mirrored from
// `BarrierResponse`): payloads shorter than 56 bytes, zero-valued lineage
// fields, or field-misordered lineage slabs are rejected. Primary-side
// consumer (`BlockExecutor.Probe`) MUST reject responses whose echoed
// lineage does not match the request lineage; mismatches return
// `ErrProbeLineageMismatch` and MUST NOT contribute to recovery facts.
type ProbeResponse struct {
	Lineage   RecoveryLineage // echoed from request — primary validates exact match
	SyncedLSN uint64          // R: replica's durable frontier
	WalTail   uint64          // S: oldest retained
	WalHead   uint64          // H: newest written
}

// probeReqSize / probeRespSize are the exact byte lengths of valid
// encoded probe payloads. Short, over-long, or field-misordered payloads
// are rejected by the decoders and MUST NOT contribute to recovery
// decisions (architect round-21 uniform rule, T4b-1 BarrierResp shape).
const (
	probeReqSize  = 32 // [32B lineage]
	probeRespSize = 56 // [32B lineage][8B R][8B S][8B H]
)

// --- Wire helpers ---

// WriteMsg sends a typed message on the connection.
//
// Wire preamble (12 bytes): [4B magic "SWRP"][1B version][1B type]
// [1B flags=0][1B reserved=0][4B length][payload...].
// Flags and reserved are always 0 in V1; future versions may define bits.
func WriteMsg(conn net.Conn, msgType byte, payload []byte) error {
	header := make([]byte, preambleLen)
	binary.BigEndian.PutUint32(header[0:4], MagicV3Repl)
	header[4] = ProtocolVersion
	header[5] = msgType
	header[6] = 0 // flags
	header[7] = 0 // reserved
	binary.BigEndian.PutUint32(header[8:12], uint32(len(payload)))
	if _, err := conn.Write(header); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := conn.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// MaxPayloadSize is the upper bound for incoming message payloads.
// Prevents memory exhaustion from malicious or buggy senders.
const MaxPayloadSize = 16 * 1024 * 1024 // 16MB

// ReadMsg reads a typed message from the connection. Validates magic + version
// before accepting the frame; mismatches are rejected with a descriptive error
// so the caller can close the conn rather than attempt to recover.
func ReadMsg(conn net.Conn) (msgType byte, payload []byte, err error) {
	header := make([]byte, preambleLen)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != MagicV3Repl {
		return 0, nil, fmt.Errorf("transport: bad magic: 0x%08x (want 0x%08x)", magic, MagicV3Repl)
	}
	version := header[4]
	if version != ProtocolVersion {
		return 0, nil, fmt.Errorf("transport: unsupported version: %d (want %d)", version, ProtocolVersion)
	}
	msgType = header[5]
	// header[6] (flags) and header[7] (reserved) are ignored in V1; reserved
	// for forward-compatible extension.
	length := binary.BigEndian.Uint32(header[8:12])
	if length > MaxPayloadSize {
		return 0, nil, fmt.Errorf("transport: payload too large: %d > %d", length, MaxPayloadSize)
	}
	if length > 0 {
		payload = make([]byte, length)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return 0, nil, err
		}
	}
	return msgType, payload, nil
}

// EncodeLineage serializes a recovery lineage.
// Wire: [8B sessionID][8B epoch][8B endpointVersion][8B frontierHint]
func EncodeLineage(l RecoveryLineage) []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[0:8], l.SessionID)
	binary.BigEndian.PutUint64(buf[8:16], l.Epoch)
	binary.BigEndian.PutUint64(buf[16:24], l.EndpointVersion)
	binary.BigEndian.PutUint64(buf[24:32], l.EffectiveFrontierHint())
	return buf
}

// DecodeLineage deserializes a recovery lineage.
func DecodeLineage(buf []byte) (RecoveryLineage, error) {
	if len(buf) < 32 {
		return RecoveryLineage{}, fmt.Errorf("transport: short recovery lineage")
	}
	return RecoveryLineage{
		SessionID:       binary.BigEndian.Uint64(buf[0:8]),
		Epoch:           binary.BigEndian.Uint64(buf[8:16]),
		EndpointVersion: binary.BigEndian.Uint64(buf[16:24]),
		TargetLSN:       binary.BigEndian.Uint64(buf[24:32]),
	}, nil
}

// EncodeShipEntry serializes a ship entry.
// Wire: [32B lineage][4B LBA][8B LSN][data...]
func EncodeShipEntry(e ShipEntry) []byte {
	buf := make([]byte, 44+len(e.Data))
	copy(buf[0:32], EncodeLineage(e.Lineage))
	binary.BigEndian.PutUint32(buf[32:36], e.LBA)
	binary.BigEndian.PutUint64(buf[36:44], e.LSN)
	copy(buf[44:], e.Data)
	return buf
}

// DecodeShipEntry deserializes a ship entry.
func DecodeShipEntry(buf []byte) (ShipEntry, error) {
	if len(buf) < 44 {
		return ShipEntry{}, fmt.Errorf("transport: short ship entry")
	}
	lineage, err := DecodeLineage(buf[0:32])
	if err != nil {
		return ShipEntry{}, err
	}
	return ShipEntry{
		Lineage: lineage,
		LBA:     binary.BigEndian.Uint32(buf[32:36]),
		LSN:     binary.BigEndian.Uint64(buf[36:44]),
		Data:    buf[44:],
	}, nil
}

// EncodeProbeReq serializes a probe request (T4c-1 wire shape).
// Wire: [32B lineage (SessionID, Epoch, EndpointVersion, TargetLSN)]
func EncodeProbeReq(p ProbeRequest) []byte {
	buf := make([]byte, probeReqSize)
	copy(buf[0:32], EncodeLineage(p.Lineage))
	return buf
}

// DecodeProbeReq deserializes a probe request. Strict and fail-closed
// per round-21 uniform rule (mirrored from `DecodeBarrierResp`):
//   - payloads shorter than 32 bytes are invalid
//   - zero-valued lineage fields are invalid (catches partially-
//     initialized-struct drift; replica's acceptMutationLineage already
//     rejects zeros, but the wire fails closed before the handler runs)
//   - exact field order inside the lineage slab must be
//     SessionID, Epoch, EndpointVersion, TargetLSN
//
// Replica handler MUST treat any error return as "no valid probe" and
// MUST close the conn without echoing — a malformed request cannot
// produce a response that the primary can validate.
func DecodeProbeReq(buf []byte) (ProbeRequest, error) {
	if len(buf) < probeReqSize {
		return ProbeRequest{}, fmt.Errorf("transport: short probe request: %d bytes, want %d",
			len(buf), probeReqSize)
	}
	lineage, err := DecodeLineage(buf[0:32])
	if err != nil {
		return ProbeRequest{}, fmt.Errorf("transport: probe request lineage decode: %w", err)
	}
	if lineage.SessionID == 0 || lineage.Epoch == 0 ||
		lineage.EndpointVersion == 0 || lineage.EffectiveFrontierHint() == 0 {
		return ProbeRequest{}, fmt.Errorf("transport: probe request has zero-valued lineage field (sessionID=%d epoch=%d endpointVersion=%d frontierHint=%d)",
			lineage.SessionID, lineage.Epoch, lineage.EndpointVersion, lineage.EffectiveFrontierHint())
	}
	return ProbeRequest{Lineage: lineage}, nil
}

// EncodeProbeResp serializes a probe response (T4c-1 wire shape).
// Wire: [32B echoed lineage][8B syncedLSN][8B walTail][8B walHead]
func EncodeProbeResp(p ProbeResponse) []byte {
	buf := make([]byte, probeRespSize)
	copy(buf[0:32], EncodeLineage(p.Lineage))
	binary.BigEndian.PutUint64(buf[32:40], p.SyncedLSN)
	binary.BigEndian.PutUint64(buf[40:48], p.WalTail)
	binary.BigEndian.PutUint64(buf[48:56], p.WalHead)
	return buf
}

// DecodeProbeResp deserializes a probe response. Strict and fail-closed
// per round-21 uniform rule (mirrored from `DecodeBarrierResp`).
// Primary-side consumer MUST additionally validate the echoed lineage
// against the request lineage (`ErrProbeLineageMismatch`) — the decoder
// only enforces well-formed payload, not lineage-match.
func DecodeProbeResp(buf []byte) (ProbeResponse, error) {
	if len(buf) < probeRespSize {
		return ProbeResponse{}, fmt.Errorf("transport: short probe response: %d bytes, want %d",
			len(buf), probeRespSize)
	}
	lineage, err := DecodeLineage(buf[0:32])
	if err != nil {
		return ProbeResponse{}, fmt.Errorf("transport: probe response lineage decode: %w", err)
	}
	if lineage.SessionID == 0 || lineage.Epoch == 0 ||
		lineage.EndpointVersion == 0 || lineage.EffectiveFrontierHint() == 0 {
		return ProbeResponse{}, fmt.Errorf("transport: probe response has zero-valued lineage field (sessionID=%d epoch=%d endpointVersion=%d frontierHint=%d)",
			lineage.SessionID, lineage.Epoch, lineage.EndpointVersion, lineage.EffectiveFrontierHint())
	}
	return ProbeResponse{
		Lineage:   lineage,
		SyncedLSN: binary.BigEndian.Uint64(buf[32:40]),
		WalTail:   binary.BigEndian.Uint64(buf[40:48]),
		WalHead:   binary.BigEndian.Uint64(buf[48:56]),
	}, nil
}

// EncodeRebuildBlock serializes one rebuild block.
// Wire: [32B lineage][4B LBA][data...]
func EncodeRebuildBlock(lineage RecoveryLineage, lba uint32, data []byte) []byte {
	buf := make([]byte, 36+len(data))
	copy(buf[0:32], EncodeLineage(lineage))
	binary.BigEndian.PutUint32(buf[32:36], lba)
	copy(buf[36:], data)
	return buf
}

// DecodeRebuildBlock deserializes one rebuild block.
func DecodeRebuildBlock(buf []byte) (lineage RecoveryLineage, lba uint32, data []byte, err error) {
	if len(buf) < 36 {
		return RecoveryLineage{}, 0, nil, fmt.Errorf("transport: short rebuild block")
	}
	lineage, err = DecodeLineage(buf[0:32])
	if err != nil {
		return RecoveryLineage{}, 0, nil, err
	}
	return lineage, binary.BigEndian.Uint32(buf[32:36]), buf[36:], nil
}
