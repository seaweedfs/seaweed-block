// Package transport provides minimal TCP block replication for the sparrow.
// Three operations: Ship (WAL entry), Probe (handshake with R/S/H), Rebuild (full base copy).
//
// Wire format is intentionally simple — no framing library needed.
// Each message: [1B type][4B length][payload...]
package transport

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
)

// RecoveryLineage binds a mutating recovery stream to one accepted recovery
// contract. The tuple is ordered by semantic authority:
// epoch -> endpointVersion -> sessionID, with targetLSN frozen by the engine.
// All four fields MUST be >= 1; zero-valued identity lineage is rejected
// at engine event ingestion before any recovery command is emitted.
type RecoveryLineage struct {
	SessionID       uint64
	Epoch           uint64
	EndpointVersion uint64
	TargetLSN       uint64
}

// ShipEntry is one replicated block write.
type ShipEntry struct {
	Lineage RecoveryLineage
	LBA     uint32
	LSN     uint64
	Data    []byte
}

// ProbeResponse carries the replica's recovery boundaries.
type ProbeResponse struct {
	SyncedLSN uint64 // R: replica's durable frontier
	WalTail   uint64 // S: oldest retained
	WalHead   uint64 // H: newest written
}

// --- Wire helpers ---

// WriteMsg sends a typed message on the connection.
func WriteMsg(conn net.Conn, msgType byte, payload []byte) error {
	header := make([]byte, 5)
	header[0] = msgType
	binary.BigEndian.PutUint32(header[1:5], uint32(len(payload)))
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

// ReadMsg reads a typed message from the connection.
func ReadMsg(conn net.Conn) (msgType byte, payload []byte, err error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, nil, err
	}
	msgType = header[0]
	length := binary.BigEndian.Uint32(header[1:5])
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
// Wire: [8B sessionID][8B epoch][8B endpointVersion][8B targetLSN]
func EncodeLineage(l RecoveryLineage) []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[0:8], l.SessionID)
	binary.BigEndian.PutUint64(buf[8:16], l.Epoch)
	binary.BigEndian.PutUint64(buf[16:24], l.EndpointVersion)
	binary.BigEndian.PutUint64(buf[24:32], l.TargetLSN)
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

// EncodeProbeResp serializes a probe response.
// Wire: [8B syncedLSN][8B walTail][8B walHead]
func EncodeProbeResp(p ProbeResponse) []byte {
	buf := make([]byte, 24)
	binary.BigEndian.PutUint64(buf[0:8], p.SyncedLSN)
	binary.BigEndian.PutUint64(buf[8:16], p.WalTail)
	binary.BigEndian.PutUint64(buf[16:24], p.WalHead)
	return buf
}

// DecodeProbeResp deserializes a probe response.
func DecodeProbeResp(buf []byte) (ProbeResponse, error) {
	if len(buf) < 24 {
		return ProbeResponse{}, fmt.Errorf("transport: short probe response")
	}
	return ProbeResponse{
		SyncedLSN: binary.BigEndian.Uint64(buf[0:8]),
		WalTail:   binary.BigEndian.Uint64(buf[8:16]),
		WalHead:   binary.BigEndian.Uint64(buf[16:24]),
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
