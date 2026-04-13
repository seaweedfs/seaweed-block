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

// ShipEntry is one replicated block write.
type ShipEntry struct {
	LBA  uint32
	LSN  uint64
	Data []byte
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

// EncodeShipEntry serializes a ship entry.
// Wire: [4B LBA][8B LSN][data...]
func EncodeShipEntry(e ShipEntry) []byte {
	buf := make([]byte, 12+len(e.Data))
	binary.BigEndian.PutUint32(buf[0:4], e.LBA)
	binary.BigEndian.PutUint64(buf[4:12], e.LSN)
	copy(buf[12:], e.Data)
	return buf
}

// DecodeShipEntry deserializes a ship entry.
func DecodeShipEntry(buf []byte) (ShipEntry, error) {
	if len(buf) < 12 {
		return ShipEntry{}, fmt.Errorf("transport: short ship entry")
	}
	return ShipEntry{
		LBA:  binary.BigEndian.Uint32(buf[0:4]),
		LSN:  binary.BigEndian.Uint64(buf[4:12]),
		Data: buf[12:],
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
// Wire: [4B LBA][data...]
func EncodeRebuildBlock(lba uint32, data []byte) []byte {
	buf := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(buf[0:4], lba)
	copy(buf[4:], data)
	return buf
}

// DecodeRebuildBlock deserializes one rebuild block.
func DecodeRebuildBlock(buf []byte) (lba uint32, data []byte, err error) {
	if len(buf) < 4 {
		return 0, nil, fmt.Errorf("transport: short rebuild block")
	}
	return binary.BigEndian.Uint32(buf[0:4]), buf[4:], nil
}
