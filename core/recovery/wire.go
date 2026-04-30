package recovery

import (
	"encoding/binary"
	"fmt"
	"io"
)

// Wire format for the dual-lane recover protocol.
//
// Frame layout: [1 byte type] [4 byte payload length, big-endian] [payload].
// All multi-byte integers in payloads are big-endian.
//
// This is a minimal protocol for the POC. It is intentionally separate
// from `core/transport/protocol.go` so the new dual-lane mechanism can
// be wired and tested without disturbing the existing single-lane path.
// Integration with the production wire format is a later step (decision
// is open in the layer-2 mini-plan: reuse MsgShipEntry vs add explicit
// lane field).

type frameType byte

const (
	frameSessionStart frameType = 1 // primary → replica: open session
	frameBaseBlock    frameType = 2 // primary → replica: base-lane block
	frameBaseDone     frameType = 3 // primary → replica: base lane is finished
	frameWALEntry     frameType = 4 // primary → replica: WAL-lane entry
	frameBarrierReq   frameType = 5 // primary → replica: request frontier sync
	frameBarrierResp  frameType = 6 // replica → primary: ack with achievedLSN
	frameBaseBatchAck frameType = 7 // replica → primary: incremental ack for pin_floor
)

const maxFramePayload = 16 * 1024 * 1024 // 16 MiB sanity cap

// writeFrame writes one frame. Caller-side mutex is the caller's job;
// concurrent writes to the same conn are unsafe.
func writeFrame(w io.Writer, t frameType, payload []byte) error {
	if len(payload) > maxFramePayload {
		return fmt.Errorf("recovery: frame payload %d exceeds cap %d", len(payload), maxFramePayload)
	}
	hdr := [5]byte{byte(t)}
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("recovery: write frame header: %w", err)
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("recovery: write frame payload: %w", err)
		}
	}
	return nil
}

// readFrame reads one frame. Returns (type, payload, err).
func readFrame(r io.Reader) (frameType, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	if n > maxFramePayload {
		return 0, nil, fmt.Errorf("recovery: frame payload %d exceeds cap", n)
	}
	if n == 0 {
		return frameType(hdr[0]), nil, nil
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, fmt.Errorf("recovery: read frame payload: %w", err)
	}
	return frameType(hdr[0]), payload, nil
}

// --- frame payload encodings ---

// sessionStart: [8 SessionID][8 FromLSN][8 TargetLSN][4 NumBlocks]
type sessionStartPayload struct {
	SessionID uint64
	FromLSN   uint64
	TargetLSN uint64
	NumBlocks uint32
}

func encodeSessionStart(s sessionStartPayload) []byte {
	out := make([]byte, 8+8+8+4)
	binary.BigEndian.PutUint64(out[0:], s.SessionID)
	binary.BigEndian.PutUint64(out[8:], s.FromLSN)
	binary.BigEndian.PutUint64(out[16:], s.TargetLSN)
	binary.BigEndian.PutUint32(out[24:], s.NumBlocks)
	return out
}

func decodeSessionStart(p []byte) (sessionStartPayload, error) {
	if len(p) != 28 {
		return sessionStartPayload{}, fmt.Errorf("recovery: sessionStart payload len=%d want 28", len(p))
	}
	return sessionStartPayload{
		SessionID: binary.BigEndian.Uint64(p[0:]),
		FromLSN:   binary.BigEndian.Uint64(p[8:]),
		TargetLSN: binary.BigEndian.Uint64(p[16:]),
		NumBlocks: binary.BigEndian.Uint32(p[24:]),
	}, nil
}

// baseBlock: [4 LBA][block bytes]
func encodeBaseBlock(lba uint32, data []byte) []byte {
	out := make([]byte, 4+len(data))
	binary.BigEndian.PutUint32(out[0:], lba)
	copy(out[4:], data)
	return out
}

func decodeBaseBlock(p []byte) (lba uint32, data []byte, err error) {
	if len(p) < 4 {
		return 0, nil, fmt.Errorf("recovery: baseBlock payload len=%d want >= 4", len(p))
	}
	return binary.BigEndian.Uint32(p[0:]), p[4:], nil
}

// WALEntryKind labels each WAL-lane frame with its origin so the
// receiver / observability layer can distinguish historical backlog
// replay from session-live writes pushed by the WAL shipper after
// session start.
//
// IMPORTANT: this is an observability + test-pinning aid, NOT a
// convergence proof. "Live line caught up" still requires
// barrier-ack + AchievedLSN ≥ targetLSN at the coordinator (architect
// ruling on G7-redo Layer-2 review). Do not infer "caught up" from
// the absence of Backlog frames alone — a quiet session could just
// mean both lanes are idle, not that the system is in sync.
type WALEntryKind byte

const (
	// WALKindBacklog is emitted by the sender's streamBacklog loop
	// (`ScanLBAs(fromLSN, ...)` historical replay). Bytes were
	// already durable on the primary at session start; this is the
	// "fill the gap up to frozen target" stream.
	WALKindBacklog WALEntryKind = 1
	// WALKindSessionLive is emitted by the sender's drainAndSeal
	// after the WAL shipper has fed live writes through
	// PushLiveWrite. These entries were written on the primary
	// AFTER session start (typically LSN > targetLSN, but routing
	// pushed them into the session lane per CHK-NO-FAKE-LIVE-
	// DURING-BACKLOG).
	WALKindSessionLive WALEntryKind = 2
)

func (k WALEntryKind) String() string {
	switch k {
	case WALKindBacklog:
		return "Backlog"
	case WALKindSessionLive:
		return "SessionLive"
	default:
		return fmt.Sprintf("WALEntryKind(%d)", byte(k))
	}
}

// walEntry: [1 Kind][4 LBA][8 LSN][block bytes]
func encodeWALEntry(kind WALEntryKind, lba uint32, lsn uint64, data []byte) []byte {
	out := make([]byte, 1+4+8+len(data))
	out[0] = byte(kind)
	binary.BigEndian.PutUint32(out[1:], lba)
	binary.BigEndian.PutUint64(out[5:], lsn)
	copy(out[13:], data)
	return out
}

func decodeWALEntry(p []byte) (kind WALEntryKind, lba uint32, lsn uint64, data []byte, err error) {
	if len(p) < 13 {
		return 0, 0, 0, nil, fmt.Errorf("recovery: walEntry payload len=%d want >= 13", len(p))
	}
	kind = WALEntryKind(p[0])
	if kind != WALKindBacklog && kind != WALKindSessionLive {
		return 0, 0, 0, nil, fmt.Errorf("recovery: walEntry kind=%d not in {1,2}", kind)
	}
	return kind, binary.BigEndian.Uint32(p[1:]), binary.BigEndian.Uint64(p[5:]), p[13:], nil
}

// WriteWALEntryFrame writes one WAL-lane frame on the dual-lane wire
// — public surface for transport-side EmitFunc that targets the
// recovery dual-lane port (P2d profile = DualLaneWALFrame).
//
// Wraps the package-internal frame format (writeFrame + encodeWALEntry)
// so the transport layer can emit recovery-compatible WAL frames
// without duplicating the encoding logic.
//
// `kind` MUST be WALKindBacklog (DrainBacklog scan emit) or
// WALKindSessionLive (NotifyAppend Realtime emit during session).
// The receiver-side rebuild_session uses kind only for observability
// counts (BacklogApplied / SessionLiveApplied); apply behavior is
// identical for both.
func WriteWALEntryFrame(w io.Writer, kind WALEntryKind, lba uint32, lsn uint64, data []byte) error {
	return writeFrame(w, frameWALEntry, encodeWALEntry(kind, lba, lsn, data))
}

// baseBatchAck: [8 SessionID][8 AcknowledgedLSN][4 BaseLBAUpper]
//
// SessionID identifies which session this ack belongs to (must match
// the active session on the receiving conn; mismatch → FailureProtocol).
// AcknowledgedLSN is the receiver's durable frontier as of this ack;
// drives `coord.SetPinFloor` per docs/recovery-pin-floor-wire.md §4.
// BaseLBAUpper reports the LBA prefix [0, BaseLBAUpper) durably
// installed on the receiver — advisory for future retransmit logic
// (NOT used for pin_floor in this milestone).
type baseBatchAckPayload struct {
	SessionID       uint64
	AcknowledgedLSN uint64
	BaseLBAUpper    uint32
}

func encodeBaseBatchAck(p baseBatchAckPayload) []byte {
	out := make([]byte, 8+8+4)
	binary.BigEndian.PutUint64(out[0:], p.SessionID)
	binary.BigEndian.PutUint64(out[8:], p.AcknowledgedLSN)
	binary.BigEndian.PutUint32(out[16:], p.BaseLBAUpper)
	return out
}

func decodeBaseBatchAck(p []byte) (baseBatchAckPayload, error) {
	if len(p) != 20 {
		return baseBatchAckPayload{}, fmt.Errorf("recovery: baseBatchAck payload len=%d want 20", len(p))
	}
	return baseBatchAckPayload{
		SessionID:       binary.BigEndian.Uint64(p[0:]),
		AcknowledgedLSN: binary.BigEndian.Uint64(p[8:]),
		BaseLBAUpper:    binary.BigEndian.Uint32(p[16:]),
	}, nil
}

// barrierResp: [8 AchievedLSN]
func encodeBarrierResp(achieved uint64) []byte {
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], achieved)
	return out[:]
}

func decodeBarrierResp(p []byte) (uint64, error) {
	if len(p) != 8 {
		return 0, fmt.Errorf("recovery: barrierResp payload len=%d want 8", len(p))
	}
	return binary.BigEndian.Uint64(p), nil
}
