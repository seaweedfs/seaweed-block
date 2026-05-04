package nvme

// NVMe/TCP Identify builders (admin opcode 0x06).
//
// Port-model port from weed/storage/blockvol/nvme/identify.go
// (V2), with 11 corrections applied per Batch 11 port plan §3.5
// deep-read findings (D1–D11, 2026-04-22):
//
//   D1  Serial "SWF00001" stub → derived from VolumeID (R1, §3.3 N1)
//   D2  CMIC=0x08 / ANACAP=0x08 / ANAGRPMAX=1 / NANAGRPID=1 /
//        Identify NS ANAGRPID=1 → ALL ZERO in 11a/11b per QA
//        finding #3 (Get Log Page ANA is 11c-conditional; advertising
//        ANA before that commit ships would violate §6 stop-rule #4)
//   D3  ONCS=0x0C (WriteZeros + DSM) → 0 (neither opcode implemented)
//   D4  Identify NS NSFEAT=0x01 (thin-prov / Trim advertised) → 0
//        (Dataset Management not in T2 scope)
//   D5  Identify NS DLFEAT=0x04 (deallocate returns zeros) → 0
//        (deallocate not implemented)
//   D6  AERTL=3 → 0 (one pending AER slot per finding #2; NVMe
//        encodes AERTL as "max N - 1", so value 0 == capacity 1)
//   D7  OFCS=0x01 (Disconnect supported) → 0 (Disconnect is 11b)
//   D8  SubNQN at offset 768 is NUL-terminated, NOT space-padded —
//        Linux kernel uses strcmp() against Connect's SubNQN
//   D9  LBAF[0] LBADS = log2(blockSize) = 9 for 512 B → unchanged
//   D10 IOCCSZ = (64 + MaxH2CDataLength) / 16; load-bearing for
//        kernel inline-write optimization; we compute from our
//        actual MaxH2CDataLength (32 KiB)
//   D11 VS (Version) = NVMe 1.3 (0x00010300); MNAN = 1
//        (NVMe ≥1.4 would require MNAN non-zero; we pin 1.3 for
//        simplicity and set MNAN=1 for forward-compat)
//
// N1 (port plan §3.3): NGUID derivation = sha256(SubsysNQN + "\x00"
// + VolumeID)[:16]; EUI-64 = NGUID[:8]. Deterministic, per-volume
// unique across SubsysNQN × VolumeID. 4 QA-owned pins (same-input
// determinism + both-dimension uniqueness + EUI-64-equals-prefix).

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/bits"
)

// handleAdminIdentify decodes the CNS value from CDW10 and
// dispatches to the matching builder. Unknown CNS values return
// InvalidField (Generic SC=0x02) without reaching a builder.
//
// BUG-001 fix: response flows through respCh. The 4 KiB C2HData
// payload is placed in response.c2hData; writeResponse emits the
// C2HData PDU followed by the CapsuleResp atomically.
func (s *Session) handleAdminIdentify(req *Request) error {
	cmd := &req.capsule
	cns := uint8(cmd.D10 & 0xFF)
	var data []byte
	switch cns {
	case cnsIdentifyController:
		data = s.buildIdentifyController()
	case cnsIdentifyNamespace:
		data = s.buildIdentifyNamespace(cmd.NSID)
	case cnsActiveNSList:
		data = s.buildActiveNSList(cmd.NSID)
	case cnsNSDescriptorList:
		data = s.buildNSDescriptorList(cmd.NSID)
	default:
		req.resp.Status = MakeStatusField(SCTGeneric, SCInvalidField, true)
		s.enqueueResponse(&response{resp: req.resp})
		return nil
	}
	s.enqueueResponse(&response{resp: req.resp, c2hData: data})
	return nil
}

// buildIdentifyController emits the 4 KiB Identify Controller
// data structure. Field positions match V2 identify.go + NVMe
// 1.3 spec §5.15.2.2. Every field that would advertise a
// capability the batch does not serve is zeroed per port plan
// §6 stop rule #4 ("advertised ≡ implemented").
func (s *Session) buildIdentifyController() []byte {
	buf := make([]byte, identifySize)

	subNQN := s.ctrlSubNQN()
	volumeID := s.ctrlVolumeID()

	// bytes 0-1: VID (PCI Vendor ID) — 0 for software target
	// bytes 2-3: SSVID — 0

	// bytes 4-23: Serial Number (20 bytes, space-padded ASCII).
	// D1 fix: derive from VolumeID rather than "SWF00001" stub.
	// 16 hex chars = 16 ASCII bytes; pad the remaining 4 with
	// spaces (SPC-5 §6.6.2 convention).
	copyPaddedWithSpace(buf[4:24], serialFromVolumeIDNVMe(volumeID))

	// bytes 24-63: Model Number (40 bytes, space-padded ASCII).
	copyPaddedWithSpace(buf[24:64], "SeaweedFS V3 BlockVol")

	// bytes 64-71: Firmware Revision (8 bytes, space-padded ASCII).
	copyPaddedWithSpace(buf[64:72], "0001")

	// byte 72: RAB (Recommended Arbitration Burst)
	buf[72] = 6

	// bytes 73-75: IEEE OUI (0 — not a registered manufacturer)

	// byte 76: CMIC (Controller Multi-Path I/O Capabilities).
	// D2 fix: V2 sets bit 3 (ANA reporting supported); we set 0
	// because ANA log is 11c-conditional per QA finding #3.
	buf[76] = 0x00

	// byte 77: MDTS (Maximum Data Transfer Size, 2^MDTS × min page).
	// MDTS=3 → 32 KiB with 4 KiB min-page. Matches our
	// MaxH2CDataLength cap (D10).
	buf[77] = 3

	// bytes 78-79: CNTLID (Controller ID) — what this session's
	// admin Connect allocated. Host reads it from the
	// CapsuleResp DW0 on admin Connect; we also echo it here so
	// Identify data is internally consistent with Connect resp.
	if s.ctrl != nil {
		binary.LittleEndian.PutUint16(buf[78:], s.ctrl.cntlID)
	}

	// bytes 80-83: VS (Version). D11: NVMe 1.3.
	binary.LittleEndian.PutUint32(buf[80:], nvmeVersion13)

	// bytes 84-259: various optional / derived controller caps
	// — most are 0 for a minimal software target.

	// bytes 256-257: OACS (Optional Admin Command Support).
	// Bit table per NVMe 1.3 §5.15.2.2. 11a implements:
	//   bit 0 (Security Send/Recv)    = 0
	//   bit 1 (Format NVM)             = 0
	//   bit 2 (Firmware Commit/Image)  = 0
	//   bit 3 (Namespace Mgmt)         = 0
	//   bit 4 (Device Self-Test)       = 0
	//   bit 5 (Directives)             = 0
	//   bit 6 (NVMe-MI Send/Receive)   = 0
	//   bit 7 (Virtualization)         = 0
	//   bit 8 (Doorbell Buffer Config) = 0
	// Batch 11b will light the appropriate bits when Abort /
	// Get Log / Format land. 11a leaves all 0.
	binary.LittleEndian.PutUint16(buf[256:], 0x0000)

	// byte 258: ACL (Abort Command Limit, zero-based).
	// We don't implement Abort; keep 0 (meaning 1 outstanding
	// abort allowed by spec, but since we never accept one,
	// moot).
	buf[258] = 0

	// byte 259: AERL (Async Event Request Limit, zero-based).
	// D6 fix: single pending AER slot → AERL=0 means "1 max".
	buf[259] = 0

	// byte 260: FRMW (Firmware Updates). Read-only slot 1.
	buf[260] = 0x02

	// byte 261: LPA (Log Page Attributes). 0 until Get Log Page
	// lands in 11c conditional (port plan §3.1 A15). If LPA
	// advertises Error/SMART/FW log support here but the handler
	// rejects, §6 stop rule #4 fires.
	buf[261] = 0x00

	// byte 262: ELPE (Error Log Page Entries, zero-based).
	buf[262] = 0

	// bytes 320-321: KAS (Keep Alive Support, 100 ms granularity).
	// 0 = no KAS; >0 = supported with this granularity. 11b
	// implements Set Features 0x0F (store KATO) + opcode 0x18
	// (KeepAlive responds Success), so KAS=1 (100ms minimum
	// granularity). Per QA constraint #2 the target does NOT
	// arm a fatal-timeout watchdog — KAS advertises that the
	// mechanism exists, not that missed KeepAlives terminate
	// the connection. Matches V2 behavior.
	binary.LittleEndian.PutUint16(buf[320:], 1)

	// byte 341: ANACAP (ANA Capabilities).
	// D2 fix: V2 sets bit 3 (Optimized state reported); we zero
	// the whole byte until 11c ANA log ships.
	buf[341] = 0x00

	// bytes 344-347: ANAGRPMAX. D2: 0.
	binary.LittleEndian.PutUint32(buf[344:], 0)
	// bytes 348-351: NANAGRPID. D2: 0.
	binary.LittleEndian.PutUint32(buf[348:], 0)

	// byte 512: SQES (Submission Queue Entry Size).
	// Low nibble = min power-of-2 (6 = 64 B); high nibble = max.
	buf[512] = 0x66
	// byte 513: CQES (Completion Queue Entry Size). 4 = 16 B.
	buf[513] = 0x44

	// bytes 514-515: MAXCMD (Max Commands per queue).
	binary.LittleEndian.PutUint16(buf[514:], 64)

	// bytes 516-519: NN (Number of Namespaces) = 1 in T2.
	binary.LittleEndian.PutUint32(buf[516:], 1)

	// bytes 520-521: ONCS (Optional NVM Command Support).
	// D3 fix: V2 sets bits 2+3 (Dataset Mgmt / Write Zeros).
	// Neither is implemented in T2 → all 0.
	binary.LittleEndian.PutUint16(buf[520:], 0x0000)

	// byte 525: VWC (Volatile Write Cache). bit 0 = present.
	// We have ioFlush mapped to Good in ckpt 7; VWC=1 is
	// consistent (advertise cache → host issues Flush → we
	// return Good). Symmetric to iSCSI MODE SENSE page 08
	// WCE=1 decision in Batch 10.5.
	buf[525] = 0x01

	// bytes 536-539: SGLS (SGL Support). bit 0 = SGLs supported
	// (REQUIRED for NVMe/TCP transport per spec).
	binary.LittleEndian.PutUint32(buf[536:], 0x00000001)

	// bytes 540-543: MNAN (Maximum Number of Allowed Namespaces).
	// D11: NVMe ≥1.4 requires non-zero; we're 1.3 but set 1 for
	// forward-compat and to match V2.
	binary.LittleEndian.PutUint32(buf[540:], 1)

	// bytes 768-1023: SubNQN (256 bytes, NUL-terminated, NOT
	// space-padded). D8: Linux kernel uses strcmp() against the
	// Connect SubNQN; padding with 0x20 would break matching.
	// The make([]byte) zero-fill already gives us NUL padding.
	copy(buf[768:1024], subNQN)

	// bytes 1792-1795: IOCCSZ (IO Capsule Command Supported
	// Size, 16-byte units). D10: (64 + MaxH2CDataLength) / 16.
	// We advertise 32 KiB → (64 + 32768) / 16 = 2052.
	const maxH2CDataLen uint32 = 0x8000 // must match handleICReq
	ioccsz := (64 + maxH2CDataLen) / 16
	binary.LittleEndian.PutUint32(buf[1792:], ioccsz)

	// bytes 1796-1799: IORCSZ (IO Response Capsule Supported
	// Size, 16-byte units). 16 / 16 = 1.
	binary.LittleEndian.PutUint32(buf[1796:], 1)

	// bytes 1800-1801: ICDOFF (In Capsule Data Offset, 16-byte
	// units). 0 means inline data immediately follows SQE.
	binary.LittleEndian.PutUint16(buf[1800:], 0)

	// byte 1802: FCATT (Fabrics Controller Attributes). bit 0 =
	// 0 means "I/O controller" (not a discovery controller).
	buf[1802] = 0x00

	// byte 1803: MSDBD (Maximum SGL Data Block Descriptors).
	buf[1803] = 1

	// bytes 1804-1805: OFCS (Optional Fabric Commands Supported).
	// D7 fix: V2 sets bit 0 (Disconnect). Disconnect is not in
	// Batch 11 scope → 0.
	binary.LittleEndian.PutUint16(buf[1804:], 0x0000)

	return buf
}

// buildIdentifyNamespace emits the 4 KiB Identify Namespace
// structure. Per NVMe 1.3 §5.15.2.3. Only NSID=1 is valid in
// T2; other NSIDs return a zero-filled buffer per spec
// "namespace not exist" semantics (the NSZE=0 signals
// non-existence to the host).
func (s *Session) buildIdentifyNamespace(nsid uint32) []byte {
	buf := make([]byte, identifySize)

	// Per NVMe 1.3: NSID=0xFFFFFFFF is "broadcast" (common
	// behaviors). We treat it as NSID=1 for T2 single-ns.
	// Other unknown NSIDs return all zeros (NSZE=0 → non-existent).
	effectiveNSID := nsid
	if effectiveNSID == 0xFFFFFFFF {
		effectiveNSID = 1
	}
	if s.handler == nil || effectiveNSID != s.handler.NSID() {
		return buf
	}

	blockSize := s.handler.BlockSize()
	nsze := s.handler.VolumeSize() / uint64(blockSize)

	// bytes 0-7: NSZE (Namespace Size in blocks)
	binary.LittleEndian.PutUint64(buf[0:], nsze)
	// bytes 8-15: NCAP (Namespace Capacity)
	binary.LittleEndian.PutUint64(buf[8:], nsze)
	// bytes 16-23: NUSE (Namespace Utilization)
	binary.LittleEndian.PutUint64(buf[16:], nsze)

	// byte 24: NSFEAT. D4 fix: V2 sets bit 0 (Thin provisioning
	// / Trim). DSM is skip-list → 0.
	buf[24] = 0x00

	// byte 25: NLBAF (Number of LBA Formats minus 1). One format.
	buf[25] = 0

	// byte 26: FLBAS (Formatted LBA Size). bits 3:0 = index (0).
	buf[26] = 0

	// byte 27: MC (Metadata Capabilities). 0 (no metadata).
	buf[27] = 0

	// byte 28: DLFEAT. D5 fix: V2 sets bit 2 (deallocate returns
	// zeros). Deallocate not implemented → 0.
	buf[28] = 0x00

	// byte 30: NMIC (Namespace Multi-path I/O Capabilities).
	// 0 = single controller. T2 single-session-per-subsystem.
	buf[30] = 0

	// bytes 92-95: ANAGRPID. D2 fix: V2 sets 1; we zero until
	// ANA log ships.
	binary.LittleEndian.PutUint32(buf[92:], 0)

	// bytes 104-119: NGUID (16 bytes). R1: per-(SubsysNQN,
	// VolumeID) deterministic derivation.
	nguid := nguidFromIdentity(s.ctrlSubNQN(), s.ctrlVolumeID())
	copy(buf[104:120], nguid[:])

	// bytes 120-127: EUI-64 (8 bytes). R1: EUI-64 = NGUID[:8].
	copy(buf[120:128], nguid[:8])

	// bytes 128-131: LBAF[0] (LBA Format 0).
	// Bits 23:16 = LBADS (log2 of block size). D9: 512 → 9.
	lbads := uint8(bits.TrailingZeros32(blockSize))
	binary.LittleEndian.PutUint32(buf[128:], uint32(lbads)<<16)

	return buf
}

// buildActiveNSList emits the Active Namespace list (CNS 0x02).
// Format per NVMe 1.3 §5.15.2.4: 1024 × uint32 NSIDs, ascending,
// 0 terminates. We have one namespace (NSID=1) when requested
// starting from NSID=0.
//
// The CDW1.NSID parameter is the "starting NSID" — response
// returns namespaces with ID > that. NSID=0 → all; NSID=1 → none
// (nothing above 1); etc.
func (s *Session) buildActiveNSList(startNSID uint32) []byte {
	buf := make([]byte, identifySize)
	// For single-namespace T2: emit NSID=1 only if caller's
	// starting NSID is 0 (i.e. asking for the first one).
	if startNSID == 0 {
		binary.LittleEndian.PutUint32(buf[0:], 1)
	}
	// Buffer is already zero-filled; zero terminates the list.
	return buf
}

// buildNSDescriptorList emits the Namespace Identification
// Descriptor list (CNS 0x03). Per NVMe 1.3 §5.15.2.5: sequence
// of descriptors, each with NIDT / NIDL / reserved / payload.
// We emit one NGUID descriptor (NIDT=0x02, NIDL=16).
func (s *Session) buildNSDescriptorList(nsid uint32) []byte {
	buf := make([]byte, identifySize)
	// Only NSID=1 is valid; for others return zero buffer
	// (indicates no descriptors, not an error).
	effectiveNSID := nsid
	if effectiveNSID == 0xFFFFFFFF {
		effectiveNSID = 1
	}
	if s.handler == nil || effectiveNSID != s.handler.NSID() {
		return buf
	}

	// NGUID descriptor:
	//   byte 0: NIDT (Namespace Identifier Type) = 0x02 (NGUID)
	//   byte 1: NIDL (length) = 16
	//   bytes 2-3: reserved
	//   bytes 4-19: NGUID payload
	nguid := nguidFromIdentity(s.ctrlSubNQN(), s.ctrlVolumeID())
	buf[0] = 0x02
	buf[1] = 16
	// bytes 2-3 reserved (zero)
	copy(buf[4:20], nguid[:])

	// EUI-64 descriptor could follow (NIDT=0x01, NIDL=8) but
	// NGUID is sufficient for Linux nvme-cli unique-ID building
	// per R1 test plan. Keep single descriptor in 11a.

	return buf
}

// ctrlSubNQN returns the subsystem NQN this session's admin
// controller was created with. Falls back to the session's
// expectedSubNQN if the controller isn't allocated yet (e.g. in
// narrow test setups). Never returns empty.
func (s *Session) ctrlSubNQN() string {
	if s.ctrl != nil && s.ctrl.subNQN != "" {
		return s.ctrl.subNQN
	}
	return s.expectedSubNQN
}

// ctrlVolumeID returns the volume ID this session's admin
// controller was pinned to. Falls back to the target config.
func (s *Session) ctrlVolumeID() string {
	if s.ctrl != nil && s.ctrl.volumeID != "" {
		return s.ctrl.volumeID
	}
	if s.target != nil {
		return s.target.cfg.VolumeID
	}
	return ""
}

// nguidFromIdentity builds a 16-byte NGUID from SubsysNQN +
// VolumeID. Algorithm (R1, port plan §3.3 N1):
//
//   NGUID = sha256(SubsysNQN + "\x00" + VolumeID)[:16]
//
// QA pins four invariants (TestT2V2Port_NVMe_IdentifyNamespaceDescriptor_Deterministic):
//   (a) same (SubsysNQN, VolumeID) → same NGUID across handler lifetimes
//   (b) same VolumeID, different SubsysNQN → different NGUID
//   (c) same SubsysNQN, different VolumeID → different NGUID
//   (d) EUI-64 = NGUID[:8] always
//
// No IEEE OUI prefix enforcement — sha256 bytes serve as an
// opaque identifier. If Linux requires a registered OUI we
// revisit in 11c (Discovery Bridge per port plan Q1).
func nguidFromIdentity(subNQN, volumeID string) [16]byte {
	h := sha256.New()
	h.Write([]byte(subNQN))
	h.Write([]byte{0x00})
	h.Write([]byte(volumeID))
	sum := h.Sum(nil)
	var out [16]byte
	copy(out[:], sum[:16])
	return out
}

// serialFromVolumeIDNVMe builds a 16-character ASCII serial
// from VolumeID. Symmetric with iscsi.serialFromVolumeID (Batch
// 10.5 follow-up #2) — keeps the "advertised ≡ implemented"
// discipline aligned across both protocols.
//
// Renamed from serialFromVolumeID to avoid colliding with the
// iSCSI helper if the two packages ever share a file.
func serialFromVolumeIDNVMe(volumeID string) string {
	sum := sha256.Sum256([]byte(volumeID))
	return hex.EncodeToString(sum[:8])
}

// copyPaddedWithSpace writes src into dst, padding the remainder
// with ASCII space (0x20). Used by Identify Controller ASCII
// fields (Serial, Model, Firmware Rev). V2 helper shape
// preserved.
func copyPaddedWithSpace(dst []byte, src string) {
	n := copy(dst, src)
	for i := n; i < len(dst); i++ {
		dst[i] = ' '
	}
}
