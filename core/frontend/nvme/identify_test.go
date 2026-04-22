// Ownership: sw unit + component tests for Batch 11a Identify
// builders + queue routing + CNTLID registry. Maps to port
// plan §4 and pins D1–D11 deep-read findings at field level.
//
// QA A-tier Phase 3 tests (NAA/NGUID determinism, advertised ≡
// implemented, boundary guard) are QA-owned per §4 and land
// in `t2_v2port_nvme_*_test.go` after sw port ships.
package nvme_test

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend"
	"github.com/seaweedfs/seaweed-block/core/frontend/nvme"
	"github.com/seaweedfs/seaweed-block/core/frontend/testback"
)

// newIdentifyHarness spins up a Target + client pair ready for
// admin Identify.
func newIdentifyHarness(t *testing.T) (*nvme.Target, string, *nvmeClient) {
	t.Helper()
	rec := testback.NewRecordingBackend(frontend.Identity{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1,
	})
	prov := testback.NewStaticProvider(rec)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	cli := dialAndConnect(t, addr)
	return tg, addr, cli
}

// -------- D1–D11 field-level pins on Identify Controller --------

func TestT2Batch11a_IdentifyController_SerialIsNotStub(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, data := cli.adminIdentify(t, 0x01 /* Controller */, 0)
	expectStatusSuccess(t, status, "Identify Controller")
	if len(data) != 4096 {
		t.Fatalf("len=%d want 4096", len(data))
	}
	// D1: Serial (offset 4, 20 bytes) must NOT be the V2 stub.
	serial := string(bytes.TrimRight(data[4:24], " "))
	if serial == "SWF00001" {
		t.Fatal("Identify Controller Serial still uses V2 'SWF00001' stub — D1 regression")
	}
	// Must be 16 lowercase hex chars (serialFromVolumeIDNVMe shape).
	if len(serial) != 16 {
		t.Fatalf("serial %q length=%d want 16", serial, len(serial))
	}
	for _, b := range []byte(serial) {
		if !((b >= '0' && b <= '9') || (b >= 'a' && b <= 'f')) {
			t.Fatalf("serial %q contains non-hex byte 0x%02x", serial, b)
		}
	}
}

func TestT2Batch11a_IdentifyController_ANAFieldsAllZero(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// D2: CMIC (76) + ANACAP (341) + ANAGRPMAX (344-347) +
	// NANAGRPID (348-351) all must be 0 in 11a/11b.
	if data[76] != 0 {
		t.Fatalf("CMIC=0x%02x want 0 (ANA not implemented in 11a/11b)", data[76])
	}
	if data[341] != 0 {
		t.Fatalf("ANACAP=0x%02x want 0", data[341])
	}
	if got := binary.LittleEndian.Uint32(data[344:348]); got != 0 {
		t.Fatalf("ANAGRPMAX=%d want 0", got)
	}
	if got := binary.LittleEndian.Uint32(data[348:352]); got != 0 {
		t.Fatalf("NANAGRPID=%d want 0", got)
	}
}

func TestT2Batch11a_IdentifyController_ONCSAllZero(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// D3: ONCS (520-521) — Write Zeros + Dataset Mgmt bits
	// must be 0 (neither opcode implemented in T2 scope).
	if got := binary.LittleEndian.Uint16(data[520:522]); got != 0 {
		t.Fatalf("ONCS=0x%04x want 0 (WriteZeros + DSM not implemented)", got)
	}
}

func TestT2Batch11a_IdentifyController_AERLIsOneSlot(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// D6: AERL (259) zero-based = 0 means 1 pending AER slot.
	if data[259] != 0 {
		t.Fatalf("AERL=%d want 0 (single pending slot per finding #2)", data[259])
	}
}

func TestT2Batch11a_IdentifyController_OFCSZero(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// D7: OFCS (1804-1805) — Disconnect bit must be 0 (not
	// implemented in Batch 11).
	if got := binary.LittleEndian.Uint16(data[1804:1806]); got != 0 {
		t.Fatalf("OFCS=0x%04x want 0 (Disconnect not implemented)", got)
	}
}

func TestT2Batch11a_IdentifyController_SubNQNIsNulTerminated(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// D8: SubNQN at offset 768, 256 bytes. Must be NUL-terminated
	// (NOT space-padded) — Linux strcmp() depends on this.
	subnqn := data[768:1024]
	want := "nqn.2026-04.example.v3:subsys"
	if !strings.HasPrefix(string(subnqn), want) {
		t.Fatalf("SubNQN prefix=%q want %q", subnqn[:len(want)], want)
	}
	// Byte immediately after the string must be NUL (0x00), NOT space.
	term := subnqn[len(want)]
	if term != 0x00 {
		t.Fatalf("SubNQN terminator byte=0x%02x want 0x00 (NUL); space-padding would break strcmp()",
			term)
	}
}

func TestT2Batch11a_IdentifyController_IOCCSZMatchesMaxH2C(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// D10: IOCCSZ (1792-1795) = (64 + MaxH2CDataLength) / 16.
	// Our MaxH2CDataLength = 32 KiB → (64+32768)/16 = 2052.
	got := binary.LittleEndian.Uint32(data[1792:1796])
	want := uint32((64 + 0x8000) / 16)
	if got != want {
		t.Fatalf("IOCCSZ=%d want %d", got, want)
	}
}

func TestT2Batch11a_IdentifyController_VSIsNvme13(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// D11: VS (80-83) = NVMe 1.3.
	got := binary.LittleEndian.Uint32(data[80:84])
	if got != 0x00010300 {
		t.Fatalf("VS=0x%08x want 0x00010300 (NVMe 1.3)", got)
	}
	// MNAN (540-543) = 1 (forward-compat).
	mnan := binary.LittleEndian.Uint32(data[540:544])
	if mnan != 1 {
		t.Fatalf("MNAN=%d want 1", mnan)
	}
}

func TestT2Batch11a_IdentifyController_SGLSBitSet(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// SGLS (536-539) bit 0 MUST be 1 — required for NVMe/TCP.
	sgls := binary.LittleEndian.Uint32(data[536:540])
	if sgls&0x01 == 0 {
		t.Fatalf("SGLS=0x%08x bit 0 clear; required for NVMe/TCP", sgls)
	}
}

func TestT2Batch11a_IdentifyController_CNTLIDEchoesConnect(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x01, 0)
	// CNTLID (78-79) must match what admin Connect allocated.
	got := binary.LittleEndian.Uint16(data[78:80])
	if got != cli.cntlID {
		t.Fatalf("Identify CNTLID=%d != Connect-allocated CNTLID=%d", got, cli.cntlID)
	}
}

// -------- Identify Namespace --------

func TestT2Batch11a_IdentifyNamespace_SizeMatchesHandler(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, data := cli.adminIdentify(t, 0x00 /* Namespace */, 1)
	expectStatusSuccess(t, status, "Identify Namespace")
	if len(data) != 4096 {
		t.Fatalf("len=%d want 4096", len(data))
	}
	// NSZE (0-7). 1 MiB / 512 B = 2048 blocks.
	nsze := binary.LittleEndian.Uint64(data[0:8])
	if nsze != uint64(nvme.DefaultVolumeBlocks) {
		t.Fatalf("NSZE=%d want %d", nsze, nvme.DefaultVolumeBlocks)
	}
	// NCAP (8-15) + NUSE (16-23) same as NSZE in T2.
	if got := binary.LittleEndian.Uint64(data[8:16]); got != nsze {
		t.Fatalf("NCAP=%d want %d", got, nsze)
	}
	if got := binary.LittleEndian.Uint64(data[16:24]); got != nsze {
		t.Fatalf("NUSE=%d want %d", got, nsze)
	}
}

func TestT2Batch11a_IdentifyNamespace_NSFEATAndDLFEATZero(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x00, 1)
	// D4: NSFEAT (24) bit 0 (thin-prov/Trim) must be 0.
	if data[24] != 0 {
		t.Fatalf("NSFEAT=0x%02x want 0 (DSM not implemented)", data[24])
	}
	// D5: DLFEAT (28) must be 0 (deallocate not implemented).
	if data[28] != 0 {
		t.Fatalf("DLFEAT=0x%02x want 0", data[28])
	}
}

func TestT2Batch11a_IdentifyNamespace_ANAGRPIDZero(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x00, 1)
	// D2: Identify NS ANAGRPID (92-95) must be 0.
	got := binary.LittleEndian.Uint32(data[92:96])
	if got != 0 {
		t.Fatalf("Identify NS ANAGRPID=%d want 0", got)
	}
}

func TestT2Batch11a_IdentifyNamespace_LBAF0LBADS9(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x00, 1)
	// D9: LBAF[0] (128-131) bits 23:16 = LBADS. For 512 B = 9.
	lbaf0 := binary.LittleEndian.Uint32(data[128:132])
	lbads := uint8((lbaf0 >> 16) & 0xff)
	if lbads != 9 {
		t.Fatalf("LBAF[0] LBADS=%d want 9 (log2 512)", lbads)
	}
}

func TestT2Batch11a_IdentifyNamespace_NGUIDAndEUI64Present(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	_, data := cli.adminIdentify(t, 0x00, 1)
	// NGUID at 104-119.
	nguid := data[104:120]
	if bytes.Equal(nguid, make([]byte, 16)) {
		t.Fatal("NGUID all zero — derivation not running (R1 regression)")
	}
	// EUI-64 at 120-127 must equal NGUID[:8] (R1 pin d).
	eui64 := data[120:128]
	if !bytes.Equal(eui64, nguid[:8]) {
		t.Fatalf("EUI-64 != NGUID[:8]: eui=%x nguid8=%x", eui64, nguid[:8])
	}
}

// -------- Active NS List + NS Descriptor List --------

func TestT2Batch11a_ActiveNSList_SingleNS(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, data := cli.adminIdentify(t, 0x02 /* Active NS List */, 0)
	expectStatusSuccess(t, status, "Active NS List")
	// First entry = NSID 1; rest zero.
	if got := binary.LittleEndian.Uint32(data[0:4]); got != 1 {
		t.Fatalf("first NSID=%d want 1", got)
	}
	if got := binary.LittleEndian.Uint32(data[4:8]); got != 0 {
		t.Fatalf("second NSID=%d want 0 (zero-terminated)", got)
	}
}

func TestT2Batch11a_NSDescriptorList_EmitsNGUID(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()
	status, data := cli.adminIdentify(t, 0x03 /* NS Descriptor List */, 1)
	expectStatusSuccess(t, status, "NS Descriptor List")
	// First descriptor: NIDT=0x02 (NGUID), NIDL=16, 2 bytes
	// reserved, 16 bytes payload.
	if data[0] != 0x02 {
		t.Fatalf("NIDT=0x%02x want 0x02 (NGUID)", data[0])
	}
	if data[1] != 16 {
		t.Fatalf("NIDL=%d want 16", data[1])
	}
	if bytes.Equal(data[4:20], make([]byte, 16)) {
		t.Fatal("NGUID descriptor payload all zero")
	}
}

// -------- Queue routing enforcement --------

// Admin-queue Write must be rejected (admin queue doesn't serve
// IO opcodes). Uses the existing test client's admin conn to
// craft the command directly — writeCmd goes through ioConn
// normally.
func TestT2Batch11a_AdminQueue_RejectsIOOpcode(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()

	// Send Write directly on the admin queue connection.
	cid := uint16(cli.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x01, // ioWrite
		CID:    cid,
		NSID:   1,
	}
	if err := cli.adminW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send admin-queue Write: %v", err)
	}
	resp := recvCapsuleResp(t, cli.adminR)
	// Must be rejected; the admin dispatcher falls through to
	// InvalidOpcode for non-admin opcodes.
	if resp.Status == 0 {
		t.Fatal("admin queue accepted Write opcode — queue routing not enforced")
	}
	sct := (resp.Status >> 9) & 0x7
	sc := (resp.Status >> 1) & 0xff
	if sct != 0x0 || sc != 0x01 {
		t.Fatalf("admin-queue Write status=0x%04x SCT=%d SC=0x%02x want Generic/InvalidOpcode",
			resp.Status, sct, sc)
	}
}

// IO-queue Identify must be rejected (IO queue doesn't serve
// admin opcodes).
func TestT2Batch11a_IOQueue_RejectsAdminOpcode(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()

	cid := uint16(cli.cid.Add(1))
	cmd := nvme.CapsuleCommand{
		OpCode: 0x06, // adminIdentify
		CID:    cid,
	}
	if err := cli.ioW.SendHeaderOnly(0x4, &cmd, 64); err != nil {
		t.Fatalf("send IO-queue Identify: %v", err)
	}
	resp := recvCapsuleResp(t, cli.ioR)
	if resp.Status == 0 {
		t.Fatal("IO queue accepted Identify opcode — queue routing not enforced")
	}
	sct := (resp.Status >> 9) & 0x7
	sc := (resp.Status >> 1) & 0xff
	if sct != 0x0 || sc != 0x01 {
		t.Fatalf("IO-queue Identify status=0x%04x SCT=%d SC=0x%02x want Generic/InvalidOpcode",
			resp.Status, sct, sc)
	}
}

// -------- CNTLID registry lifecycle --------

// IO Connect citing an unknown CNTLID must be rejected with
// ConnectInvalidHost (SC=0x82), distinct from wrong-SubNQN
// (SC=0x80 — ConnectInvalidParameters).
func TestT2Batch11a_IOConnect_UnknownCNTLID_Rejected(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1", ReplicaID: "r1"})
	prov := testback.NewStaticProvider(rec)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tg.Close()

	// Open a raw IO-queue connection WITHOUT a preceding admin
	// Connect, citing an invented CNTLID.
	conn := dialAndHandshake(t, addr)
	defer conn.Close()
	r := nvme.NewReader(conn)
	w := nvme.NewWriter(conn)

	cmd := nvme.CapsuleCommand{
		OpCode: 0x7F,
		FCType: 0x01,
		CID:    1,
		D10:    uint32(1) << 16, // QID=1 (IO)
	}
	cd := nvme.ConnectData{
		CNTLID:  9999, // never allocated
		SubNQN:  "nqn.2026-04.example.v3:subsys",
		HostNQN: "nqn.2026-04.example.host:1",
	}
	cdBuf := make([]byte, 1024)
	cd.Marshal(cdBuf)
	if err := w.SendWithData(0x4, 0, &cmd, 64, cdBuf); err != nil {
		t.Fatalf("send rogue IO Connect: %v", err)
	}
	resp := recvCapsuleResp(t, r)
	if resp.Status == 0 {
		t.Fatal("unknown-CNTLID IO Connect accepted; CNTLID validation not enforced")
	}
	sct := (resp.Status >> 9) & 0x7
	sc := (resp.Status >> 1) & 0xff
	// ConnectInvalidHost = SCT 1 (CommandSpecific) / SC 0x82.
	if sct != 1 || sc != 0x82 {
		t.Fatalf("unknown-CNTLID status=0x%04x SCT=%d SC=0x%02x want 1/0x82 (ConnectInvalidHost)",
			resp.Status, sct, sc)
	}
}

// Admin Connect with a preset CNTLID (not 0xFFFF and not 0x0000)
// must be rejected with ConnectInvalidParameters.
func TestT2Batch11a_AdminConnect_PresetCNTLID_Rejected(t *testing.T) {
	rec := testback.NewRecordingBackend(frontend.Identity{VolumeID: "v1"})
	prov := testback.NewStaticProvider(rec)
	tg := nvme.NewTarget(nvme.TargetConfig{
		Listen:    "127.0.0.1:0",
		SubsysNQN: "nqn.2026-04.example.v3:subsys",
		VolumeID:  "v1",
		Provider:  prov,
	})
	addr, err := tg.Start()
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer tg.Close()

	conn := dialAndHandshake(t, addr)
	defer conn.Close()
	r := nvme.NewReader(conn)
	w := nvme.NewWriter(conn)

	cmd := nvme.CapsuleCommand{
		OpCode: 0x7F,
		FCType: 0x01,
		CID:    1,
		D10:    uint32(0) << 16, // QID=0 (admin)
	}
	cd := nvme.ConnectData{
		CNTLID:  42, // invalid preset
		SubNQN:  "nqn.2026-04.example.v3:subsys",
		HostNQN: "nqn.2026-04.example.host:1",
	}
	cdBuf := make([]byte, 1024)
	cd.Marshal(cdBuf)
	_ = w.SendWithData(0x4, 0, &cmd, 64, cdBuf)
	resp := recvCapsuleResp(t, r)
	if resp.Status == 0 {
		t.Fatal("admin Connect with preset CNTLID=42 accepted")
	}
	sct := (resp.Status >> 9) & 0x7
	sc := (resp.Status >> 1) & 0xff
	if sct != 1 || sc != 0x80 {
		t.Fatalf("admin-preset-CNTLID status=0x%04x SCT=%d SC=0x%02x want 1/0x80 (ConnectInvalidParameters)",
			resp.Status, sct, sc)
	}
}

// Admin + IO round-trip end-to-end: admin Connect → Identify
// all CNS variants → IO Connect → Write/Read.
func TestT2Route_NVMe_AdminIdentifyPlusIORoundTrip(t *testing.T) {
	tg, _, cli := newIdentifyHarness(t)
	defer tg.Close()
	defer cli.close()

	// Identify Controller / Namespace / Active NS / NSDescList.
	for _, cns := range []uint8{0x01, 0x00, 0x02, 0x03} {
		var nsid uint32
		if cns == 0x00 || cns == 0x03 {
			nsid = 1
		}
		status, data := cli.adminIdentify(t, cns, nsid)
		if status != 0 {
			t.Fatalf("Identify CNS 0x%02x: status=0x%04x", cns, status)
		}
		if len(data) != 4096 {
			t.Fatalf("Identify CNS 0x%02x: len=%d", cns, len(data))
		}
	}

	// IO Write → IO Read round-trip on the same CNTLID's IO queue.
	payload := make([]byte, nvme.DefaultBlockSize)
	copy(payload, []byte("batch11a-endtoend"))
	status := cli.writeCmd(t, 0, 1, payload)
	expectStatusSuccess(t, status, "IO Write post-Identify")
	status, data := cli.readCmd(t, 0, 1, int(nvme.DefaultBlockSize))
	expectStatusSuccess(t, status, "IO Read post-Identify")
	if !bytes.Equal(data, payload) {
		t.Fatal("end-to-end round-trip mismatch")
	}
}
