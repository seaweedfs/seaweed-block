package nvme

// Per-controller register state for NVMe-oF admin queue
// (Batch 11b — port plan §7, §3.1 A16).
//
// V3 does NOT port V2's controller.go wholesale: V2 has admin
// session registry, storage/subsystem plumbing, quiesce races,
// and multi-subsystem policy. T2 scope is one subsystem per
// Target + one admin controller per admin-queue Connect.
// This file introduces the minimum register state Linux
// nvme-cli needs to complete connect:
//
//   CAP  (Controller Capabilities, 64-bit, read-only)
//   VS   (Version, 32-bit, read-only — NVMe 1.3 per D11)
//   CC   (Controller Configuration, 32-bit, writable)
//   CSTS (Controller Status, 32-bit, read-only, derived from CC)
//
// Linux sequence on connect:
//   1. PropertyGet(CAP)  — read capabilities
//   2. PropertyGet(VS)   — confirm version
//   3. PropertySet(CC, EN=0)  — quiesce first (some drivers)
//   4. PropertyGet(CSTS)      — expect RDY=0
//   5. PropertySet(CC, EN=1)  — enable controller
//   6. Poll PropertyGet(CSTS) until RDY=1 (Linux 1s timeout)
//   7. Identify / Set Features / IO queue Connect...
//
// Per QA finding testing-#3 (2026-04-22): CC.EN→CSTS.RDY flip
// MUST happen within 1s. T2 flips synchronously (same
// PropertySet dispatch) — no timer, no delay — so any
// PropertyGet(CSTS) after PropertySet(CC) sees the flipped
// value immediately. This is the "deterministic latency upper
// bound" the plan requires.

import (
	"sync"
)

// adminController is the per-(admin-queue-Connect) state
// allocated when a host issues Fabric Connect with QID=0.
//
// 11a introduced cntlID + identity fields. 11b adds:
//   - register state (CAP/VS/CC/CSTS)
//   - granted IO queue count (Set Features 0x07)
//   - KATO timeout in 100ms units (Set Features 0x0F)
//   - single-slot pending AER tracking (QA finding testing-#2)
//
// Accessors (setCC / cstsReady / etc.) are methods so the
// CC→CSTS causality is enforced in one place.
type adminController struct {
	cntlID   uint16
	subNQN   string
	hostNQN  string
	volumeID string // pinned for Identify NGUID/Serial derivation (R1)

	// Register state. Guarded by mu so concurrent
	// PropertyGet/Set from different sessions see a consistent
	// snapshot. (In practice T2 only has one admin session per
	// controller so contention is zero, but the mutex costs
	// nothing and closes the door on future parallel admin
	// queues.)
	mu   sync.Mutex
	cap  uint64 // Controller Capabilities
	vs   uint32 // Version (NVMe 1.3)
	cc   uint32 // Controller Configuration (written by host)
	csts uint32 // Controller Status (derived from cc)

	// Set Features state (11b A11/A12). Values the host asked
	// for; Get Features returns the same values back.
	numQueues uint32 // DW0 response for Feature 0x07; low 16 = NSQR, high 16 = NCQR
	kato      uint32 // Keep Alive Timeout, milliseconds

	// Single-slot pending AER (QA finding testing-#2, AERL=0
	// means capacity 1). pendingAERCID != 0 means an AER is
	// parked waiting for an event that T2 never produces.
	// Identified by the initiator's CID so the response (if
	// ever emitted in 11c+) matches.
	pendingAERCID    uint16
	pendingAERHasCID bool // distinguishes CID 0 (valid) from "no pending"
}

// newAdminController constructs a controller in its boot state:
// CC=0, CSTS=0, CAP/VS set to the T2-advertised values.
func newAdminController(id uint16, subNQN, hostNQN, volumeID string) *adminController {
	return &adminController{
		cntlID:   id,
		subNQN:   subNQN,
		hostNQN:  hostNQN,
		volumeID: volumeID,
		cap:      defaultCAP(),
		vs:       nvmeVersion13,
		cc:       0,
		csts:     0,
	}
}

// defaultCAP builds the Controller Capabilities register.
// Layout per NVMe 1.3 §3.1.1:
//   bits  0:15  MQES   — Max Queue Entries Supported (zero-based)
//   bit   16    CQR    — Contiguous Queues Required (1 per TCP transport)
//   bits 17:18  AMS    — Arbitration Mechanism Supported (0 = Round Robin only)
//   bits 19:23  reserved
//   bits 24:31  TO     — Timeout, 500ms units (time until CSTS.RDY flips)
//   bits 32:35  DSTRD  — Doorbell Stride (0 = 4-byte; not used over TCP)
//   bit   36    NSSRS  — NVM Subsystem Reset Supported (0 in T2)
//   bits 37:44  CSS    — Command Sets Supported (bit 37 = NVM command set)
//   bit   45    BPS    — Boot Partition Support (0)
//   bits 46:47  reserved
//   bits 48:51  MPSMIN — Min Memory Page Size (0 = 4 KiB)
//   bits 52:55  MPSMAX — Max Memory Page Size (0 = 4 KiB)
//   bits 56:63  reserved (PMR/CMB flags on NVMe 1.4+)
//
// We set:
//   MQES = 63  (64 entries per queue — matches Identify MAXCMD=64)
//   CQR  = 1   (required for NVMe/TCP)
//   TO   = 2   (1 second — matches Linux kernel's 1s CC.EN→RDY poll
//              per QA finding testing-#3; keeps upper bound explicit)
//   CSS  bit 0 = 1 (NVM command set)
//   everything else = 0
func defaultCAP() uint64 {
	var cap uint64
	cap |= uint64(63) // MQES
	cap |= 1 << 16    // CQR
	cap |= 2 << 24    // TO = 1 second
	cap |= 1 << 37    // CSS bit 0 = NVM
	return cap
}

// getCAP returns the CAP register value.
func (c *adminController) getCAP() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cap
}

// getVS returns the VS register value.
func (c *adminController) getVS() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.vs
}

// getCC returns the CC register value.
func (c *adminController) getCC() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cc
}

// getCSTS returns the CSTS register value.
func (c *adminController) getCSTS() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.csts
}

// setCC writes a new CC value and updates CSTS synchronously.
// Per QA finding testing-#3 the CC.EN→CSTS.RDY flip must be
// deterministic within Linux's 1s timeout; doing it inside the
// same mutex window guarantees the next getCSTS observes the
// flip — latency is "whatever it takes for one wire round-trip
// between PropertySet and PropertyGet", well under 1s on
// loopback or any reasonable network.
//
// Returns the new CSTS for tests that want to assert the
// derivation.
func (c *adminController) setCC(newCC uint32) uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	prevEN := c.cc & 0x01
	newEN := newCC & 0x01
	c.cc = newCC

	// CSTS layout per NVMe 1.3 §3.1.6:
	//   bit 0  RDY — Ready
	//   bit 1  CFS — Controller Fatal Status
	//   bits 2:3 SHST — Shutdown Status
	//   bits 4-  reserved
	//
	// T2 flips RDY in lockstep with CC.EN. Shutdown handling
	// (SHST) is not in 11b scope — CC.SHN triggers a clean
	// quiesce in V2 but T2 just clears RDY and leaves SHST=0.
	// That's enough for Linux's 1s RDY poll + clean disconnect;
	// a real shutdown sequence can be folded in later if m01
	// exposes a need (Discovery Bridge).
	if newEN == 1 {
		c.csts |= 0x01 // set RDY
	} else {
		c.csts &^= 0x01 // clear RDY
	}
	_ = prevEN // kept for future state-transition logging
	return c.csts
}

// setNumQueues records the granted IO queue count for Get
// Features (Feature 0x07) later. The wire format packs
// NSQR (0:15) + NCQR (16:31) into a single uint32.
func (c *adminController) setNumQueues(v uint32) {
	c.mu.Lock()
	c.numQueues = v
	c.mu.Unlock()
}

// getNumQueues returns what Set Features 0x07 was granted.
func (c *adminController) getNumQueues() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.numQueues
}

// setKATO records the Keep Alive Timeout (ms) the host asked
// for. Per QA finding testing-#2: 11b stores it for Get
// Features round-trip but does NOT arm a timer or enforce
// shutdown on missed KeepAlive. A real timer lands with T6 or
// later if m01 exposes a need (Discovery Bridge).
func (c *adminController) setKATO(ms uint32) {
	c.mu.Lock()
	c.kato = ms
	c.mu.Unlock()
}

// getKATO returns the stored KATO for Get Features round-trip.
func (c *adminController) getKATO() uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.kato
}

// tryParkAER tries to store cid as a pending AER. Returns true
// if the single slot was free (AER accepted — handler MUST NOT
// emit a CapsuleResp), false if the slot is full (handler
// rejects with AER-limit-exceeded).
//
// T2 never produces an event, so parked AERs never complete.
// Cleared on admin session close so the slot frees up when
// the host disconnects.
func (c *adminController) tryParkAER(cid uint16) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingAERHasCID {
		return false
	}
	c.pendingAERCID = cid
	c.pendingAERHasCID = true
	return true
}

// clearPendingAER is called on admin-session close.
func (c *adminController) clearPendingAER() {
	c.mu.Lock()
	c.pendingAERCID = 0
	c.pendingAERHasCID = false
	c.mu.Unlock()
}
