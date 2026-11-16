package ublk

import "unsafe"

// Kernel ABI of the ublk driver (include/uapi/linux/ublk_cmd.h). The
// struct layouts and ioctl encodings are checked by uapi_test.go.

// Control commands (/dev/ublk-control), ioctl-encoded (UBLK_U_CMD_*).
const (
	cmdGetDevInfo  = 0x02
	cmdAddDev      = 0x04
	cmdDelDev      = 0x05
	cmdStartDev    = 0x06
	cmdStopDev     = 0x07
	cmdSetParams   = 0x08
	ioFetchReq     = 0x20
	ioCommitAndRef = 0x21
)

// Device flags (UBLK_F_*).
const (
	flagCmdIoctlEncode = 1 << 6
)

// Fetch/commit CQE results.
const (
	ioResOK    = 0
	ioResAbort = -19 // -ENODEV: the queue is being torn down; no re-fetch
)

// Parameter types and basic attributes.
const (
	paramTypeBasic     = 1 << 0
	attrVolatileCache  = 1 << 2
	attrFUA            = 1 << 3
	maxQueueDepth      = 4096 // UBLK_MAX_QUEUE_DEPTH
	ioDescSize         = 24   // sizeof(struct ublksrv_io_desc)
	cmdBufOffset       = 0    // UBLKSRV_CMD_BUF_OFFSET
	ctrlDevicePath     = "/dev/ublk-control"
	charDevicePrefix   = "/dev/ublkc"
	blockDevicePrefix  = "/dev/ublkb"
	devIDAuto          = ^uint32(0)
	queueIDNone        = ^uint16(0)
	maxIOBufBytesLimit = 1 << 25 // single IO buffer max (UBLK_IO_BUF_BITS)
)

// ctrlCmd is struct ublksrv_ctrl_cmd (32 bytes), carried in sqe->cmd.
type ctrlCmd struct {
	DevID      uint32
	QueueID    uint16
	Len        uint16
	Addr       uint64
	Data       uint64
	DevPathLen uint16
	Pad        uint16
	Reserved   uint32
}

// devInfo is struct ublksrv_ctrl_dev_info (64 bytes).
type devInfo struct {
	NrHWQueues    uint16
	QueueDepth    uint16
	State         uint16
	Pad0          uint16
	MaxIOBufBytes uint32
	DevID         uint32
	SrvPID        int32
	Pad1          uint32
	Flags         uint64
	SrvFlags      uint64
	OwnerUID      uint32
	OwnerGID      uint32
	Reserved1     uint64
	Reserved2     uint64
}

// ioDesc is struct ublksrv_io_desc (24 bytes), in the mmap'd descriptor
// array of /dev/ublkcN, indexed by tag.
type ioDesc struct {
	OpFlags     uint32
	NrSectors   uint32
	StartSector uint64
	Addr        uint64
}

func (d ioDesc) request() request {
	return request{op: uint8(d.OpFlags & 0xff), flags: d.OpFlags &^ 0xff, startSector: d.StartSector, nrSectors: d.NrSectors}
}

// ioCmd is struct ublksrv_io_cmd (16 bytes), carried in sqe->cmd.
type ioCmd struct {
	QID    uint16
	Tag    uint16
	Result int32
	Addr   uint64
}

// paramBasic is struct ublk_param_basic (32 bytes).
type paramBasic struct {
	Attrs            uint32
	LogicalBSShift   uint8
	PhysicalBSShift  uint8
	IOOptShift       uint8
	IOMinShift       uint8
	MaxSectors       uint32
	ChunkSectors     uint32
	DevSectors       uint64
	VirtBoundaryMask uint64
}

// params is the leading part of struct ublk_params that this server
// sets: len, types and the basic parameters (len names exactly this much).
type params struct {
	Len   uint32
	Types uint32
	Basic paramBasic
}

// bytesOf views a fixed-size struct as bytes (an inline command or a
// command buffer the kernel reads or fills).
func bytesOf[T any](v *T) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(v)), unsafe.Sizeof(*v))
}

// iowr and ior encode a ublk command number like the kernel's _IOWR and
// _IOR('u', nr, size).
func iowr(nr, size uint32) uint32 { return 3<<30 | size<<16 | uint32('u')<<8 | nr }
func ior(nr, size uint32) uint32  { return 2<<30 | size<<16 | uint32('u')<<8 | nr }

const (
	ctrlCmdSize = 32
	ioCmdSize   = 16
)

var (
	uCmdGetDevInfo       = ior(cmdGetDevInfo, ctrlCmdSize)
	uCmdAddDev           = iowr(cmdAddDev, ctrlCmdSize)
	uCmdDelDev           = iowr(cmdDelDev, ctrlCmdSize)
	uCmdStartDev         = iowr(cmdStartDev, ctrlCmdSize)
	uCmdStopDev          = iowr(cmdStopDev, ctrlCmdSize)
	uCmdSetParams        = iowr(cmdSetParams, ctrlCmdSize)
	uIOFetchReq          = iowr(ioFetchReq, ioCmdSize)
	uIOCommitAndFetchReq = iowr(ioCommitAndRef, ioCmdSize)
)

// queueDescOffset is the mmap offset of queue q's descriptor array.
func queueDescOffset(q int) int64 {
	return cmdBufOffset + int64(q)*maxQueueDepth*ioDescSize
}
