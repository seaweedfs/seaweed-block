package ublk

import (
	"testing"
	"unsafe"
)

// The kernel reads these structs byte for byte: their sizes and the
// command encodings must match include/uapi/linux/ublk_cmd.h exactly.
func TestUAPI_StructLayouts(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"ublksrv_ctrl_cmd", unsafe.Sizeof(ctrlCmd{}), 32},
		{"ublksrv_ctrl_dev_info", unsafe.Sizeof(devInfo{}), 64},
		{"ublksrv_io_desc", unsafe.Sizeof(ioDesc{}), 24},
		{"ublksrv_io_cmd", unsafe.Sizeof(ioCmd{}), 16},
		{"ublk_param_basic", unsafe.Sizeof(paramBasic{}), 32},
		{"ublk_params header+basic", unsafe.Sizeof(params{}), 40},
	} {
		if tc.got != tc.want {
			t.Errorf("sizeof(%s) = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
	for _, tc := range []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"ctrl_cmd.addr", unsafe.Offsetof(ctrlCmd{}.Addr), 8},
		{"ctrl_cmd.data", unsafe.Offsetof(ctrlCmd{}.Data), 16},
		{"dev_info.max_io_buf_bytes", unsafe.Offsetof(devInfo{}.MaxIOBufBytes), 8},
		{"dev_info.flags", unsafe.Offsetof(devInfo{}.Flags), 24},
		{"io_desc.start_sector", unsafe.Offsetof(ioDesc{}.StartSector), 8},
		{"io_cmd.addr", unsafe.Offsetof(ioCmd{}.Addr), 8},
		{"param_basic.max_sectors", unsafe.Offsetof(paramBasic{}.MaxSectors), 8},
		{"param_basic.dev_sectors", unsafe.Offsetof(paramBasic{}.DevSectors), 16},
	} {
		if tc.got != tc.want {
			t.Errorf("offsetof(%s) = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}

func TestUAPI_CommandEncodings(t *testing.T) {
	// _IOWR('u', nr, struct ublksrv_ctrl_cmd) and friends, computed from
	// the kernel's macro definition.
	for _, tc := range []struct {
		name      string
		got, want uint32
	}{
		{"UBLK_U_CMD_GET_DEV_INFO", uCmdGetDevInfo, 0x80207502},
		{"UBLK_U_CMD_ADD_DEV", uCmdAddDev, 0xc0207504},
		{"UBLK_U_CMD_DEL_DEV", uCmdDelDev, 0xc0207505},
		{"UBLK_U_CMD_START_DEV", uCmdStartDev, 0xc0207506},
		{"UBLK_U_CMD_STOP_DEV", uCmdStopDev, 0xc0207507},
		{"UBLK_U_CMD_SET_PARAMS", uCmdSetParams, 0xc0207508},
		{"UBLK_U_IO_FETCH_REQ", uIOFetchReq, 0xc0107520},
		{"UBLK_U_IO_COMMIT_AND_FETCH_REQ", uIOCommitAndFetchReq, 0xc0107521},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %#x, want %#x", tc.name, tc.got, tc.want)
		}
	}
	if got := queueDescOffset(2); got != 2*4096*24 {
		t.Errorf("queue 2 descriptor offset %d", got)
	}
}

func TestUAPI_DescriptorToRequest(t *testing.T) {
	d := ioDesc{OpFlags: ioOpWrite | ioFlagFUA, NrSectors: 8, StartSector: 100}
	r := d.request()
	if r.op != ioOpWrite || r.flags&ioFlagFUA == 0 || r.nrSectors != 8 || r.startSector != 100 {
		t.Fatalf("request %+v from %+v", r, d)
	}
}
