package csi

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/rpc/control"
)

// INV-AUTH-HOLDER-IDENTITY-001: with replica-tagged frontends, a target is published only when the primary replica has a
// usable frontend; another replica's frontend is at most an extra path to the primary's own target. Untagged responses
// (an older master) keep the previous first-usable behaviour.
func TestPublishTargetIsPrimaryOwned(t *testing.T) {
	ft := func(replica, addr, iqn string) *control.FrontendTarget {
		return &control.FrontendTarget{Protocol: string(ProtocolISCSI), Addr: addr, Iqn: iqn, ReplicaId: replica}
	}
	status := func(fronts ...*control.FrontendTarget) *control.StatusResponse {
		return &control.StatusResponse{VolumeId: "v1", Assigned: true, ReplicaId: "r1", Epoch: 3, Frontends: fronts}
	}
	// the primary's only frontend is one CSI rejects (loopback): the supporting replica's must not stand in
	if got, ok := publishTargetFromStatus(status(ft("r1", "127.0.0.1:3260", "iqn.v1"), ft("r2", "192.0.2.2:3260", "iqn.v1")), false, true); ok {
		t.Fatalf("rejected primary frontend: published %+v, want no target", got)
	}
	if got, ok := publishTargetFromStatus(status(ft("r1", "127.0.0.1:3260", "iqn.v1"), ft("r2", "192.0.2.2:3260", "iqn.v1")), true, true); ok {
		t.Fatalf("rejected primary frontend (multipath): published %+v, want no target", got)
	}
	// primary first; a supporting path to the SAME target joins only as a multipath path
	got, ok := publishTargetFromStatus(status(ft("r2", "192.0.2.2:3260", "iqn.v1"), ft("r1", "192.0.2.1:3260", "iqn.v1")), true, false)
	if !ok || got.ISCSIAddr != "192.0.2.1:3260" || !got.Multipath || len(got.ISCSIAddrs) != 2 || got.ISCSIAddrs[0] != "192.0.2.1:3260" {
		t.Fatalf("multipath: %+v ok=%v, want the primary first with the supporting path", got, ok)
	}
	// a supporting frontend of ANOTHER target never joins
	got, ok = publishTargetFromStatus(status(ft("r1", "192.0.2.1:3260", "iqn.v1"), ft("r2", "192.0.2.2:3260", "iqn.other")), true, false)
	if !ok || got.ISCSIAddr != "192.0.2.1:3260" || got.Multipath || got.IQN != "iqn.v1" {
		t.Fatalf("foreign target: %+v ok=%v, want only the primary's", got, ok)
	}
	// iSCSI paths match on IQN AND LUN (codex01): a supporting q/LUN2 is another target, never a path to the primary's q/LUN1
	lun := func(replica, addr string, n uint32) *control.FrontendTarget {
		return &control.FrontendTarget{Protocol: string(ProtocolISCSI), Addr: addr, Iqn: "iqn.q", Lun: n, ReplicaId: replica}
	}
	got, ok = publishTargetFromStatus(status(lun("r1", "192.0.2.1:3260", 1), lun("r2", "192.0.2.2:3260", 2)), true, false)
	if !ok || got.ISCSIAddr != "192.0.2.1:3260" || got.LUN != 1 || got.Multipath || len(got.ISCSIAddrs) != 0 {
		t.Fatalf("LUN mismatch: %+v ok=%v, want only the primary's q/LUN1 (no multipath)", got, ok)
	}
	got, ok = publishTargetFromStatus(status(lun("r1", "192.0.2.1:3260", 1), lun("r2", "192.0.2.2:3260", 1)), true, false)
	if !ok || got.LUN != 1 || !got.Multipath || len(got.ISCSIAddrs) != 2 || got.ISCSIAddrs[0] != "192.0.2.1:3260" {
		t.Fatalf("nonzero LUN match: %+v ok=%v, want multipath primary-first at LUN 1", got, ok)
	}
	// two PRIMARY-owned units of one IQN (q/LUN1 at A, q/LUN2 at B) are two targets, never one multipath target with
	// LUN1 for both (codex01: the final grouping binds IQN and LUN too)
	got, ok = publishTargetFromStatus(status(lun("r1", "192.0.2.1:3260", 1), lun("r1", "192.0.2.2:3260", 2)), true, false)
	if !ok || got.ISCSIAddr != "192.0.2.1:3260" || got.LUN != 1 || got.Multipath || len(got.ISCSIAddrs) != 0 {
		t.Fatalf("two primary units: %+v ok=%v, want q/LUN1 at 192.0.2.1 alone (no multipath with LUN2's path)", got, ok)
	}
	got, ok = publishTargetFromStatus(status(lun("", "192.0.2.1:3260", 1), lun("", "192.0.2.2:3260", 2)), true, false)
	if !ok || got.LUN != 1 || got.Multipath {
		t.Fatalf("untagged two units: %+v ok=%v, want q/LUN1 alone", got, ok)
	}
	// no primary frontend at all, supporting present: no target
	if got, ok := publishTargetFromStatus(status(ft("r2", "192.0.2.2:3260", "iqn.v1")), false, false); ok {
		t.Fatalf("no primary frontend: published %+v, want no target", got)
	}
	// untagged (older master): unchanged first-usable selection
	got, ok = publishTargetFromStatus(status(ft("", "192.0.2.2:3260", "iqn.v1")), false, false)
	if !ok || got.ISCSIAddr != "192.0.2.2:3260" {
		t.Fatalf("untagged: %+v ok=%v, want the first usable frontend", got, ok)
	}
}
