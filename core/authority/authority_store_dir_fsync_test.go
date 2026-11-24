package authority

import (
	"errors"
	"strings"
	"testing"
)

// A record's rename is durable only after the directory fsync: a Put whose
// directory fsync fails must fail (fail closed), for every record, not only
// for workspace (lease) records. Otherwise the caller believes a write that
// a crash can undo.
func TestPutFailsWhenTheDirectoryFsyncFails(t *testing.T) {
	for _, c := range []struct {
		name string
		rec  DurableRecord
	}{
		{"assignment record", DurableRecord{VolumeID: "v1", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d", CtrlAddr: "c", WriteSeq: 1}},
		{"workspace record", DurableRecord{VolumeID: "v2", ReplicaID: "r1", Epoch: 1, EndpointVersion: 1, DataAddr: "d", CtrlAddr: "c", WriteSeq: 1,
			Workspace: &WorkspaceState{Lease: &WorkspaceLease{ReplicaID: "r1", Epoch: 1}}}},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, err := NewFileAuthorityStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			SetDirSyncFaultForTest(func() error { return errors.New("injected directory fsync failure") })
			defer SetDirSyncFaultForTest(nil)
			err = s.Put(c.rec)
			if err == nil || !strings.Contains(err.Error(), "injected directory fsync failure") {
				t.Fatalf("Put with a failed directory fsync: %v; want it to fail", err)
			}
		})
	}
}
