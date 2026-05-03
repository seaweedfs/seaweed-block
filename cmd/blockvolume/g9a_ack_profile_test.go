package main

import (
	"testing"

	"github.com/seaweedfs/seaweed-block/core/frontend/durable"
	"github.com/seaweedfs/seaweed-block/core/replication"
)

func TestG9A_ParseReplicationAckProfile(t *testing.T) {
	tests := []struct {
		name      string
		profile   string
		wantMode  replication.DurabilityMode
		wantWrite durable.WriteAckPolicy
		wantErr   bool
	}{
		{
			name:      "best effort default",
			profile:   "best-effort",
			wantMode:  replication.DurabilityBestEffort,
			wantWrite: durable.WriteAckBestEffort,
		},
		{
			name:      "sync quorum gates foreground writes",
			profile:   "sync-quorum",
			wantMode:  replication.DurabilitySyncQuorum,
			wantWrite: durable.WriteAckRequireObserverAck,
		},
		{
			name:      "sync all gates foreground writes",
			profile:   "sync-all",
			wantMode:  replication.DurabilitySyncAll,
			wantWrite: durable.WriteAckRequireObserverAck,
		},
		{
			name:    "unknown profile rejected",
			profile: "full-sync-ish",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotMode, gotWrite, err := parseReplicationAckProfile(tt.profile)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseReplicationAckProfile(%q): expected error", tt.profile)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseReplicationAckProfile(%q): %v", tt.profile, err)
			}
			if gotMode != tt.wantMode {
				t.Fatalf("mode=%v want %v", gotMode, tt.wantMode)
			}
			if gotWrite != tt.wantWrite {
				t.Fatalf("writeAck=%v want %v", gotWrite, tt.wantWrite)
			}
		})
	}
}
