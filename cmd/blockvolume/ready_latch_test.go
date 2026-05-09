package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweed-block/core/adapter"
	"github.com/seaweedfs/seaweed-block/core/frontend"
)

type recordingPrimaryOpenProvider struct {
	volumeID string
	calls    int
	err      error
}

func (p *recordingPrimaryOpenProvider) Open(_ context.Context, volumeID string) (frontend.Backend, error) {
	p.calls++
	p.volumeID = volumeID
	return nil, p.err
}

func TestReadyAssignment_PrimaryEnsuresDurableLineageAndPrintsReadyLine(t *testing.T) {
	prov := &recordingPrimaryOpenProvider{}
	var stdout, stderr bytes.Buffer

	handleReadyAssignment(adapter.AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
	}, flags{
		volumeID: "v1", replicaID: "r2", printReadyLine: true,
	}, prov, &stdout, &stderr)

	if prov.calls != 1 || prov.volumeID != "v1" {
		t.Fatalf("Open calls=%d volume=%q, want 1/v1", prov.calls, prov.volumeID)
	}
	if got := stdout.String(); !strings.Contains(got, `"phase":"assignment-received"`) ||
		!strings.Contains(got, `"epoch":2`) {
		t.Fatalf("ready line missing assignment fields: %s", got)
	}
	if got := stderr.String(); !strings.Contains(got, "durable primary lineage ensured volume=v1 replica=r2 epoch=2 ev=1") {
		t.Fatalf("ensure log mismatch: %s", got)
	}
}

func TestReadyAssignment_IgnoresOtherReplica(t *testing.T) {
	prov := &recordingPrimaryOpenProvider{}

	handleReadyAssignment(adapter.AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r1", Epoch: 2, EndpointVersion: 1,
	}, flags{
		volumeID: "v1", replicaID: "r2",
	}, prov, nil, nil)

	if prov.calls != 0 {
		t.Fatalf("Open called for other replica: %d", prov.calls)
	}
}

func TestReadyAssignment_ReportsDurableEnsureFailure(t *testing.T) {
	prov := &recordingPrimaryOpenProvider{err: errors.New("boom")}
	var stderr bytes.Buffer

	handleReadyAssignment(adapter.AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
	}, flags{
		volumeID: "v1", replicaID: "r2",
	}, prov, nil, &stderr)

	if prov.calls != 1 {
		t.Fatalf("Open calls=%d, want 1", prov.calls)
	}
	if got := stderr.String(); !strings.Contains(got, "durable primary lineage ensure failed") ||
		!strings.Contains(got, "boom") {
		t.Fatalf("failure log mismatch: %s", got)
	}
}
