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

type recordingLatchProvider struct {
	recordingPrimaryOpenProvider
	latchVolumeID string
	latchCalls    int
	latchChanged  bool
	latchErr      error
}

func (p *recordingLatchProvider) LatchVolumeIdentity(volumeID string) (bool, error) {
	p.latchCalls++
	p.latchVolumeID = volumeID
	return p.latchChanged, p.latchErr
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

func TestReadyAssignment_UsesDurableIdentityLatchWhenAvailable(t *testing.T) {
	prov := &recordingLatchProvider{latchChanged: true}
	var stderr bytes.Buffer

	handleReadyAssignment(adapter.AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
	}, flags{
		volumeID: "v1", replicaID: "r2",
	}, prov, nil, &stderr)

	if prov.latchCalls != 1 || prov.latchVolumeID != "v1" {
		t.Fatalf("LatchVolumeIdentity calls=%d volume=%q, want 1/v1", prov.latchCalls, prov.latchVolumeID)
	}
	if prov.calls != 0 {
		t.Fatalf("Open must not be called when durable identity latch is available: %d", prov.calls)
	}
	if got := stderr.String(); !strings.Contains(got, "durable lineage latched volume=v1 replica=r2 epoch=2 ev=1 changed=true") {
		t.Fatalf("latch log mismatch: %s", got)
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

func TestReadyAssignment_IgnoresTypedNilProvider(t *testing.T) {
	var prov *recordingPrimaryOpenProvider

	handleReadyAssignment(adapter.AssignmentInfo{
		VolumeID: "v1", ReplicaID: "r2", Epoch: 2, EndpointVersion: 1,
	}, flags{
		volumeID: "v1", replicaID: "r2",
	}, prov, nil, nil)
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
