package lifecycleclient

import (
	"context"
	"testing"

	control "github.com/seaweedfs/seaweed-block/core/rpc/control"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type resolverProbe struct {
	reply *control.CreateVolumeResponse
	err   error
	calls int
}

func (p *resolverProbe) GetVolume(_ context.Context, q *control.GetLifecycleVolumeRequest, _ ...grpc.CallOption) (*control.CreateVolumeResponse, error) {
	p.calls++
	if q.GetVolumeId() != "v" {
		panic("wrong name")
	}
	return p.reply, p.err
}

func TestResolveVolumeNeverInventsAnIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, volume, id string
		err              error
		good             bool
	}{
		{"valid", "v", "1234567890abcdef1234567890abcdef", nil, true},
		{"empty", "v", "", nil, false},
		{"malformed", "v", "bad", nil, false},
		{"wrong-name", "other", "1234567890abcdef1234567890abcdef", nil, false},
		{"missing", "", "", status.Error(codes.NotFound, "missing"), false},
		{"transport", "", "", status.Error(codes.Unavailable, "unavailable"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &resolverProbe{reply: &control.CreateVolumeResponse{VolumeId: tc.volume, LayoutId: tc.id}, err: tc.err}
			id, err := ResolveVolume(context.Background(), p, "v")
			if p.calls != 1 || (err == nil) != tc.good || (!tc.good && !id.IsZero()) {
				t.Fatalf("calls=%d id=%s err=%v", p.calls, id, err)
			}
		})
	}
}
