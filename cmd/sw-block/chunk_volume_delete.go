package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore"
	"github.com/seaweedfs/seaweed-block/core/rpc/control"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)

// chunkVolumeDelete permanently reserves deletion at the master, then asks
// that master for the stable no-live-lease proof before the HEAD CAS.
// A refused or interrupted delete keeps its reservation for a later rerun.
func chunkVolumeDelete(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("volume-delete", time.Minute, stdout, stderr)
	var vol volumeFlags
	var requestID string
	c.bindVolume(&vol, "volume", "volume")
	c.fs.StringVar(&requestID, "request-id", "", "this delete's operation id; a rerun with the same id resumes its admission")
	var ref chunkstore.VolumeRef
	return c.run(args, true, func() (err error) {
		if c.master == "" || requestID == "" || c.timeout <= 0 {
			return errors.New("--master and --request-id are required; --timeout must be positive")
		}
		ref, err = vol.ref("volume")
		return err
	}, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		conn, err := link.Dial(c.master)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		auth := &masterAuthority{lc: control.NewLifecycleServiceClient(conn), token: requestID, vol: ref.Name}
		if err := cat.DeleteVolume(ctx, ref, auth); err != nil {
			return nil, err
		}
		return map[string]string{"volume": ref.Dir(), "state": "deleted"}, nil
	})
}

// masterAuthority forwards both facts to the owner of the durable reservation
// and lease horizons. A successful reservation alone is never a lease proof.
type masterAuthority struct {
	lc         control.LifecycleServiceClient
	token, vol string
}

func (m *masterAuthority) req() *control.MarkVolumeDeletingRequest {
	return &control.MarkVolumeDeletingRequest{VolumeId: m.vol, Token: m.token}
}
func (m *masterAuthority) Reserve(ctx context.Context) error {
	_, err := m.lc.ReserveVolumeDeletion(ctx, m.req())
	return admissionErr(err)
}
func (m *masterAuthority) NoLiveLease(ctx context.Context) error {
	_, err := m.lc.CheckVolumeDeletionLease(ctx, m.req())
	return err
}

// admissionErr: FailedPrecondition and InvalidArgument are definite
// refusals; anything else (unavailable, deadline, a lost response) leaves
// the outcome unknown.
func admissionErr(err error) error {
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.FailedPrecondition, codes.InvalidArgument:
		return fmt.Errorf("%w: %v", chunkstore.ErrNotAdmitted, err)
	}
	return err
}
