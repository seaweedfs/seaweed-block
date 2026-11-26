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

// chunkVolumeDelete: sw-block ops chunk volume-delete --volume v
// --chunk-filer host:port --master host:port --request-id r deletes a
// volume in the layout kernel's order. The blockmaster holds the delete's
// authority: ReserveVolumeDeletion commits the reservation (refused while
// an authority line is live; no grant for the volume after it; never given
// back), and CheckVolumeNoLiveLease runs between the HEAD read and the
// update that marks HEAD deleting. A refused delete leaves the volume
// reserved; rerun with the same --request-id once its cause is cleared. A
// rerun on a deleted volume sweeps any object that landed late.
func chunkVolumeDelete(args []string, stdout, stderr io.Writer) int {
	c := newBCommand("volume-delete", time.Minute, stdout, stderr)
	var vol volumeFlags
	var requestID string
	c.bindVolume(&vol, "volume", "volume")
	c.fs.StringVar(&requestID, "request-id", "", "this delete's operation id; a rerun with the same id resumes it")
	var ref chunkstore.VolumeRef
	return c.run(args, true, func() (err error) {
		if c.master == "" || requestID == "" || c.timeout <= 0 {
			return errors.New("--master and --request-id are required; --timeout must be positive")
		}
		ref, err = vol.ref("volume")
		return err
	}, func(ctx context.Context, cat *chunkstore.BCatalog) (any, error) {
		conn, err := link.DialMasterFromEnv(c.master)
		if err != nil {
			return nil, err
		}
		defer conn.Close()
		auth := &masterAuthority{lc: control.NewLifecycleServiceClient(conn), req: &control.MarkVolumeDeletingRequest{VolumeId: ref.Name, Token: requestID}}
		skipped, err := cat.DeleteVolume(ctx, ref, auth)
		if err != nil {
			return nil, err
		}
		out := map[string]any{"volume": ref.Dir(), "state": "deleted"}
		if len(skipped) > 0 {
			// Roots HEAD names whose objects an earlier run already deleted
			// (after releasing every source): reported, not read.
			names := make([]string, 0, len(skipped))
			for _, rn := range skipped {
				names = append(names, rn.String())
			}
			out["already_deleted_roots"] = names
		}
		return out, nil
	})
}

// masterAuthority is the layout delete's authority, held by the
// blockmaster: both operations are its RPCs, nothing is decided here.
type masterAuthority struct {
	lc  control.LifecycleServiceClient
	req *control.MarkVolumeDeletingRequest
}

func (m *masterAuthority) Reserve(ctx context.Context) error {
	_, err := m.lc.ReserveVolumeDeletion(ctx, m.req)
	return authorityErr(err)
}

func (m *masterAuthority) NoLiveLease(ctx context.Context) error {
	_, err := m.lc.CheckVolumeNoLiveLease(ctx, m.req)
	return authorityErr(err)
}

// authorityErr: FailedPrecondition and InvalidArgument are definite
// refusals; anything else (unavailable, deadline, a lost response) leaves
// the outcome unknown and a rerun decides.
func authorityErr(err error) error {
	switch status.Code(err) {
	case codes.OK:
		return nil
	case codes.FailedPrecondition, codes.InvalidArgument:
		return fmt.Errorf("%w: %v", chunkstore.ErrNotAdmitted, err)
	}
	return err
}
