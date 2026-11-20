package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/masterpb"
)

// ErrNotSingleFiler: block metadata needs exactly one filer behind the
// configured address. Several filers in one group each serve the namespace
// from their own metadata store and catch up on each other's changes
// asynchronously, so a compare-and-swap on one filer does not exclude a
// concurrent writer on another.
var ErrNotSingleFiler = errors.New("chunkstore: block metadata needs exactly one filer")

// singleFilerWait bounds the whole check (a var for tests).
var singleFilerWait = 15 * time.Second

// CheckSingleFiler asks the filer for its masters and filer group
// (filer_pb.SeaweedFiler/GetFilerConfiguration), then asks the masters for
// the filers registered in that group (master_pb.Seaweed/ListClusterNodes,
// client_type "filer"). It passes only when a master answers with exactly
// one filer. More than one filer is refused; so is an answer it cannot get
// or confirm (fail closed).
func (c *SWFS) CheckSingleFiler(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, singleFilerWait)
	defer cancel()
	// The dial is lazy and the filer may still be starting: wait for it, within the bound.
	cfg, err := c.fc.GetFilerConfiguration(ctx, &filerpb.GetFilerConfigurationRequest{}, grpc.WaitForReady(true))
	if err != nil {
		return fmt.Errorf("%w: filer %s configuration: %v", ErrNotSingleFiler, c.cfg.FilerGRPC, err)
	}
	if len(cfg.GetMasters()) == 0 {
		return fmt.Errorf("%w: filer %s reports no master, cannot list its group", ErrNotSingleFiler, c.cfg.FilerGRPC)
	}
	var tried []string
	for _, m := range cfg.GetMasters() {
		addr, err := masterGRPCAddress(m)
		if err != nil {
			tried = append(tried, fmt.Sprintf("%s: %v", m, err))
			continue
		}
		nodes, err := listFilers(ctx, addr, cfg.GetFilerGroup())
		if err != nil {
			tried = append(tried, fmt.Sprintf("%s: %v", addr, err))
			continue
		}
		switch len(nodes) {
		case 1:
			return nil
		case 0:
			tried = append(tried, addr+": lists no filer in group "+strconv.Quote(cfg.GetFilerGroup()))
		default:
			return fmt.Errorf("%w: filer group %q has %d filers %v (master %s)", ErrNotSingleFiler, cfg.GetFilerGroup(), len(nodes), nodes, addr)
		}
	}
	return fmt.Errorf("%w: could not confirm a single filer: %s", ErrNotSingleFiler, strings.Join(tried, "; "))
}

func listFilers(ctx context.Context, addr, group string) ([]string, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	resp, err := masterpb.NewSeaweedClient(conn).ListClusterNodes(ctx, &masterpb.ListClusterNodesRequest{ClientType: "filer", FilerGroup: group})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, n := range resp.GetClusterNodes() {
		out = append(out, n.GetAddress())
	}
	return out, nil
}

// masterGRPCAddress turns a SeaweedFS server address into its gRPC address:
// "host:port.grpcPort" names the gRPC port; "host:port" uses port+10000
// (SeaweedFS's convention).
func masterGRPCAddress(a string) (string, error) {
	host, port, err := net.SplitHostPort(a)
	if err != nil {
		// "host:port.grpcPort" does not parse as host:port; split it first.
		i := strings.LastIndex(a, ":")
		if i < 0 {
			return "", fmt.Errorf("bad master address %q", a)
		}
		host, port = a[:i], a[i+1:]
	}
	if http, grpcPort, ok := strings.Cut(port, "."); ok {
		if _, err := strconv.Atoi(http); err != nil {
			return "", fmt.Errorf("bad master address %q", a)
		}
		if _, err := strconv.Atoi(grpcPort); err != nil {
			return "", fmt.Errorf("bad master address %q", a)
		}
		return net.JoinHostPort(host, grpcPort), nil
	}
	p, err := strconv.Atoi(port)
	if err != nil || p <= 0 || p+10000 > 65535 {
		return "", fmt.Errorf("bad master address %q", a)
	}
	return net.JoinHostPort(host, strconv.Itoa(p+10000)), nil
}
