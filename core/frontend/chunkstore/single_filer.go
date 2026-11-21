package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/filerpb"
	"github.com/seaweedfs/seaweed-block/core/frontend/chunkstore/masterpb"
	"github.com/seaweedfs/seaweed-block/core/rpc/link"
)

// ErrNotSingleFiler: block metadata needs exactly one filer behind the
// configured address. Several filers in one group each serve the namespace
// from their own metadata store and catch up on each other's changes
// asynchronously, so a compare-and-swap on one filer does not exclude a
// concurrent writer on another. This is a startup census, not an ongoing
// fence: a filer added to the group later is not detected.
var ErrNotSingleFiler = errors.New("chunkstore: block metadata needs exactly one filer")

// singleFilerWait bounds the whole check; singleFilerRetry spaces the
// rounds; singleFilerAttempt bounds one master call within the remaining
// time (vars for tests). A filer registers with the master only some
// seconds after it serves (14 s measured with a fresh filer), so "the
// target is not listed yet" is retried until the bound.
var (
	singleFilerWait    = 60 * time.Second
	singleFilerRetry   = time.Second
	singleFilerAttempt = 5 * time.Second
)

// CheckSingleFiler asks the filer for its masters and filer group
// (filer_pb.SeaweedFiler/GetFilerConfiguration), then asks every master for
// the filers registered in that group (master_pb.Seaweed/ListClusterNodes,
// client_type "filer"). It passes when a master lists exactly one filer and
// that filer is the configured one. Any master listing another filer, or
// several, refuses at once, whichever master answers first. No master
// listing the target yet, or none answering, is retried until the bound and
// then refused (fail closed).
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
	for {
		ok, err := c.singleFilerRound(ctx, cfg, &tried)
		if ok || err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: could not confirm filer %s as the only filer within %s: %s", ErrNotSingleFiler, c.cfg.FilerGRPC, singleFilerWait, strings.Join(tried, "; "))
		case <-time.After(singleFilerRetry):
		}
	}
}

// singleFilerRound asks every master once. It refuses when any master lists
// several filers or a filer other than the target; it passes when no master
// refuses and at least one lists exactly the target; otherwise it asks for
// another round (tried keeps each master's answer).
func (c *SWFS) singleFilerRound(ctx context.Context, cfg *filerpb.GetFilerConfigurationResponse, tried *[]string) (bool, error) {
	*tried = (*tried)[:0]
	group := cfg.GetFilerGroup()
	sawTarget := false
	for _, m := range cfg.GetMasters() {
		addr, err := grpcAddress(m)
		if err != nil {
			*tried = append(*tried, fmt.Sprintf("%s: %v", m, err))
			continue
		}
		actx, cancel := context.WithTimeout(ctx, singleFilerAttempt)
		nodes, err := listFilers(actx, addr, group)
		cancel()
		if err != nil {
			*tried = append(*tried, fmt.Sprintf("%s: %v", addr, err))
			continue
		}
		switch {
		case len(nodes) > 1:
			return false, fmt.Errorf("%w: filer group %q has %d filers %v (master %s)", ErrNotSingleFiler, group, len(nodes), nodes, addr)
		case len(nodes) == 1 && !c.isTarget(ctx, nodes[0]):
			return false, fmt.Errorf("%w: master %s lists filer %s in group %q, not the configured %s", ErrNotSingleFiler, addr, nodes[0], group, c.cfg.FilerGRPC)
		case len(nodes) == 1:
			sawTarget = true
		default:
			*tried = append(*tried, addr+": lists no filer in group "+strconv.Quote(group)+" yet")
		}
	}
	if ctx.Err() != nil {
		// The bound passed while a later master was still being asked:
		// its view is unknown, so an earlier favourable answer does not
		// count.
		return false, nil
	}
	return sawTarget, nil
}

// isTarget reports whether a filer address listed by a master is the
// configured filer: the same gRPC port, and the same host or hosts that
// resolve to a common address. Addresses compare with their IPv6 zone; an
// address that does not parse never matches (fail closed).
func (c *SWFS) isTarget(ctx context.Context, listed string) bool {
	got, err := grpcAddress(listed)
	if err != nil {
		return false
	}
	gh, gp, err1 := net.SplitHostPort(got)
	wh, wp, err2 := net.SplitHostPort(c.cfg.FilerGRPC)
	if err1 != nil || err2 != nil || gp != wp {
		return false
	}
	if gh == wh {
		return true
	}
	ga, err1 := net.DefaultResolver.LookupHost(ctx, gh)
	wa, err2 := net.DefaultResolver.LookupHost(ctx, wh)
	if err1 != nil || err2 != nil {
		return false
	}
	for _, a := range ga {
		pa, err := netip.ParseAddr(a)
		if err != nil {
			continue
		}
		for _, b := range wa {
			if pb, err := netip.ParseAddr(b); err == nil && pa.Unmap() == pb.Unmap() {
				return true
			}
		}
	}
	return false
}

func listFilers(ctx context.Context, addr, group string) ([]string, error) {
	conn, err := link.Dial(addr)
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

// grpcAddress turns a SeaweedFS server address into its gRPC address:
// "host:port.grpcPort" names the gRPC port; "host:port" uses port+10000
// (SeaweedFS's convention).
func grpcAddress(a string) (string, error) {
	host, port, err := net.SplitHostPort(a)
	if err != nil {
		// "host:port.grpcPort" does not parse as host:port; split it first.
		i := strings.LastIndex(a, ":")
		if i < 0 {
			return "", fmt.Errorf("bad server address %q", a)
		}
		host, port = a[:i], a[i+1:]
	}
	if http, grpcPort, ok := strings.Cut(port, "."); ok {
		if _, err := strconv.Atoi(http); err != nil {
			return "", fmt.Errorf("bad server address %q", a)
		}
		if _, err := strconv.Atoi(grpcPort); err != nil {
			return "", fmt.Errorf("bad server address %q", a)
		}
		return net.JoinHostPort(host, grpcPort), nil
	}
	p, err := strconv.Atoi(port)
	if err != nil || p <= 0 || p+10000 > 65535 {
		return "", fmt.Errorf("bad server address %q", a)
	}
	return net.JoinHostPort(host, strconv.Itoa(p+10000)), nil
}
