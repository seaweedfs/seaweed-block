package nvme

import (
	"errors"
	"fmt"
	"net"
)

// Transport names the NVMe-oF transport used by Target.
type Transport string

const (
	// TransportTCP is the currently implemented NVMe-oF data path.
	TransportTCP Transport = "tcp"
	// TransportRDMA is reserved for the future NVMe/RDMA listener.
	TransportRDMA Transport = "rdma"
)

// ErrTransportUnsupported is returned when the target is asked to bind a
// transport whose listener is not implemented.
var ErrTransportUnsupported = errors.New("nvme: transport unsupported")

// ListenerFactory is the narrow seam a future RDMA listener must satisfy. It is
// intentionally net.Listener-shaped because the NVMe session layer already
// consumes net.Conn.
type ListenerFactory func(transport Transport, listen string) (net.Listener, error)

func defaultListenerFactory(transport Transport, listen string) (net.Listener, error) {
	switch normalizeTransport(transport) {
	case TransportTCP:
		return net.Listen("tcp", listen)
	case TransportRDMA:
		return nil, fmt.Errorf("%w: %s listener not implemented", ErrTransportUnsupported, TransportRDMA)
	default:
		return nil, fmt.Errorf("%w: %s", ErrTransportUnsupported, transport)
	}
}

func normalizeTransport(transport Transport) Transport {
	if transport == "" {
		return TransportTCP
	}
	return transport
}
