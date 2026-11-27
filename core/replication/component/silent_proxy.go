package component

import (
	"io"
	"net"
	"sync"
)

// SilentProxy sits between the primary and one replica. Normally it
// forwards both ways. GoSilent makes every connection open at that moment
// a black hole: the proxy keeps it open and discards what arrives, in both
// directions, and never sends FIN or RST. That is a path that died without
// telling anyone (a partition, a dead host whose kernel answers nothing),
// so the primary's writes into it still "succeed" into its socket buffer.
// Connections accepted after GoSilent forward normally: the network is back,
// but a connection that went silent stays silent.
type SilentProxy struct {
	ln     net.Listener
	target string

	mu     sync.Mutex
	conns  []*proxiedConn
	closed bool
	wg     sync.WaitGroup
}

type proxiedConn struct {
	client, server net.Conn
	mu             sync.Mutex
	silent         bool
}

func (p *proxiedConn) isSilent() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.silent
}

// NewSilentProxy listens on a fresh loopback port and forwards to target.
func NewSilentProxy(target string) (*SilentProxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &SilentProxy{ln: ln, target: target}
	p.wg.Add(1)
	go p.accept()
	return p, nil
}

// Addr is the address the primary dials instead of the replica's.
func (p *SilentProxy) Addr() string { return p.ln.Addr().String() }

func (p *SilentProxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.ln.Accept()
		if err != nil {
			return
		}
		server, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		pc := &proxiedConn{client: client, server: server}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			_ = client.Close()
			_ = server.Close()
			return
		}
		p.conns = append(p.conns, pc)
		p.mu.Unlock()
		p.wg.Add(2)
		go p.pump(pc, client, server)
		go p.pump(pc, server, client)
	}
}

// pump copies src to dst until src ends; once the connection is silent it
// keeps reading src and drops the bytes, so src's sender never blocks and
// never learns anything.
func (p *SilentProxy) pump(pc *proxiedConn, src, dst net.Conn) {
	defer p.wg.Done()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 && !pc.isSilent() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			if err != io.EOF && pc.isSilent() {
				return
			}
			if !pc.isSilent() {
				_ = dst.Close()
			}
			return
		}
	}
}

// GoSilent turns every currently open connection into a black hole.
func (p *SilentProxy) GoSilent() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, pc := range p.conns {
		pc.mu.Lock()
		pc.silent = true
		pc.mu.Unlock()
	}
}

// Close stops the proxy and closes every connection it holds.
func (p *SilentProxy) Close() {
	p.mu.Lock()
	p.closed = true
	conns := p.conns
	p.mu.Unlock()
	_ = p.ln.Close()
	for _, pc := range conns {
		_ = pc.client.Close()
		_ = pc.server.Close()
	}
	p.wg.Wait()
}
