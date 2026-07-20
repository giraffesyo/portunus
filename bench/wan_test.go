package bench

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/giraffesyo/portunus"
	"github.com/hashicorp/yamux"
)

// The canonical WAN profile: a long path where a static window caps a stream
// at window/RTT regardless of how fast the link is. At 200ms RTT a 256KB
// window tops out near 1.3 MB/s; this is the regime real tunnels run in and
// the one BDP autotuning exists for.
const (
	wanOneWay = 100 * time.Millisecond // 200ms RTT
	wanBytes  = 4 << 20                // transferred per iteration
)

// laggyConn delivers writes after a fixed delay, in strict enqueue order —
// one goroutine per direction. Per-write goroutines would let chunks
// overtake each other, which to the peer is a corrupt frame stream rather
// than a slow link.
type laggyConn struct {
	net.Conn
	delay  time.Duration
	queue  chan timedChunk
	closed chan struct{}
	once   sync.Once
}

type timedChunk struct {
	at time.Time
	b  []byte
}

func newLaggyConn(c net.Conn, delay time.Duration) *laggyConn {
	l := &laggyConn{
		Conn:  c,
		delay: delay,
		// Bounded: a real link has finite buffering, and an unbounded
		// queue lets a large window park gigabytes in the harness, which
		// measures allocator pressure rather than the protocol.
		queue:  make(chan timedChunk, 64),
		closed: make(chan struct{}),
	}
	go l.deliver()
	return l
}

func (c *laggyConn) Write(p []byte) (int, error) {
	b := make([]byte, len(p))
	copy(b, p)
	select {
	case c.queue <- timedChunk{at: time.Now().Add(c.delay), b: b}:
		return len(p), nil
	case <-c.closed:
		return 0, io.ErrClosedPipe
	}
}

func (c *laggyConn) deliver() {
	for {
		select {
		case <-c.closed:
			return
		case ch := <-c.queue:
			if d := time.Until(ch.at); d > 0 {
				select {
				case <-time.After(d):
				case <-c.closed:
					return
				}
			}
			if _, err := c.Conn.Write(ch.b); err != nil {
				return
			}
		}
	}
}

func (c *laggyConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

func laggyConns(delay time.Duration) (net.Conn, net.Conn) {
	a, b := net.Pipe()
	return newLaggyConn(a, delay), newLaggyConn(b, delay)
}

// BenchmarkWANMux measures a single stream over a 200ms path with autotuning
// starting from the 64KB protocol floor: every byte of window is earned.
func BenchmarkWANMux(b *testing.B) {
	a, z := laggyConns(wanOneWay)
	cfg := &portunus.Config{
		InitialWindow:     64 << 10,
		MaxWindow:         32 << 20,
		KeepaliveInterval: 50 * time.Millisecond,
		KeepaliveTimeout:  30 * time.Second,
	}
	ctx := context.Background()

	type res struct {
		s   *portunus.NativeSession
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := portunus.Server(z, cfg)
		ch <- res{s, err}
	}()
	cs, err := portunus.Client(a, cfg)
	if err != nil {
		b.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		b.Fatal(r.err)
	}
	defer cs.Close()
	defer r.s.Close()

	go func() {
		for {
			st, err := r.s.AcceptStream(ctx)
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	st, err := cs.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(wanBytes)
	b.ResetTimer()
	for b.Loop() {
		for sent := 0; sent < wanBytes; sent += len(buf) {
			if _, err := st.Write(buf); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkWANYamuxDefault is yamux as deployed out of the box: a fixed
// 256KB window, which is the configuration real tunnels are running.
func BenchmarkWANYamuxDefault(b *testing.B) {
	benchWANYamux(b, 256*1024)
}

// BenchmarkWANYamuxTuned gives yamux a hand-tuned 16MB window — the best a
// static window can do, if an operator knows the path in advance.
func BenchmarkWANYamuxTuned(b *testing.B) {
	benchWANYamux(b, 16*1024*1024)
}

func benchWANYamux(b *testing.B, window uint32) {
	a, z := laggyConns(wanOneWay)
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.MaxStreamWindowSize = window
	cfg.LogOutput = io.Discard

	type res struct {
		s   *yamux.Session
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := yamux.Server(z, cfg)
		ch <- res{s, err}
	}()
	cs, err := yamux.Client(a, cfg)
	if err != nil {
		b.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		b.Fatal(r.err)
	}
	defer cs.Close()
	defer r.s.Close()

	go func() {
		for {
			st, err := r.s.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	st, err := cs.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(wanBytes)
	b.ResetTimer()
	for b.Loop() {
		for sent := 0; sent < wanBytes; sent += len(buf) {
			if _, err := st.Write(buf); err != nil {
				b.Fatal(err)
			}
		}
	}
}
