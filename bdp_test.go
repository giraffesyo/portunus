package mux

import (
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// laggyConn delays delivery so a test can exercise a long fat pipe: the
// regime where a static window caps throughput at window/RTT no matter how
// fast the link is.
//
// Delivery is strictly ordered by a single goroutine. Spawning one goroutine
// per write would let chunks race and reorder, which to the peer is a
// corrupt frame stream, not a slow link.
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
		Conn:   c,
		delay:  delay,
		queue:  make(chan timedChunk, 1024),
		closed: make(chan struct{}),
	}
	go l.deliver()
	return l
}

func (c *laggyConn) Write(p []byte) (int, error) {
	// Copied because the caller may reuse p as soon as Write returns.
	b := make([]byte, len(p))
	copy(b, p)
	select {
	case c.queue <- timedChunk{at: time.Now().Add(c.delay), b: b}:
		return len(p), nil
	case <-c.closed:
		return 0, io.ErrClosedPipe
	}
}

// deliver writes chunks in enqueue order, each held until its arrival time,
// so every chunk sees the same delay and none overtakes another.
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

// laggyPair returns two sessions whose carrier delays every write, giving a
// round trip of roughly 2*delay.
func laggyPair(t *testing.T, delay time.Duration, cfg *Config) (*NativeSession, *NativeSession) {
	t.Helper()
	a, b := net.Pipe()
	la := newLaggyConn(a, delay)
	lb := newLaggyConn(b, delay)

	type res struct {
		s   *NativeSession
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := Server(lb, cfg)
		ch <- res{s, err}
	}()
	cs, err := Client(la, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() { cs.Close(); r.s.Close() })
	return cs, r.s
}

func TestBDPWindowGrowsOnHighLatencyPath(t *testing.T) {
	cfg := &Config{
		InitialWindow:     64 << 10, // the protocol floor: all growth is earned
		MaxWindow:         8 << 20,
		KeepaliveInterval: 50 * time.Millisecond,
		KeepaliveTimeout:  10 * time.Second,
	}
	client, server := laggyPair(t, 10*time.Millisecond, cfg)
	c := ctx(t)

	go func() {
		st, err := server.AcceptStream(c)
		if err != nil {
			return
		}
		io.Copy(io.Discard, st)
	}()

	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	// Push enough data, for long enough, that several probes complete.
	deadline := time.Now().Add(3 * time.Second)
	buf := make([]byte, 32<<10)
	for time.Now().Before(deadline) {
		if _, err := st.Write(buf); err != nil {
			t.Fatalf("write: %v", err)
		}
	}

	if got := server.bdp.target(); got <= uint64(cfg.InitialWindow) {
		t.Fatalf("BDP target did not grow: %d bytes (initial %d)", got, cfg.InitialWindow)
	}
	if rtt := server.bdp.rtt(); rtt == 0 {
		t.Fatal("no RTT sample was taken")
	}
}

// Autotuning must never let a window shrink or a limit move backwards, which
// the peer treats as a protocol error.
func TestWindowLimitsNeverMoveBackwards(t *testing.T) {
	client, server := pair(t, &Config{
		InitialWindow:     64 << 10,
		MaxWindow:         4 << 20,
		KeepaliveInterval: 20 * time.Millisecond,
		KeepaliveTimeout:  5 * time.Second,
	})
	c := ctx(t)

	go func() {
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 256<<10)
	for range 20 {
		if _, err := st.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	// A backwards or no-op limit is a protocol error at the peer, so
	// surviving this exchange is the assertion.
	if err := client.closedErr(); err != nil {
		t.Fatalf("client died during autotune: %v", err)
	}
	if err := server.closedErr(); err != nil {
		t.Fatalf("server died during autotune: %v", err)
	}
}

// The session receive budget must bound autotuned growth rather than letting
// many streams each climb to MaxWindow.
func TestReceiveBudgetBoundsGrowth(t *testing.T) {
	b := newBudget(1<<20, 512<<10)

	// One stream may climb to its own cap.
	if got := b.grow(64<<10, 512<<10); got != 512<<10 {
		t.Fatalf("first grow gave %d, want %d", got, 512<<10)
	}
	// A second may take the remaining headroom but not exceed the budget.
	got := b.grow(64<<10, 512<<10)
	if got > 512<<10 {
		t.Fatalf("second grow exceeded stream cap: %d", got)
	}
	if b.granted > 1<<20 {
		t.Fatalf("budget exceeded: granted %d of %d", b.granted, 1<<20)
	}
	// Releasing frees headroom for later streams.
	b.release(got)
	if b.granted > 1<<20 {
		t.Fatalf("granted %d after release", b.granted)
	}
}

// Growth must stop when delay says the path is congested, not the window —
// otherwise a loaded session grows its window because it is loaded, which is
// the bufferbloat spiral.
func TestBDPRejectsCongestedSamples(t *testing.T) {
	b := newBDPEstimator(64<<10, 8<<20)
	now := time.Now()

	// A window-limited sample (the sender ran out of credit) grows the
	// estimate and establishes the min RTT.
	b.shouldProbe(1<<20, 1, time.Now())
	b.markSent(now, 1<<20, 0)
	if first := b.onACK(1, 1<<20+512<<10, 1, now.Add(10*time.Millisecond)); first == 0 {
		t.Fatal("a window-limited sample should have grown the window")
	}

	// A stalled sample whose RTT has doubled means the path, not the
	// window, is the constraint: growing then only deepens queues.
	before := b.target()
	b.shouldProbe(1<<21+1<<20, 2, time.Now())
	b.markSent(now, 1<<21+1<<20, 1)
	b.onACK(2, 1<<21+1<<20+512<<10, 2, now.Add(200*time.Millisecond))
	if b.target() > before {
		t.Fatalf("congested sample grew the window: %d -> %d", before, b.target())
	}
}

// An unstamped probe (one whose batch never reached the carrier) must be
// discarded rather than producing a bogus zero-time RTT.
func TestBDPIgnoresUnstampedProbe(t *testing.T) {
	b := newBDPEstimator(64<<10, 8<<20)
	b.shouldProbe(1<<20, 7, time.Now())
	// No markSent: the flush never happened.
	if got := b.onACK(7, 1<<21, 1, time.Now()); got != 0 {
		t.Fatalf("unstamped probe produced a target: %d", got)
	}
	if b.rtt() != 0 {
		t.Fatalf("unstamped probe produced an RTT: %v", b.rtt())
	}
}

// Keepalive must declare a silent peer dead, and must judge that on
// receive-side silence rather than on our ability to send.
func TestKeepaliveDetectsSilentPeer(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	// Drain but never respond: the carrier is writable, the peer is not
	// alive. Sending succeeds throughout, so only receive-side silence can
	// detect this.
	go io.Copy(io.Discard, b)

	sess, err := Client(a, &Config{
		KeepaliveInterval: 40 * time.Millisecond,
		KeepaliveTimeout:  150 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	done := make(chan error, 1)
	go func() { done <- sess.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("session ended with nil error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("keepalive did not detect a silent peer")
	}
}

// A responsive peer must never be declared dead.
func TestKeepaliveToleratesLiveIdlePeer(t *testing.T) {
	client, _ := pair(t, &Config{
		KeepaliveInterval: 20 * time.Millisecond,
		KeepaliveTimeout:  200 * time.Millisecond,
	})
	time.Sleep(600 * time.Millisecond) // many probe intervals, no traffic
	if err := client.closedErr(); err != nil {
		t.Fatalf("keepalive killed a live idle session: %v", err)
	}
}

func TestIncoherentKeepaliveConfigRejected(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	_, err := Client(conn, &Config{
		KeepaliveInterval: time.Second,
		KeepaliveTimeout:  time.Second, // must exceed the interval
	})
	if err == nil {
		t.Fatal("accepted a liveness timeout at the probe interval")
	}
}

// Autotuning must survive the transfer it is tuning: bytes still arrive
// intact while windows are growing underneath.
func TestDataIntegrityDuringAutotune(t *testing.T) {
	client, server := pair(t, &Config{
		InitialWindow:     64 << 10,
		MaxWindow:         4 << 20,
		KeepaliveInterval: 15 * time.Millisecond,
		KeepaliveTimeout:  5 * time.Second,
	})
	c := ctx(t)

	payload := make([]byte, 3<<20)
	for i := range payload {
		payload[i] = byte(i*7 + i/256)
	}

	go func() {
		st, err := server.AcceptStream(c)
		if err != nil {
			return
		}
		io.Copy(st, st)
		st.CloseWrite()
	}()

	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		st.Write(payload)
		st.CloseWrite()
	}()
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo mismatch during autotune: got %d bytes, want %d", len(got), len(payload))
	}
}

// An app-limited sample is no evidence the window is the constraint: the
// sender never stalled and barely used the window it had, so the estimate
// must hold steady.
func TestBDPDoesNotGrowWhenAppLimited(t *testing.T) {
	b := newBDPEstimator(64<<10, 8<<20)
	now := time.Now()
	before := b.target()

	b.shouldProbe(1<<20, 1, now)
	b.markSent(now, 1<<20, 5)
	// Same stall count at ACK, and only a few KB moved against a 64KB
	// window: the application, not the window, was the limit.
	if got := b.onACK(1, 1<<20+4<<10, 5, now.Add(10*time.Millisecond)); got != 0 {
		t.Fatalf("grew on an app-limited sample: %d", got)
	}
	if b.target() != before {
		t.Fatalf("window moved on an app-limited sample: %d -> %d", before, b.target())
	}
}

// A probe that never completes must not disable autotuning for the life of
// the session: the slot is reclaimed once it is clearly stale.
func TestBDPRecoversFromLostProbe(t *testing.T) {
	b := newBDPEstimator(64<<10, 8<<20)
	now := time.Now()

	if !b.shouldProbe(1<<20, 1, now) {
		t.Fatal("first probe was not reserved")
	}
	b.markSent(now, 1<<20, 0)
	// Its ACK never arrives. A later probe attempt must reclaim the slot.
	if b.shouldProbe(1<<21, 2, now.Add(time.Second)) {
		t.Fatal("reserved a second probe while one was still plausibly in flight")
	}
	if !b.shouldProbe(1<<21, 3, now.Add(30*time.Second)) {
		t.Fatal("a lost probe permanently wedged the estimator")
	}
}
