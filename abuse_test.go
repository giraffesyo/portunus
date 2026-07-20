package portunus

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/giraffesyo/portunus/internal/frame"
)

// A PING flood must be cut off rather than answered indefinitely: every
// inbound PING obliges an echo, which makes it a free amplifier.
func TestAbusePingFloodIsStopped(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()

	for i := range 5000 {
		var buf [frame.PingLen]byte
		p.send(frame.Header{Type: frame.TypePing}, frame.AppendPing(buf[:0], uint64(i)))
		if sess.closedErr() != nil {
			break
		}
	}
	assertCalm(t, sess, "ping flood")
}

// Empty DATA frames cost parse and dispatch while consuming no flow-control
// credit, so no byte budget catches them.
func TestAbuseEmptyFrameFloodIsStopped(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("x"))

	for range 5000 {
		p.send(frame.Header{Type: frame.TypeData, StreamID: 3}, nil)
		if sess.closedErr() != nil {
			break
		}
	}
	assertCalm(t, sess, "empty frame flood")
}

// Reset limiting is opt-in, and when enabled must cut off the churn.
func TestAbuseResetFloodIsStoppedWhenEnabled(t *testing.T) {
	cfg := &Config{
		MaxIncomingStreams: 4096,
		AcceptBacklog:      4096,
		MaxResetsPerSecond: 200,
	}
	sess, p := rawPair(t, cfg)
	p.settings()

	// Odd IDs are peer-initiated against a server session.
	for i := range 5000 {
		id := uint32(2*i + 1)
		p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: id}, []byte("x"))
		p.send(frame.Header{Type: frame.TypeRST, StreamID: id}, frame.AppendRST(nil, CodeCanceled, 0))
		if sess.closedErr() != nil {
			break
		}
	}
	assertCalm(t, sess, "reset flood")
}

// assertCalm waits for the session to end and checks it did so by asking the
// peer to back off, rather than by claiming a protocol violation: exceeding a
// rate limit is a judgment about behavior, not malformed input.
func assertCalm(t *testing.T, sess *NativeSession, why string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := sess.closedErr(); err != nil {
			var se *SessionError
			if !errors.As(err, &se) {
				t.Fatalf("%s: closed with %v, want *SessionError", why, err)
			}
			if se.Code != CodeCalm {
				t.Fatalf("%s: closed with code %d, want CodeCalm (%d)", why, se.Code, CodeCalm)
			}
			if st := sess.Stats(); st.AbuseTrips == 0 {
				t.Errorf("%s: AbuseTrips not recorded", why)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s: session survived the flood", why)
}

// A conformant peer keeps well inside the limits: normal traffic, including
// keepalive PINGs and ordinary stream churn, must never trip them.
func TestAbuseLimitsToleratesNormalTraffic(t *testing.T) {
	client, server := pair(t, &Config{
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
			go func(st Stream) { io.Copy(st, st); st.Close() }(st)
		}
	}()

	// Churn streams, cancel half of them, and let keepalive run throughout.
	for i := range 300 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		st.Write([]byte("payload"))
		if i%2 == 0 {
			st.CancelWrite(CodeApp)
		} else {
			st.CloseWrite()
		}
		st.Close()
	}
	time.Sleep(200 * time.Millisecond) // several keepalive intervals

	if err := client.closedErr(); err != nil {
		t.Fatalf("client tripped a limit on normal traffic: %v", err)
	}
	if err := server.closedErr(); err != nil {
		t.Fatalf("server tripped a limit on normal traffic: %v", err)
	}
}

// The advertised minimum ping interval must actually reach the peer, so a
// conformant implementation can respect it.
func TestPingMinIntervalIsAdvertised(t *testing.T) {
	a, b := netPipe()
	defer b.Close()

	frames := readFrames(t, b)
	sess, err := Server(a, &Config{KeepaliveInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	f := <-frames
	if f.h.Type != frame.TypeSettings {
		t.Fatalf("first frame is %v, want SETTINGS", f.h.Type)
	}
	settings, err := frame.ParseSettings(f.p)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range settings {
		if s.ID == frame.SettingPingMinInterval {
			if s.Value == 0 {
				t.Fatal("advertised a zero minimum ping interval")
			}
			return
		}
	}
	t.Fatal("SETTINGS did not advertise a minimum ping interval")
}

// A panic inside the library must fail the session, not the host
// application. This is what turns any latent bug reachable from peer input
// into one broken connection rather than an outage.
func TestPanicIsContainedToTheSession(t *testing.T) {
	client, _ := pair(t, nil)

	// recoverPanic is the containment path both library goroutines install;
	// exercising it directly is the only way to assert the behavior without
	// planting a real panic in production code.
	func() {
		defer client.recoverPanic("test goroutine")
		panic("synthetic failure")
	}()

	err := client.closedErr()
	if err == nil {
		t.Fatal("a panic did not fail the session")
	}
	var se *SessionError
	if !errors.As(err, &se) {
		t.Fatalf("got %v, want *SessionError", err)
	}
	if !strings.Contains(se.Reason, "synthetic failure") || !strings.Contains(se.Reason, "test goroutine") {
		t.Fatalf("error should name the panic and the goroutine, got %q", se.Reason)
	}
	// And the session must be genuinely closed, not merely marked.
	if _, err := client.OpenStream(context.Background()); err == nil {
		t.Fatal("session still usable after a contained panic")
	}
}

func TestStatsReportTraffic(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	go func() {
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go func(st Stream) { io.Copy(io.Discard, st); st.Close() }(st)
		}
	}()

	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	payload := make([]byte, 512<<10)
	if _, err := st.Write(payload); err != nil {
		t.Fatal(err)
	}
	st.Close()
	time.Sleep(100 * time.Millisecond)

	cs := client.Stats()
	if cs.StreamsOpened == 0 {
		t.Error("StreamsOpened not counted")
	}
	if cs.BytesSent < uint64(len(payload)) {
		t.Errorf("BytesSent = %d, want at least %d", cs.BytesSent, len(payload))
	}
	if cs.Flushes == 0 || cs.FramesSent == 0 {
		t.Errorf("flush counters empty: %+v", cs)
	}
	if cs.FramesPerFlush <= 0 {
		t.Error("FramesPerFlush not derived")
	}
	if cs.MaxFlushBytes == 0 {
		t.Error("MaxFlushBytes not recorded")
	}

	ss := server.Stats()
	if ss.StreamsAccepted == 0 {
		t.Error("StreamsAccepted not counted")
	}
	if ss.BytesReceived < uint64(len(payload)) {
		t.Errorf("BytesReceived = %d, want at least %d", ss.BytesReceived, len(payload))
	}
	if ss.FramesReceived == 0 {
		t.Error("FramesReceived not counted")
	}
}

func TestStatsReportRefusals(t *testing.T) {
	cfg := &Config{AcceptBacklog: 1, MaxIncomingStreams: 1}
	client, server := pair(t, cfg)
	c := ctx(t)

	for range 8 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		st.Write([]byte("hi"))
	}
	time.Sleep(200 * time.Millisecond)

	if got := server.Stats().StreamsRefused; got == 0 {
		t.Fatal("refused streams were not counted")
	}
}

// Idle reaping is opt-in, and when enabled must free the slots a silent peer
// would otherwise hold forever.
func TestIdleStreamsAreReapedWhenEnabled(t *testing.T) {
	cfg := &Config{
		StreamIdleTimeout: 100 * time.Millisecond,
		KeepaliveInterval: 30 * time.Millisecond,
		KeepaliveTimeout:  5 * time.Second,
	}
	client, server := pair(t, cfg)
	c := ctx(t)

	// Open streams, send once, then go quiet — a silent peer holding slots.
	for range 4 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	for range 4 {
		if _, err := server.AcceptStream(c); err != nil {
			t.Fatal(err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		live := server.incoming
		server.mu.Unlock()
		if live == 0 {
			if got := server.Stats().StreamsReaped; got == 0 {
				t.Error("reaped streams were not counted")
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("idle streams were never reaped")
}

// With reaping off (the default), a quiet stream must survive: holding a
// connection open with no traffic is normal for a tunnel.
func TestIdleStreamsSurviveByDefault(t *testing.T) {
	client, server := pair(t, &Config{
		KeepaliveInterval: 20 * time.Millisecond,
		KeepaliveTimeout:  5 * time.Second,
	})
	cs, ss := openAccept(t, client, server)

	time.Sleep(300 * time.Millisecond) // many keepalive sweeps

	if _, err := cs.Write([]byte("still here")); err != nil {
		t.Fatalf("quiet stream was reaped with reaping disabled: %v", err)
	}
	buf := make([]byte, len("still here"))
	ss.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadFull(ss, buf); err != nil {
		t.Fatalf("peer end unusable: %v", err)
	}
}

// With reset limiting off (the default), rapid stream churn must be allowed:
// a proxy resets as fast as it opens connections, and the structural Rapid
// Reset defense — a slot held until the application closes it — does not
// depend on rate limiting.
func TestRapidChurnAllowedByDefault(t *testing.T) {
	client, server := pair(t, &Config{MaxIncomingStreams: 512, AcceptBacklog: 512})
	c := ctx(t)

	go func() {
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go func(st Stream) { io.Copy(io.Discard, st); st.Close() }(st)
		}
	}()

	// Far faster than any rate a limiter could safely allow.
	for i := range 3000 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatalf("open %d: %v", i, err)
		}
		st.Write([]byte("x"))
		st.Close()
	}
	if err := client.closedErr(); err != nil {
		t.Fatalf("rapid churn was rejected by default: %v", err)
	}
}

// A write that fails at batch admission must give its flow-control credit
// back. Without that, every deadline expiry permanently shrinks the window
// until the stream cannot send at all even though the peer keeps granting
// credit.
func TestFailedWriteReturnsCredit(t *testing.T) {
	client, server := pair(t, nil)
	cs, _ := openAccept(t, client, server)
	st := cs.(*NativeStream)

	st.mu.Lock()
	before := st.sent
	st.mu.Unlock()

	// A deadline already in the past fails the write without sending.
	st.SetWriteDeadline(time.Now().Add(-time.Second))
	if _, err := st.Write(make([]byte, 32<<10)); err == nil {
		t.Fatal("write past its deadline succeeded")
	}

	st.mu.Lock()
	after := st.sent
	st.mu.Unlock()
	if after != before {
		t.Fatalf("failed write consumed %d bytes of credit (sent %d -> %d)", after-before, before, after)
	}
}

// The first batch is sequence zero, and a zero-copy writer that joins it must
// still wait for the flush: returning early would let the caller reuse a
// buffer the kernel is reading out of the iovec.
func TestFirstBatchZeroCopyWriterWaits(t *testing.T) {
	client, server := pair(t, nil)
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

	// Write a zero-copy sized payload, then immediately overwrite the
	// buffer. If Write returned before its flush, the overwrite races the
	// kernel read and the race detector reports it.
	buf := make([]byte, 64<<10)
	for range 50 {
		if _, err := st.Write(buf); err != nil {
			t.Fatal(err)
		}
		for i := range buf {
			buf[i] = byte(i)
		}
	}

	// The writer must never observe a batch it joined as already complete.
	w := client.w
	w.mu.Lock()
	doneSeq, seq := w.doneSeq, w.seq
	w.mu.Unlock()
	if doneSeq > seq {
		t.Fatalf("completed batches (%d) exceeds batches started (%d)", doneSeq, seq)
	}
}
