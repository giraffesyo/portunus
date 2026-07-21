package portunus

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/giraffesyo/portunus/internal/frame"
)

// rawPeer drives the wire directly against a real session, so receiver rules
// can be exercised without a cooperating implementation on the other side.
type rawPeer struct {
	t    *testing.T
	conn net.Conn
}

// rawPair returns a session under test plus a raw peer speaking to it. The
// session is a server, so peer-initiated (client) stream IDs are odd.
func rawPair(t *testing.T, cfg *Config) (*NativeSession, *rawPeer) {
	t.Helper()
	a, b := net.Pipe()
	p := &rawPeer{t: t, conn: b}
	// net.Pipe is a synchronous rendezvous, so the peer must be reading
	// before the session writes its SETTINGS handshake. (A real carrier
	// buffers it; this is the pipe artifact DESIGN.md warns about.)
	go p.drain()
	sess, err := Server(a, cfg)
	if err != nil {
		b.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { sess.Close(); b.Close() })
	return sess, p
}

// drain consumes everything the session sends so its writes never block on a
// full pipe. net.Pipe is synchronous, so an unread peer would deadlock.
func (p *rawPeer) drain() {
	io.Copy(io.Discard, p.conn)
}

func (p *rawPeer) send(h frame.Header, payload []byte) {
	p.t.Helper()
	var hdr [frame.HeaderSize]byte
	h.Length = uint32(len(payload))
	h.Encode(hdr[:])
	p.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := p.conn.Write(hdr[:]); err != nil {
		return // session already dead; the assertion is on sess.Wait()
	}
	if len(payload) > 0 {
		p.conn.Write(payload)
	}
}

func (p *rawPeer) settings() {
	payload, _ := frame.AppendSettings(nil, []frame.Setting{
		{ID: frame.SettingVersion, Value: frame.ProtocolVersion},
		{ID: frame.SettingInitialWindow, Value: frame.FloorInitialWindow},
		{ID: frame.SettingMaxFrameSize, Value: frame.FloorMaxFrameSize},
	})
	p.send(frame.Header{Type: frame.TypeSettings}, payload)
}

// expectDead asserts the session terminates (a protocol error was detected)
// rather than hanging or accepting the malformed input.
func expectDead(t *testing.T, sess *NativeSession, why string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- sess.Wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatalf("%s: session ended with nil error", why)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("%s: session survived", why)
	}
}

// expectAlive asserts the session tolerated the input.
func expectAlive(t *testing.T, sess *NativeSession, why string) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	if err := sess.closedErr(); err != nil {
		t.Fatalf("%s: session died: %v", why, err)
	}
}

func TestProtocolUnknownFrameTypeIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: 9, StreamID: 0}, nil)
	expectDead(t, sess, "unknown frame type")
}

// PADDING is reserved for v2 and must be rejected by a v1 receiver rather
// than silently ignored.
func TestProtocolReservedPaddingIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypePadding}, []byte("pad"))
	expectDead(t, sess, "reserved PADDING type")
}

func TestProtocolStreamFrameOnSessionIDIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 0}, []byte("x"))
	expectDead(t, sess, "DATA on stream 0")
}

func TestProtocolFrameForUnopenedStreamIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	// No SYN ever sent for ID 7, and it is above max-seen.
	p.send(frame.Header{Type: frame.TypeData, StreamID: 7}, []byte("x"))
	expectDead(t, sess, "DATA for unopened stream")
}

func TestProtocolDuplicateSYNIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("a"))
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("b"))
	expectDead(t, sess, "duplicate SYN")
}

func TestProtocolSYNOnControlFrameIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("a"))
	p.send(frame.Header{Type: frame.TypeWindowUpdate, Flags: frame.FlagSYN, StreamID: 3},
		frame.AppendWindowUpdate(nil, 1<<20))
	expectDead(t, sess, "SYN on control frame")
}

func TestProtocolDataAfterFINIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN | frame.FlagFIN, StreamID: 3}, []byte("a"))
	p.send(frame.Header{Type: frame.TypeData, StreamID: 3}, []byte("b"))
	expectDead(t, sess, "DATA after FIN")
}

func TestProtocolWindowOverrunIsFatal(t *testing.T) {
	cfg := &Config{InitialWindow: frame.FloorInitialWindow}
	sess, p := rawPair(t, cfg)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("a"))
	// Nobody reads, so no credit is ever returned: exceed the window.
	big := make([]byte, frame.FloorMaxFrameSize)
	for i := 0; i < frame.FloorInitialWindow/len(big)+2; i++ {
		p.send(frame.Header{Type: frame.TypeData, StreamID: 3}, big)
	}
	expectDead(t, sess, "flow-control overrun")
}

// A WINDOW_UPDATE that grants nothing is a MadeYouReset-style no-op primitive
// and must be rejected.
func TestProtocolNoOpWindowUpdateIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("a"))
	// The session's send limit starts at our advertised initial window.
	p.send(frame.Header{Type: frame.TypeWindowUpdate, StreamID: 3},
		frame.AppendWindowUpdate(nil, frame.FloorInitialWindow))
	expectDead(t, sess, "no-op WINDOW_UPDATE")
}

func TestProtocolBackwardsWindowUpdateIsFatal(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("a"))
	p.send(frame.Header{Type: frame.TypeWindowUpdate, StreamID: 3},
		frame.AppendWindowUpdate(nil, 1))
	expectDead(t, sess, "backwards WINDOW_UPDATE")
}

func TestProtocolMalformedControlPayloadsAreFatal(t *testing.T) {
	for name, f := range map[string]func(p *rawPeer){
		"short window update": func(p *rawPeer) {
			p.send(frame.Header{Type: frame.TypeWindowUpdate, StreamID: 3}, []byte{1, 2, 3})
		},
		"short RST": func(p *rawPeer) {
			p.send(frame.Header{Type: frame.TypeRST, StreamID: 3}, []byte{1})
		},
		"short stop sending": func(p *rawPeer) {
			p.send(frame.Header{Type: frame.TypeStopSending, StreamID: 3}, []byte{1})
		},
		"short ping": func(p *rawPeer) {
			p.send(frame.Header{Type: frame.TypePing}, []byte{1})
		},
		"misaligned settings": func(p *rawPeer) {
			p.send(frame.Header{Type: frame.TypeSettings}, make([]byte, 7))
		},
		"short goaway": func(p *rawPeer) {
			p.send(frame.Header{Type: frame.TypeGoAway}, make([]byte, 3))
		},
	} {
		t.Run(name, func(t *testing.T) {
			sess, p := rawPair(t, nil)
			p.settings()
			p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("a"))
			f(p)
			expectDead(t, sess, name)
		})
	}
}

func TestProtocolOversizedDataFrameIsFatal(t *testing.T) {
	cfg := &Config{MaxFrameSize: frame.FloorMaxFrameSize}
	sess, p := rawPair(t, cfg)
	p.settings()
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3},
		make([]byte, frame.FloorMaxFrameSize+1))
	expectDead(t, sess, "oversized DATA frame")
}

func TestProtocolBadSettingsValuesAreFatal(t *testing.T) {
	for name, s := range map[string]frame.Setting{
		"bad version":     {ID: frame.SettingVersion, Value: 99},
		"tiny window":     {ID: frame.SettingInitialWindow, Value: 1},
		"huge window":     {ID: frame.SettingInitialWindow, Value: 1 << 40},
		"tiny frame size": {ID: frame.SettingMaxFrameSize, Value: 1},
		"huge frame size": {ID: frame.SettingMaxFrameSize, Value: 1 << 30},
	} {
		t.Run(name, func(t *testing.T) {
			sess, p := rawPair(t, nil)
			payload, _ := frame.AppendSettings(nil, []frame.Setting{s})
			p.send(frame.Header{Type: frame.TypeSettings}, payload)
			expectDead(t, sess, name)
		})
	}
}

// Unknown SETTINGS IDs must be ignored, not fatal: that is the forward
// compatibility the wire format exists to provide.
func TestProtocolUnknownSettingIsIgnored(t *testing.T) {
	sess, p := rawPair(t, nil)
	payload, _ := frame.AppendSettings(nil, []frame.Setting{
		{ID: frame.SettingVersion, Value: frame.ProtocolVersion},
		{ID: 0xbeef, Value: 1234},
	})
	p.send(frame.Header{Type: frame.TypeSettings}, payload)
	expectAlive(t, sess, "unknown setting ID")
}

// Stragglers for a stream that has been torn down are dropped silently, not
// treated as protocol errors: the peer may still have frames in flight.
func TestProtocolStragglersAfterResetAreDropped(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 3}, []byte("a"))
	st, err := sess.AcceptStream(c)
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	// Give the close time to reap the stream, then send late frames.
	time.Sleep(100 * time.Millisecond)
	p.send(frame.Header{Type: frame.TypeData, StreamID: 3}, []byte("late"))
	p.send(frame.Header{Type: frame.TypeWindowUpdate, StreamID: 3}, frame.AppendWindowUpdate(nil, 1<<30))
	p.send(frame.Header{Type: frame.TypeRST, StreamID: 3}, frame.AppendRST(nil, 1, 0))
	expectAlive(t, sess, "stragglers after teardown")
}

// SYNs arriving out of ID order are legal: OpenStream assigns IDs locally, so
// a higher ID can reach the wire first.
func TestProtocolOutOfOrderSYNIsAccepted(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 63}, []byte("late-id"))
	p.send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 1}, []byte("early-id"))

	seen := map[uint64]bool{}
	for i := range 2 {
		st, err := sess.AcceptStream(c)
		if err != nil {
			t.Fatalf("accept %d: %v", i, err)
		}
		seen[st.StreamID()] = true
	}
	if !seen[1] || !seen[63] {
		t.Fatalf("accepted streams %v; want both 1 and 63", seen)
	}
	expectAlive(t, sess, "out-of-order SYN")
}

func TestProtocolPingIsAcked(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()

	type rframe struct {
		h frame.Header
		p []byte
	}
	frames := make(chan rframe, 8)
	// Start reading before constructing the session: net.Pipe rendezvous.
	go func() {
		defer close(frames)
		for {
			var hdr [frame.HeaderSize]byte
			if _, err := io.ReadFull(b, hdr[:]); err != nil {
				return
			}
			h := frame.ParseHeader(hdr[:])
			p := make([]byte, h.Length)
			if h.Length > 0 {
				if _, err := io.ReadFull(b, p); err != nil {
					return
				}
			}
			frames <- rframe{h, p}
		}
	}()

	sess, err := Server(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	next := func() rframe {
		t.Helper()
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatal("peer connection closed")
			}
			return f
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for a frame")
		}
		return rframe{}
	}
	if f := next(); f.h.Type != frame.TypeSettings {
		t.Fatalf("first frame is %v, want SETTINGS", f.h.Type)
	}

	var hdr [frame.HeaderSize]byte
	frame.Header{Type: frame.TypePing, Length: frame.PingLen}.Encode(hdr[:])
	b.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if _, err := b.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write(binary.BigEndian.AppendUint64(nil, 0x1234567890abcdef)); err != nil {
		t.Fatal(err)
	}

	f := next()
	if f.h.Type != frame.TypePing || f.h.Flags&frame.FlagACK == 0 {
		t.Fatalf("got %v flags %v, want PING ACK", f.h.Type, f.h.Flags)
	}
	if got := binary.BigEndian.Uint64(f.p); got != 0x1234567890abcdef {
		t.Fatalf("ping opaque echoed as %#x", got)
	}
}

// GOAWAY must fail blocked opens retriably rather than killing the session
// with an opaque error.
func TestProtocolGoAwayMakesOpenRetriable(t *testing.T) {
	sess, p := rawPair(t, nil)
	p.settings()
	payload, _ := frame.AppendGoAway(nil, 0, CodeNone, []byte("draining"))
	p.send(frame.Header{Type: frame.TypeGoAway}, payload)
	time.Sleep(100 * time.Millisecond)

	_, err := sess.OpenStream(context.Background())
	if !errors.Is(err, ErrGoAway) {
		t.Fatalf("OpenStream after GOAWAY: got %v, want ErrGoAway", err)
	}
}

// netPipe and readFrames give tests a raw view of what a session emits.
func netPipe() (net.Conn, net.Conn) { return net.Pipe() }

type readFrame struct {
	h frame.Header
	p []byte
}

// readFrames decodes frames from conn into a channel, starting immediately so
// a session's handshake is consumed as it is written.
func readFrames(t *testing.T, conn net.Conn) <-chan readFrame {
	t.Helper()
	ch := make(chan readFrame, 16)
	go func() {
		defer close(ch)
		for {
			var hdr [frame.HeaderSize]byte
			if _, err := io.ReadFull(conn, hdr[:]); err != nil {
				return
			}
			h := frame.ParseHeader(hdr[:])
			p := make([]byte, h.Length)
			if h.Length > 0 {
				if _, err := io.ReadFull(conn, p); err != nil {
					return
				}
			}
			ch <- readFrame{h, p}
		}
	}()
	return ch
}

// Data for a stream that has already been torn down must draw a STOP_SENDING,
// not silence (SPEC.md §8 rule 6).
//
// Silence is what the rule used to be, and it stranded peers: a sender that
// has exhausted its window is waiting for credit, and credit for a stream
// that no longer exists never comes. Telling it to stop is the only signal
// that reaches a writer parked on flow control.
func TestProtocolStragglerDataDrawsStopSending(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()

	frames := readFrames(t, b)
	sess, err := Server(a, &Config{KeepaliveInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	send := func(h frame.Header, payload []byte) {
		t.Helper()
		var hdr [frame.HeaderSize]byte
		h.Length = uint32(len(payload))
		h.Encode(hdr[:])
		b.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err := b.Write(hdr[:]); err != nil {
			t.Fatal(err)
		}
		if len(payload) > 0 {
			if _, err := b.Write(payload); err != nil {
				t.Fatal(err)
			}
		}
	}

	settings, _ := frame.AppendSettings(nil, []frame.Setting{
		{ID: frame.SettingVersion, Value: frame.ProtocolVersion},
		{ID: frame.SettingInitialWindow, Value: frame.FloorInitialWindow},
		{ID: frame.SettingMaxFrameSize, Value: frame.FloorMaxFrameSize},
	})
	send(frame.Header{Type: frame.TypeSettings}, settings)

	// Open stream 3 and let the application finish with it, so it is reaped.
	const id = 3
	send(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN | frame.FlagFIN, StreamID: id}, []byte("x"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := sess.AcceptStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	io.ReadAll(st)
	st.Close()

	// Wait for the stream to leave the session's table.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sess.mu.Lock()
		n := len(sess.streams)
		sess.mu.Unlock()
		if n == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	// A late DATA frame for that stream must be answered.
	send(frame.Header{Type: frame.TypeData, StreamID: id}, []byte("late"))

	for {
		select {
		case f, ok := <-frames:
			if !ok {
				t.Fatal("peer closed without answering the straggler")
			}
			if f.h.Type == frame.TypeStopSending && f.h.StreamID == id {
				return // the rule holds
			}
		case <-time.After(5 * time.Second):
			t.Fatal("no STOP_SENDING for data on a torn-down stream; a peer blocked on credit would never learn to stop")
		}
	}
}
