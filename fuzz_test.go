package portunus

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/giraffesyo/portunus/internal/frame"
)

// FuzzWireInput feeds arbitrary bytes to a live session as if they came from
// a peer. A session may reject the input and die — that is the expected
// outcome for most inputs — but it must never panic, never hang, and never
// leave its reader goroutine running.
//
// This complements the receiver-rule tests in protocol_test.go: those assert
// that specific violations are caught, while this asserts that nothing else
// gets through unhandled.
func FuzzWireInput(f *testing.F) {
	// Seeds: a valid handshake, then the shapes most likely to reach deep
	// code paths — a stream that opens and sends, control frames, and
	// deliberately malformed lengths.
	settings, _ := frame.AppendSettings(nil, []frame.Setting{
		{ID: frame.SettingVersion, Value: frame.ProtocolVersion},
		{ID: frame.SettingInitialWindow, Value: frame.FloorInitialWindow},
		{ID: frame.SettingMaxFrameSize, Value: frame.FloorMaxFrameSize},
	})
	handshake := encodeFrame(frame.Header{Type: frame.TypeSettings}, settings)

	f.Add(handshake)
	f.Add(append(handshake,
		encodeFrame(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN, StreamID: 1}, []byte("hello"))...))
	f.Add(append(handshake,
		encodeFrame(frame.Header{Type: frame.TypeData, Flags: frame.FlagSYN | frame.FlagFIN, StreamID: 3}, nil)...))
	f.Add(append(handshake,
		encodeFrame(frame.Header{Type: frame.TypePing}, frame.AppendPing(nil, 42))...))
	f.Add(append(handshake,
		encodeFrame(frame.Header{Type: frame.TypeRST, StreamID: 1}, frame.AppendRST(nil, 7, 0))...))
	f.Add(append(handshake,
		encodeFrame(frame.Header{Type: frame.TypeGoAway}, mustGoAway(0, 0, "bye"))...))
	// A header claiming far more payload than actually follows.
	f.Add(append(append([]byte(nil), handshake...), 0x00, 0, 0, 0, 1, 0xff, 0xff, 0xff))

	f.Fuzz(func(t *testing.T, data []byte) {
		a, b := net.Pipe()
		defer b.Close()

		// Drain whatever the session sends; net.Pipe is synchronous, so an
		// unread peer would deadlock rather than exercise anything.
		go io.Copy(io.Discard, b)

		sess, err := Server(a, &Config{
			// Short timeouts keep a single fuzz iteration quick even when
			// the input wedges the peer.
			WriteTimeout:      500 * time.Millisecond,
			KeepaliveInterval: -1,
		})
		if err != nil {
			t.Fatal(err)
		}

		// An application that accepts and drains, so streams the input
		// opens are actually consumed.
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go func() {
			for {
				st, err := sess.AcceptStream(ctx)
				if err != nil {
					return
				}
				go func(st Stream) {
					io.Copy(io.Discard, st)
					st.Close()
				}(st)
			}
		}()

		// Feed the input, then hang up. Writes may fail once the session
		// rejects something, which is fine — the assertion is on what
		// happens afterwards.
		go func() {
			b.SetWriteDeadline(time.Now().Add(2 * time.Second))
			b.Write(data)
			b.Close()
		}()

		// However the session ends, it must end: a hang here is the bug
		// this fuzzer exists to find. The budget is deliberately tight —
		// inputs are small, and a generous one starves the fuzzing
		// coordinator, which then reports its own deadline as a failure.
		const budget = 2 * time.Second
		done := make(chan struct{})
		go func() {
			sess.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(budget):
			sess.Close()
			t.Fatalf("session neither survived nor terminated within %v", budget)
		}
	})
}

func encodeFrame(h frame.Header, payload []byte) []byte {
	h.Length = uint32(len(payload))
	buf := make([]byte, frame.HeaderSize, frame.HeaderSize+len(payload))
	h.Encode(buf)
	return append(buf, payload...)
}

func mustGoAway(lastID uint32, code uint64, reason string) []byte {
	p, err := frame.AppendGoAway(nil, lastID, code, []byte(reason))
	if err != nil {
		panic(err)
	}
	return p
}

// FuzzStreamPayloads drives real traffic through a session pair and asserts
// the bytes arrive intact. Where FuzzWireInput attacks the parser, this
// attacks the data path: chunking, flow control, and the segment pool must
// preserve payloads of any size and shape.
func FuzzStreamPayloads(f *testing.F) {
	f.Add([]byte(""))
	f.Add([]byte("x"))
	f.Add(make([]byte, frame.FloorMaxFrameSize))   // exactly one max frame
	f.Add(make([]byte, frame.FloorMaxFrameSize+1)) // one byte over
	f.Add(make([]byte, zeroCopyThreshold))         // the copy/zero-copy boundary
	f.Add(make([]byte, zeroCopyThreshold-1))
	f.Add(bytes256KB())

	f.Fuzz(func(t *testing.T, payload []byte) {
		if len(payload) > 1<<20 {
			t.Skip("bounded so an iteration stays quick")
		}
		client, server := fuzzPair(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		go func() {
			st, err := server.AcceptStream(ctx)
			if err != nil {
				return
			}
			io.Copy(st, st)
			st.CloseWrite()
		}()

		st, err := client.OpenStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			st.Write(payload)
			st.CloseWrite()
		}()

		got, err := io.ReadAll(st)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(got) != len(payload) {
			t.Fatalf("echoed %d bytes, want %d", len(got), len(payload))
		}
		for i := range got {
			if got[i] != payload[i] {
				t.Fatalf("byte %d differs: got %d want %d", i, got[i], payload[i])
			}
		}
	})
}

func bytes256KB() []byte {
	b := make([]byte, 256<<10)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// pipePair returns a session pair over net.Pipe.
//
// Preferred over a real TCP pair wherever the carrier is not what is under
// test. Fuzzing at full rate exhausts ephemeral ports within seconds, and the
// resulting dial failures look like crashes; synctest forbids real networking
// outright. Because net.Pipe is a synchronous rendezvous and each constructor
// writes SETTINGS before returning, the two sessions must be built
// concurrently or they deadlock on each other's handshake.
func pipePair(t *testing.T, cfg *Config) (*NativeSession, *NativeSession) {
	t.Helper()
	a, b := net.Pipe()

	type res struct {
		s   *NativeSession
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := Server(b, cfg)
		ch <- res{s, err}
	}()
	client, err := Client(a, cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() { client.Close(); r.s.Close() })
	return client, r.s
}

func fuzzPair(t *testing.T) (*NativeSession, *NativeSession) {
	t.Helper()
	return pipePair(t, &Config{KeepaliveInterval: -1})
}
