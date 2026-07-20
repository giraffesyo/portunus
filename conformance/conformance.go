// Package conformance is a shared test suite for the mux Session and Stream
// interfaces. It runs unchanged against the native TCP session and against
// the QUIC adapter, which is the only way to know the two really are
// interchangeable — an interface both types satisfy at compile time can still
// behave differently at every semantic that matters.
//
// It imports only the standard library and mux, so the core module stays
// dependency-free.
package conformance

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/giraffesyo/mux"
)

// Pair returns two connected sessions and is called once per subtest. The
// implementation is responsible for cleaning up when the test ends.
type Pair func(t *testing.T) (client, server mux.Session)

// Run executes the suite against one implementation.
func Run(t *testing.T, newPair Pair) {
	t.Helper()
	tests := []struct {
		name string
		fn   func(*testing.T, Pair)
	}{
		{"EchoRoundTrip", testEcho},
		{"BulkIntegrity", testBulk},
		{"HalfClose", testHalfClose},
		{"CancelWriteReachesPeer", testCancelWrite},
		{"CancelReadStopsPeer", testCancelRead},
		{"ReadDeadline", testReadDeadline},
		{"ConcurrentStreams", testConcurrentStreams},
		{"StreamIDsAreDistinct", testStreamIDs},
		{"AcceptHonorsContext", testAcceptContext},
		{"ClosedSessionFailsOperations", testClosedSession},
		{"StreamIsNetConn", testNetConn},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newPair) })
	}
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return c
}

// serveEcho accepts streams and echoes them until the session ends.
func serveEcho(c context.Context, s mux.Session) {
	for {
		st, err := s.AcceptStream(c)
		if err != nil {
			return
		}
		go func(st mux.Stream) {
			io.Copy(st, st)
			st.CloseWrite()
		}(st)
	}
}

// open starts a stream and pushes a byte, so implementations that announce a
// stream lazily have actually done so before the peer is expected to see it.
func open(t *testing.T, c context.Context, s mux.Session) mux.Stream {
	t.Helper()
	st, err := s.OpenStream(c)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if _, err := st.Write([]byte{0}); err != nil {
		t.Fatalf("first write: %v", err)
	}
	return st
}

func accept(t *testing.T, c context.Context, s mux.Session) mux.Stream {
	t.Helper()
	st, err := s.AcceptStream(c)
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(st, buf); err != nil {
		t.Fatalf("reading the opening byte: %v", err)
	}
	return st
}

func testEcho(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)
	go serveEcho(c, server)

	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("hello across any transport")
	if _, err := st.Write(msg); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, msg) {
		t.Fatalf("got %q want %q", got, msg)
	}
}

// Several times any plausible flow-control window, so window updates and
// frame chunking are exercised rather than assumed.
func testBulk(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)
	go serveEcho(c, server)

	payload := make([]byte, 2<<20)
	for i := range payload {
		payload[i] = byte(i*31 + i/512)
	}

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
		t.Fatalf("echoed %d bytes, want %d", len(got), len(payload))
	}
}

func testHalfClose(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)

	cs := open(t, c, client)
	ss := accept(t, c, server)

	// Closing our send side must give the peer EOF, not a reset.
	if err := cs.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatalf("peer read after CloseWrite: %v", err)
	}
	// The reverse direction stays usable.
	if _, err := ss.Write([]byte("reply")); err != nil {
		t.Fatal(err)
	}
	if err := ss.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(cs)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "reply" {
		t.Fatalf("got %q, want %q", got, "reply")
	}
}

func testCancelWrite(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)

	cs := open(t, c, client)
	ss := accept(t, c, server)

	const code = mux.CodeApp + 7
	cs.CancelWrite(code)

	_, err := io.ReadAll(ss)
	var se *mux.StreamError
	if !errors.As(err, &se) {
		t.Fatalf("peer read after CancelWrite: got %v, want *mux.StreamError", err)
	}
	if se.Code != code {
		t.Errorf("error code = %d, want %d", se.Code, code)
	}
	if !se.Remote {
		t.Error("error should be marked remote at the peer")
	}
}

func testCancelRead(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)

	cs := open(t, c, client)
	ss := accept(t, c, server)

	const code = mux.CodeApp + 3
	cs.CancelRead(code)

	// The peer's writes must fail once the cancel lands. Keep writing until
	// one does, rather than assuming a particular delivery timing.
	deadline := time.Now().Add(10 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if _, err = ss.Write(make([]byte, 16<<10)); err != nil {
			break
		}
	}
	var se *mux.StreamError
	if !errors.As(err, &se) {
		t.Fatalf("peer write after CancelRead: got %v, want *mux.StreamError", err)
	}
	if se.Code != code {
		t.Errorf("error code = %d, want %d", se.Code, code)
	}
}

func testReadDeadline(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)

	cs := open(t, c, client)
	_ = accept(t, c, server)

	if err := cs.SetReadDeadline(time.Now().Add(80 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := cs.Read(make([]byte, 16))
	if err == nil {
		t.Fatal("read past its deadline without error")
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("read returned after %v, well before the deadline", elapsed)
	}
	// net.Conn consumers rely on deadline errors being timeouts.
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("deadline error must satisfy net.Error with Timeout() true, got %v (%T)", err, err)
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Logf("note: deadline error does not match os.ErrDeadlineExceeded: %v", err)
	}
}

func testConcurrentStreams(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)
	go serveEcho(c, server)

	const streams = 24
	var wg sync.WaitGroup
	errs := make(chan error, streams)
	for i := range streams {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := client.OpenStream(c)
			if err != nil {
				errs <- err
				return
			}
			want := bytes.Repeat([]byte{byte(i)}, 4<<10)
			go func() {
				st.Write(want)
				st.CloseWrite()
			}()
			got, err := io.ReadAll(st)
			if err != nil {
				errs <- err
				return
			}
			if !bytes.Equal(got, want) {
				errs <- errors.New("stream payload mismatch")
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func testStreamIDs(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)
	go serveEcho(c, server)

	seen := map[uint64]bool{}
	for range 8 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		id := st.StreamID()
		if seen[id] {
			t.Fatalf("stream ID %d issued twice", id)
		}
		seen[id] = true
		st.Close()
	}
}

func testAcceptContext(t *testing.T, newPair Pair) {
	client, _ := newPair(t)
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.AcceptStream(c); err == nil {
		t.Fatal("AcceptStream ignored a cancelled context")
	}
}

func testClosedSession(t *testing.T, newPair Pair) {
	client, _ := newPair(t)
	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	c := ctx(t)
	if _, err := client.OpenStream(c); err == nil {
		t.Error("OpenStream succeeded on a closed session")
	}
	if _, err := client.AcceptStream(c); err == nil {
		t.Error("AcceptStream succeeded on a closed session")
	}
}

// A Stream must be usable anywhere a net.Conn is, which is the point of
// implementing the interface at all.
func testNetConn(t *testing.T, newPair Pair) {
	client, server := newPair(t)
	c := ctx(t)
	go serveEcho(c, server)

	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	var conn net.Conn = st
	if conn.LocalAddr() == nil || conn.RemoteAddr() == nil {
		t.Fatal("a net.Conn must report both addresses")
	}
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetDeadline: %v", err)
	}
	if _, err := conn.Write([]byte("net.Conn")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, len("net.Conn"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "net.Conn" {
		t.Fatalf("got %q", buf)
	}
	conn.Close()
}
