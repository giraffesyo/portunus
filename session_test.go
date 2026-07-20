package mux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// pair returns a connected client/server session over a real loopback TCP
// connection, closing both when the test ends.
func pair(t *testing.T, cfg *Config) (*NativeSession, *NativeSession) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		ch <- res{c, err}
	}()
	cc, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}

	client, err := Client(cc, cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := Server(r.c, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

func ctx(t *testing.T) context.Context {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c
}

// openAccept opens a stream on a and returns both ends. It writes nothing,
// so the caller controls the first frame.
func openAccept(t *testing.T, a, b *NativeSession) (Stream, Stream) {
	t.Helper()
	c := ctx(t)
	as, err := a.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	// SYN rides the first frame, so nudge one byte through to materialize
	// the stream at the peer.
	if _, err := as.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	bs, err := b.AcceptStream(c)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(bs, buf); err != nil {
		t.Fatal(err)
	}
	return as, bs
}

func TestEchoRoundTrip(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

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
	msg := []byte("hello multiplexed world")
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

func TestBulkTransferExceedsWindow(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	// Several times the default 256KB window: forces many window updates.
	const size = 4 << 20
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 7)
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
		t.Fatalf("echo mismatch: got %d bytes want %d", len(got), len(payload))
	}
}

func TestConcurrentStreams(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	const streams = 64
	const perStream = 32 << 10

	go func() {
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go func(st Stream) {
				io.Copy(st, st)
				st.CloseWrite()
			}(st)
		}
	}()

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
			want := bytes.Repeat([]byte{byte(i)}, perStream)
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
				errs <- fmt.Errorf("stream %d: %d bytes, mismatch", i, len(got))
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestHalfCloseKeepsReadSideOpen(t *testing.T) {
	client, server := pair(t, nil)
	cs, ss := openAccept(t, client, server)

	if err := cs.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(ss); err != nil {
		t.Fatal(err)
	}
	// Server can still send after reading EOF.
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
		t.Fatalf("got %q", got)
	}
	// Writing after our own FIN fails.
	if _, err := cs.Write([]byte("x")); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("write after CloseWrite: %v", err)
	}
}

func TestCancelWriteDeliversCodeToPeer(t *testing.T) {
	client, server := pair(t, nil)
	cs, ss := openAccept(t, client, server)

	cs.CancelWrite(CodeApp + 7)

	_, err := io.ReadAll(ss)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("want *StreamError, got %v", err)
	}
	if se.Code != CodeApp+7 || !se.Remote {
		t.Fatalf("got %+v", se)
	}
}

func TestCancelReadStopsPeerWrites(t *testing.T) {
	client, server := pair(t, nil)
	cs, ss := openAccept(t, client, server)

	cs.CancelRead(CodeApp + 3)

	// The peer's writes fail once STOP_SENDING lands. Writing enough to
	// exceed the window guarantees we observe it rather than filling
	// buffers forever.
	deadline := time.Now().Add(5 * time.Second)
	var err error
	for time.Now().Before(deadline) {
		if _, err = ss.Write(make([]byte, 16<<10)); err != nil {
			break
		}
	}
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("want *StreamError from peer write, got %v", err)
	}
	if se.Code != CodeApp+3 || !se.Remote {
		t.Fatalf("got %+v", se)
	}
}

func TestReadDeadline(t *testing.T) {
	client, server := pair(t, nil)
	cs, _ := openAccept(t, client, server)

	if err := cs.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err := cs.Read(make([]byte, 16))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("want deadline error, got %v", err)
	}
	if d := time.Since(start); d < 40*time.Millisecond {
		t.Fatalf("returned too early: %v", d)
	}
	// A net.Conn consumer must see Timeout() == true.
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("deadline error must satisfy net.Error with Timeout(): %v", err)
	}

	// Clearing the deadline restores blocking behavior.
	if err := cs.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := cs.Read(make([]byte, 16))
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("read returned after deadline cleared: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestWriteDeadlineWhenWindowExhausted(t *testing.T) {
	client, server := pair(t, nil)
	cs, _ := openAccept(t, client, server)

	// Fill the peer's window; it never reads, so credit never returns.
	if err := cs.SetWriteDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	_, err := cs.Write(make([]byte, 8<<20))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("want deadline error, got %v", err)
	}
}

func TestAcceptStreamContextCancel(t *testing.T) {
	client, _ := pair(t, nil)
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.AcceptStream(c); !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
}

func TestSessionCloseFailsBlockedCalls(t *testing.T) {
	client, server := pair(t, nil)
	cs, _ := openAccept(t, client, server)

	readErr := make(chan error, 1)
	go func() {
		_, err := cs.Read(make([]byte, 16))
		readErr <- err
	}()
	acceptErr := make(chan error, 1)
	go func() {
		_, err := client.AcceptStream(context.Background())
		acceptErr <- err
	}()
	time.Sleep(20 * time.Millisecond)
	client.Close()

	for _, ch := range []chan error{readErr, acceptErr} {
		select {
		case err := <-ch:
			if err == nil {
				t.Fatal("blocked call returned nil error after Close")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("blocked call did not return after Close")
		}
	}
}

// A terminal session error must not look temporary/timeout to a net.Conn
// consumer: yamux's accept loops hot-looped forever on exactly that.
func TestTerminalErrorsAreNotNetErrors(t *testing.T) {
	client, _ := pair(t, nil)
	client.Close()

	_, err := client.AcceptStream(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if ne, ok := errors.AsType[net.Error](err); ok {
		t.Fatalf("terminal session error satisfies net.Error (Timeout=%v): %v", ne.Timeout(), err)
	}
}

func TestOpenStreamAfterCloseFails(t *testing.T) {
	client, _ := pair(t, nil)
	client.Close()
	if _, err := client.OpenStream(context.Background()); err == nil {
		t.Fatal("OpenStream succeeded on a closed session")
	}
}

func TestShutdownDrainsAndRefusesNewStreams(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)
	cs, ss := openAccept(t, client, server)

	go func() {
		time.Sleep(50 * time.Millisecond)
		cs.Close()
		ss.Close()
	}()

	if err := client.Shutdown(c); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if _, err := client.OpenStream(c); err == nil {
		t.Fatal("OpenStream succeeded after Shutdown")
	}
}

func TestGoAwayFailsLocalStreamsRetriably(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	// A stream the peer has never heard of (no frame sent yet).
	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	server.Close() // sends GOAWAY(lastID = 0)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err = st.Write([]byte("x")); err != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err == nil {
		t.Fatal("write to a stream above GOAWAY lastID never failed")
	}
	if !errors.Is(err, ErrGoAway) && !strings.Contains(err.Error(), "session") {
		t.Fatalf("want retriable GOAWAY-ish error, got %v", err)
	}
}

func TestAcceptBacklogRefusesOverflow(t *testing.T) {
	cfg := &Config{AcceptBacklog: 1, MaxIncomingStreams: 2}
	client, server := pair(t, cfg)
	c := ctx(t)
	_ = server

	// Open more streams than the peer will queue; the surplus is refused
	// with a retriable REFUSED reset rather than killing the session.
	var refused int
	for range 8 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte("hi")); err != nil {
			continue
		}
		st.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if _, err := io.ReadAll(st); errors.Is(err, ErrRefused) {
			refused++
		}
	}
	if refused == 0 {
		t.Fatal("no stream was refused despite an overflowing backlog")
	}
	// The session must still be usable after refusals.
	if err := client.closedErr(); err != nil {
		t.Fatalf("session died on backlog overflow: %v", err)
	}
}

func TestMaxIncomingStreamsSlotHeldUntilAppCloses(t *testing.T) {
	cfg := &Config{MaxIncomingStreams: 4, AcceptBacklog: 16}
	client, server := pair(t, cfg)
	c := ctx(t)

	// Open the maximum, then reset them all from the client. Slots must
	// stay occupied until the server application closes its ends: freeing
	// on peer reset alone is the Rapid Reset hole.
	var accepted []Stream
	for range 4 {
		cs, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cs.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		ss, err := server.AcceptStream(c)
		if err != nil {
			t.Fatal(err)
		}
		accepted = append(accepted, ss)
		cs.CancelWrite(CodeApp)
	}

	time.Sleep(100 * time.Millisecond)
	server.mu.Lock()
	live := server.incoming
	server.mu.Unlock()
	if live != 4 {
		t.Fatalf("incoming slots = %d after peer resets; want 4 (held until app close)", live)
	}

	for _, st := range accepted {
		st.Close()
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		live = server.incoming
		server.mu.Unlock()
		if live == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("incoming slots = %d after app close; want 0", live)
}

func TestStreamIDParityAndSequence(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	cs, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	if cs.StreamID()%2 != 1 {
		t.Fatalf("client stream ID %d is not odd", cs.StreamID())
	}
	ss, err := server.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	if ss.StreamID()%2 != 0 {
		t.Fatalf("server stream ID %d is not even", ss.StreamID())
	}
}

// Streams opened simultaneously from both sides must not collide.
func TestSimultaneousOpenBothDirections(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	echo := func(s *NativeSession) {
		defer wg.Done()
		st, err := s.AcceptStream(c)
		if err != nil {
			errs <- err
			return
		}
		io.Copy(st, st)
		st.CloseWrite()
	}
	wg.Add(2)
	go echo(client)
	go echo(server)

	send := func(s *NativeSession, msg string) {
		defer wg.Done()
		st, err := s.OpenStream(c)
		if err != nil {
			errs <- err
			return
		}
		st.Write([]byte(msg))
		st.CloseWrite()
		got, err := io.ReadAll(st)
		if err != nil {
			errs <- err
			return
		}
		if string(got) != msg {
			errs <- fmt.Errorf("got %q want %q", got, msg)
		}
	}
	wg.Add(2)
	go send(client, "from-client")
	go send(server, "from-server")

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestConfigValidation(t *testing.T) {
	conn, _ := net.Pipe()
	defer conn.Close()
	for _, cfg := range []*Config{
		{InitialWindow: 1 << 10}, // below floor
		{MaxFrameSize: 1 << 10},  // below floor
		{MaxIncomingStreams: -1}, // nonsense
		{AcceptBacklog: -1},      // nonsense
		{MaxFrameSize: 1 << 25},  // above 24-bit wire cap
	} {
		if _, err := Client(conn, cfg); err == nil {
			t.Errorf("accepted invalid config %+v", cfg)
		}
	}
}

// Close must release resources without depending on the peer responding or
// on anyone reading afterwards (the spdystream leak class).
func TestCloseWithUnreadDataAndSilentPeer(t *testing.T) {
	client, server := pair(t, nil)
	cs, ss := openAccept(t, client, server)

	if _, err := ss.Write(make([]byte, 64<<10)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		cs.Close() // data still queued, peer not reading
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close blocked with unread inbound data")
	}
}
