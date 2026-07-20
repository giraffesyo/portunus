package mux

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"
)

// io.Copy picks WriteTo automatically; assert it delivers the same bytes and
// reports a clean EOF as success.
func TestWriteToDeliversAllBytes(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	payload := make([]byte, 1<<20)
	for i := range payload {
		payload[i] = byte(i * 31)
	}

	go func() {
		st, err := client.OpenStream(c)
		if err != nil {
			return
		}
		st.Write(payload)
		st.CloseWrite()
	}()

	ss, err := server.AcceptStream(c)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	n, err := ss.(*NativeStream).WriteTo(&got)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != int64(len(payload)) || !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("WriteTo delivered %d bytes, want %d (equal=%v)", n, len(payload), bytes.Equal(got.Bytes(), payload))
	}
}

func TestReadFromSendsAllBytes(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	payload := make([]byte, 512<<10)
	for i := range payload {
		payload[i] = byte(i * 17)
	}

	cs, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		n, err := cs.(*NativeStream).ReadFrom(bytes.NewReader(payload))
		if err != nil || n != int64(len(payload)) {
			t.Errorf("ReadFrom: %d bytes, %v", n, err)
		}
		cs.CloseWrite()
	}()

	ss, err := server.AcceptStream(c)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(ss)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("got %d bytes, want %d", len(got), len(payload))
	}
}

// The relay path is the design's headline workload: segments received on one
// session are handed to a stream on another. Ownership must transfer cleanly
// in both directions of a bidirectional tunnel.
func TestBidirectionalRelayBetweenSessions(t *testing.T) {
	checkLeaks(t)
	// Two independent sessions, bridged by a proxy goroutine pair.
	frontClient, frontServer := pair(t, nil)
	backClient, backServer := pair(t, nil)
	c := ctx(t)

	// Backend: echo.
	go func() {
		for {
			st, err := backServer.AcceptStream(c)
			if err != nil {
				return
			}
			go func(st Stream) {
				io.Copy(st, st)
				st.CloseWrite()
			}(st)
		}
	}()

	// Proxy: front-server stream <-> back-client stream, copied both ways.
	go func() {
		for {
			in, err := frontServer.AcceptStream(c)
			if err != nil {
				return
			}
			out, err := backClient.OpenStream(c)
			if err != nil {
				return
			}
			go func() {
				io.Copy(out, in) // upstream: segments cross sessions
				out.CloseWrite()
			}()
			go func() {
				io.Copy(in, out) // downstream
				in.CloseWrite()
			}()
		}
	}()

	payload := bytes.Repeat([]byte("relay payload; "), 40000) // ~600KB
	st, err := frontClient.OpenStream(c)
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
		t.Fatalf("relayed %d bytes, want %d", len(got), len(payload))
	}
}

// A mid-relay reset must not strand segments or their flow-control credit:
// the upstream sender has to keep making progress afterwards.
func TestRelayResetMidStreamReleasesCredit(t *testing.T) {
	checkLeaks(t)
	client, server := pair(t, nil)
	c := ctx(t)

	cs, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Write([]byte("start")); err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(c)
	if err != nil {
		t.Fatal(err)
	}

	// Queue data the server never reads, then cancel its read side. The
	// credit for the discarded bytes must be returned or the writer stalls.
	go func() {
		cs.Write(make([]byte, 512<<10))
		cs.CloseWrite()
	}()
	time.Sleep(50 * time.Millisecond)
	ss.CancelRead(CodeApp + 9)

	// The writer must terminate rather than block forever on a dead window.
	done := make(chan struct{})
	go func() {
		defer close(done)
		cs.SetWriteDeadline(time.Now().Add(3 * time.Second))
		for range 64 {
			if _, err := cs.Write(make([]byte, 32<<10)); err != nil {
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("writer stalled after peer canceled its read side (credit leak)")
	}
}

// WriteTo must surface a reset as a *StreamError rather than a clean EOF, or
// io.Copy would report a truncated transfer as success.
func TestWriteToReportsResetNotEOF(t *testing.T) {
	client, server := pair(t, nil)
	cs, ss := openAccept(t, client, server)

	go func() {
		time.Sleep(50 * time.Millisecond)
		cs.CancelWrite(CodeApp + 11)
	}()

	_, err := ss.(*NativeStream).WriteTo(io.Discard)
	var se *StreamError
	if !errors.As(err, &se) {
		t.Fatalf("WriteTo after peer reset: got %v, want *StreamError", err)
	}
	if !se.Remote || se.Code != CodeApp+11 {
		t.Fatalf("got %+v", se)
	}
}

// Segments are pooled and never zeroed on reuse, so a stream must expose
// exactly its own written bytes and no more. Recycling buffers that carried
// larger payloads would otherwise surface another stream's data.
func TestPooledSegmentsExposeOnlyTheirOwnBytes(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)

	echo := func(st Stream) {
		io.Copy(st, st)
		st.CloseWrite()
	}
	go func() {
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go echo(st)
		}
	}()

	// Round-trip shrinking payloads with distinct fills through the same
	// pool classes: each reply must match its request exactly, with no
	// trailing bytes from the larger payload that used the buffer before.
	for i, size := range []int{60 << 10, 32 << 10, 900, 16 << 10, 100, 8 << 10} {
		want := bytes.Repeat([]byte{byte(i + 1)}, size)
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		go func() {
			st.Write(want)
			st.CloseWrite()
		}()
		got, err := io.ReadAll(st)
		if err != nil {
			t.Fatalf("size %d: %v", size, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("size %d: echoed %d bytes, want %d (fill %d)", size, len(got), size, i+1)
		}
		st.Close()
	}
}
