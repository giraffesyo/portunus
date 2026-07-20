package portunus

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// FuzzProtocolModel drives randomized operation interleavings against a real
// session pair while maintaining a model of what should have happened, then
// checks the implementation against the model.
//
// This is the check the input fuzzers cannot make. FuzzWireInput attacks the
// parser with arbitrary bytes and asserts only that nothing panics or hangs;
// FuzzStreamPayloads attacks the data path with one payload at a time. Neither
// says anything about what happens when opens, writes, half-closes, resets,
// cancels, and deadlines interleave across several streams — and that is where
// the defects actually were. Of the thirteen findings in the first code review
// of this package, the two most severe were a flow-control credit leak on a
// failed write and a zero-copy writer that returned before its flush; both are
// accounting and sequencing bugs invisible to an input fuzzer and caught by
// the invariants below.
//
// Four properties are asserted:
//
//  1. Delivery. A stream closed cleanly delivers exactly the bytes written,
//     in order. A stream that errored delivers a prefix of them — never
//     reordered, never invented.
//  2. Survival. Legal API calls never kill a session. This is what catches an
//     abuse limiter tuned so tightly that ordinary traffic trips it, which has
//     happened here twice.
//  3. Accounting. Once every stream is reaped, the session's receive budget
//     returns to zero. A leak means credit was reserved and never released.
//  4. Liveness. Every operation completes; the run finishes inside its budget.
func FuzzProtocolModel(f *testing.F) {
	// Seeds cover the shapes worth reaching immediately: a clean transfer,
	// a reset mid-stream, interleaved streams, and a payload straddling the
	// copy/zero-copy boundary.
	f.Add([]byte{opOpen, opWrite, 3, opCloseWrite, opDrain})
	f.Add([]byte{opOpen, opWrite, 8, opCancelWrite, opDrain})
	f.Add([]byte{opOpen, opOpen, opWrite, 5, opNextStream, opWrite, 6, opCloseWrite, opDrain})
	f.Add([]byte{opOpen, opWrite, 4, opCancelRead, opCloseWrite, opDrain})
	f.Add([]byte{opOpen, opDeadline, opWrite, 9, opCloseWrite, opDrain})

	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) == 0 || len(script) > 256 {
			t.Skip("bounded so an iteration stays quick")
		}
		runModelScript(t, script)
	})
}

// Opcodes. Values are the raw bytes a fuzz input is decoded from, so the
// mutator can reach every operation by flipping a single byte.
const (
	opOpen = iota
	opWrite
	opCloseWrite
	opCancelWrite
	opCancelRead
	opClose
	opDeadline
	opNextStream
	opDrain
	opCount
)

// writeSizes straddle every boundary the send path branches on: empty, tiny,
// either side of the zero-copy threshold, and either side of a max frame.
var writeSizes = []int{0, 1, 63, 4095, 4096, 4097, 16 << 10, 65535, 65536, 70000}

// modelStream is what the script says should have happened to one stream.
type modelStream struct {
	st      Stream
	id      uint64
	written []byte // bytes Write reported as accepted
	clean   bool   // every operation on it succeeded
	closed  bool   // send side finished, so the peer will see EOF or a reset

	// announced records that at least one frame for this stream reached the
	// send path, which is what makes the peer aware of it at all. A stream
	// opened and cancelled before it ever writes is invisible to the peer by
	// design — the reset is dropped rather than sent, since announcing a
	// stream only to reset it would oblige the peer to treat the reset as a
	// protocol error. Expecting a peer-side result for such a stream is a
	// bug in the model, not the implementation.
	announced bool
}

// observed is what the peer actually received on a stream.
type observed struct {
	data []byte
	err  error
}

func runModelScript(t *testing.T, script []byte) {
	t.Helper()
	client, server := modelPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// The peer is a pure sink: it drains each stream to completion and
	// records what arrived. Echoing would make the model depend on the
	// peer's own scheduling, which is not what is under test here.
	results := make(chan struct {
		id uint64
		observed
	}, 64)
	go func() {
		for {
			st, err := server.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func(st Stream) {
				data, err := io.ReadAll(st)
				select {
				case results <- struct {
					id uint64
					observed
				}{st.StreamID(), observed{data, err}}:
				case <-ctx.Done():
				}
				st.Close()
			}(st)
		}
	}()

	var (
		streams []*modelStream
		cur     int
		d       = decoder{b: script}
	)

	for d.more() {
		switch d.next() % opCount {
		case opOpen:
			if len(streams) >= 6 {
				continue
			}
			st, err := client.OpenStream(ctx)
			if err != nil {
				// Only a dying or draining session may refuse, and
				// nothing in this script drains one.
				t.Fatalf("OpenStream failed on a live session: %v", err)
			}
			streams = append(streams, &modelStream{st: st, id: st.StreamID(), clean: true})
			cur = len(streams) - 1

		case opWrite:
			size := writeSizes[int(d.next())%len(writeSizes)]
			ms := pick(streams, cur)
			if ms == nil || ms.closed {
				continue
			}
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(len(ms.written) + i)
			}
			n, err := ms.st.Write(payload)
			// Record what was accepted, not what was offered: a write
			// can legally report a short count with an error.
			ms.written = append(ms.written, payload[:n]...)
			if n > 0 {
				ms.announced = true
			}
			if err != nil {
				ms.clean = false
			}

		case opCloseWrite:
			ms := pick(streams, cur)
			if ms == nil || ms.closed {
				continue
			}
			if err := ms.st.CloseWrite(); err != nil {
				ms.clean = false
			} else {
				ms.announced = true // FIN carries SYN if nothing else did
			}
			ms.closed = true

		case opCancelWrite:
			code := CodeApp + uint64(d.next())
			ms := pick(streams, cur)
			if ms == nil || ms.closed {
				continue
			}
			ms.st.CancelWrite(code)
			ms.clean = false
			ms.closed = true

		case opCancelRead:
			code := CodeApp + uint64(d.next())
			if ms := pick(streams, cur); ms != nil {
				ms.st.CancelRead(code)
			}

		case opClose:
			ms := pick(streams, cur)
			if ms == nil || ms.closed {
				continue
			}
			ms.st.Close()
			ms.clean = false
			ms.closed = true

		case opDeadline:
			// A deadline in the past forces the copy path and the
			// failed-admission path, where the credit leak lived.
			if ms := pick(streams, cur); ms != nil {
				ms.st.SetWriteDeadline(time.Now().Add(-time.Millisecond))
				ms.clean = false
			}

		case opNextStream:
			if len(streams) > 0 {
				cur = (cur + 1) % len(streams)
			}

		case opDrain:
			// Nothing to do: the peer drains continuously. Present so the
			// mutator can vary op alignment cheaply.
		}
	}

	// Finish every stream so the peer's ReadAll can terminate.
	expected := 0
	for _, ms := range streams {
		if !ms.closed {
			if err := ms.st.CloseWrite(); err != nil {
				ms.clean = false
			} else {
				ms.announced = true
			}
			ms.closed = true
		}
		if ms.announced {
			expected++
		}
	}

	// Property 1: delivery.
	byID := make(map[uint64]*modelStream, len(streams))
	for _, ms := range streams {
		byID[ms.id] = ms
	}
	for range expected {
		select {
		case got := <-results:
			ms, ok := byID[got.id]
			if !ok {
				t.Fatalf("peer reported stream %d that was never opened", got.id)
			}
			checkDelivery(t, ms, got.observed)
		case <-time.After(15 * time.Second):
			// Property 4: liveness.
			t.Fatal("a stream never finished draining at the peer")
		}
	}

	// Property 2: survival. Nothing above is a protocol violation or an
	// abuse of the peer, so both sessions must still be healthy.
	if err := client.closedErr(); err != nil {
		t.Fatalf("client session died during legal operations: %v", err)
	}
	if err := server.closedErr(); err != nil {
		t.Fatalf("server session died during legal operations: %v", err)
	}

	// Property 3: accounting. Every stream is finished, so once both sides
	// reap them the receive budget must return to zero.
	for _, ms := range streams {
		ms.st.Close()
	}
	checkBudgetDrains(t, client, "client")
	checkBudgetDrains(t, server, "server")
}

// checkDelivery compares what arrived against what the model says was sent.
func checkDelivery(t *testing.T, ms *modelStream, got observed) {
	t.Helper()
	if ms.clean {
		if got.err != nil {
			t.Fatalf("stream %d: cleanly closed but peer read failed: %v", ms.id, got.err)
		}
		if !bytes.Equal(got.data, ms.written) {
			t.Fatalf("stream %d: peer received %d bytes, wrote %d; content equal=%v",
				ms.id, len(got.data), len(ms.written), bytes.Equal(got.data, ms.written))
		}
		return
	}
	// A stream that hit an error may be truncated, but whatever did arrive
	// must be an exact prefix: never reordered, never fabricated, never
	// carrying bytes from another stream's pooled buffer.
	if len(got.data) > len(ms.written) {
		t.Fatalf("stream %d: peer received %d bytes, more than the %d written",
			ms.id, len(got.data), len(ms.written))
	}
	if !bytes.Equal(got.data, ms.written[:len(got.data)]) {
		t.Fatalf("stream %d: received bytes are not a prefix of those written", ms.id)
	}
	var se *StreamError
	if got.err != nil && !errors.As(got.err, &se) && !errors.Is(got.err, io.EOF) {
		t.Fatalf("stream %d: peer read ended with %v, want a *StreamError or EOF", ms.id, got.err)
	}
}

// checkBudgetDrains waits for streams to be reaped and asserts no receive
// credit was left reserved. A leak here is credit taken and never returned,
// which shrinks the usable window permanently.
func checkBudgetDrains(t *testing.T, s *NativeSession, side string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		live := len(s.streams)
		s.mu.Unlock()
		if live == 0 && s.budget.outstanding() == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	s.mu.Lock()
	live := len(s.streams)
	s.mu.Unlock()
	t.Fatalf("%s: %d streams still live and %d bytes of receive credit outstanding after every stream finished",
		side, live, s.budget.outstanding())
}

func pick(streams []*modelStream, cur int) *modelStream {
	if len(streams) == 0 {
		return nil
	}
	return streams[cur%len(streams)]
}

// modelPair is an in-memory session pair with keepalive off, so iterations
// are neither perturbed by timers nor limited by ephemeral ports.
func modelPair(t *testing.T) (*NativeSession, *NativeSession) {
	t.Helper()
	a, b := net.Pipe()
	cfg := &Config{KeepaliveInterval: -1}

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

// decoder turns fuzz input into an operation script.
type decoder struct {
	b []byte
	i int
}

func (d *decoder) more() bool { return d.i < len(d.b) }

func (d *decoder) next() byte {
	if d.i >= len(d.b) {
		return 0
	}
	v := d.b[d.i]
	d.i++
	return v
}
