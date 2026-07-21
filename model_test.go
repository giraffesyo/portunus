package portunus

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
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
	f.Add([]byte{opOpen, opCopyWrite, 7, opCloseWrite, opDrain})
	f.Add([]byte{opOpen, opRacingWrite, 8, opCancelWrite, 1, opDrain})
	f.Add([]byte{opOpen, opWrite, 4, opDeadline, opClearDeadline, opWrite, 4, opCloseWrite})
	f.Add([]byte{opOpen, opWrite, 3, opReadDeadline, opCloseWrite, opDrain})
	f.Add([]byte{opOpen, opWrite, 5, opCloseWrite, opShutdown, opOpen, opDrain})
	f.Add([]byte{opOpen, opWrite, 3, opCloseWrite, opUseAfterClose, opDrain})
	f.Add([]byte{opOpen, opWrite, 6, opCancelWrite, 2, opUseAfterClose, opUseAfterClose})

	f.Fuzz(func(t *testing.T, script []byte) {
		if len(script) == 0 || len(script) > 256 {
			t.Skip("bounded so an iteration stays quick")
		}
		runModelScript(t, script)
	})
}

// modelWait bounds the checks that can only fail by hanging.
//
// It must stay well under the point where the fuzzing coordinator gives up on
// a worker that has not reported progress. Raising it past that does not make
// the check more patient, it makes the failure unreadable: the worker is
// killed first and the run reports only "fuzzing process hung or terminated
// unexpectedly", with the assertion that would have named the problem never
// reaching the log. Every real finding here surfaced after lowering it.
const modelWait = 5 * time.Second

// modelScriptBytes caps how much one script may write across all its streams.
//
// A fuzzing session runs one worker per core, each executing thousands of
// scripts a second, and every script builds a session pair whose buffers and
// goroutines live until the iteration ends. Without a budget a script can ask
// for megabytes, and the fuzzing process dies of its own weight rather than
// finding anything. The individual sizes still straddle every branch in the
// send path; only the total is bounded.
const modelScriptBytes = 256 << 10

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
	opCopyWrite   // io.Copy into the stream, exercising ReadFrom
	opRacingWrite // a write left in flight while the next op runs
	opClearDeadline
	opReadDeadline
	opShutdown
	opUseAfterClose
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
	closed  bool   // the script issued a terminal op: FIN or reset is on its way
	noWrite bool   // no further writes, though the stream is not yet terminated
	aborted bool   // CancelWrite was called, so the send side can never send a FIN

	// mayRefuse marks a stream the peer is entitled to reject rather than
	// accept. Opening is purely local and a stream announces itself only on
	// its first frame, so one opened before a drain but first written after
	// the peer's GOAWAY arrives is a new stream as far as the peer is
	// concerned, and refusing it is what draining means. The model must
	// allow that outcome rather than wait for a delivery that will not come.
	mayRefuse bool

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
				// io.Copy prefers the source's WriteTo, so draining this
				// way exercises the relay path that a proxy uses, which
				// io.ReadAll would bypass entirely.
				var buf bytes.Buffer
				_, err := io.Copy(&buf, st)
				data := buf.Bytes()
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
		streams      []*modelStream
		cur          int
		budget       = modelScriptBytes
		d            = decoder{b: script}
		racing       sync.WaitGroup
		draining     bool
		shutdownDone chan error
	)

	for d.more() {
		switch d.next() % opCount {
		case opOpen:
			if len(streams) >= 6 {
				continue
			}
			st, err := client.OpenStream(ctx)
			if err != nil {
				if draining {
					continue // refusing new streams is the point of a drain
				}
				t.Fatalf("OpenStream failed on a live session: %v", err)
			}
			streams = append(streams, &modelStream{
				st: st, id: st.StreamID(), clean: true, mayRefuse: draining,
			})
			cur = len(streams) - 1

		case opWrite:
			size := writeSizes[int(d.next())%len(writeSizes)]
			ms := pick(streams, cur)
			if ms == nil || ms.closed || ms.noWrite {
				continue
			}
			if size > budget {
				size = budget
			}
			budget -= size
			payload := patternFor(ms, size)
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
			ms.aborted = true

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
			// Close half-closes the send side cleanly and cancels the read
			// side; it does not abandon sending, so a later CloseWrite is
			// idempotent rather than an error.
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

		case opCopyWrite:
			size := writeSizes[int(d.next())%len(writeSizes)]
			ms := pick(streams, cur)
			if ms == nil || ms.closed || ms.noWrite {
				continue
			}
			if size > budget {
				size = budget
			}
			budget -= size
			payload := patternFor(ms, size)
			// io.Copy prefers the destination's ReadFrom, so this reaches
			// the send-side relay path that plain Write does not.
			n, err := io.Copy(ms.st, bytes.NewReader(payload))
			ms.written = append(ms.written, payload[:n]...)
			if n > 0 {
				ms.announced = true
			}
			if err != nil {
				ms.clean = false
			}

		case opRacingWrite:
			size := writeSizes[int(d.next())%len(writeSizes)]
			ms := pick(streams, cur)
			if ms == nil || ms.closed || ms.noWrite {
				continue
			}
			if size > budget {
				size = budget
			}
			budget -= size
			payload := patternFor(ms, size)
			// Left in flight deliberately: a write racing whatever comes
			// next is how the worst defect in this package was produced,
			// a cancel and a first write resolving in the wrong order.
			//
			// What it achieved is recorded by the write itself rather than
			// assumed. Assuming it announced the stream is wrong in the
			// case this op exists to reach: when the cancel wins, nothing
			// is sent and the peer never learns the stream existed, so
			// waiting for a result from it would hang. No further writes
			// follow, so appending its bytes on completion keeps them in
			// order, and everything the model reads is read after the wait.
			racing.Add(1)
			go func() {
				defer racing.Done()
				n, _ := ms.st.Write(payload)
				ms.written = append(ms.written, payload[:n]...)
				if n > 0 {
					ms.announced = true
				}
			}()
			ms.clean = false
			// Not "closed": nothing terminal has been sent. Conflating the
			// two would let the script finish without ever issuing a FIN or
			// a reset, leaving the peer reading a stream that never ends.
			ms.noWrite = true

		case opClearDeadline:
			if ms := pick(streams, cur); ms != nil {
				ms.st.SetDeadline(time.Time{})
			}

		case opReadDeadline:
			if ms := pick(streams, cur); ms != nil {
				ms.st.SetReadDeadline(time.Now().Add(-time.Millisecond))
				ms.clean = false
			}

		case opShutdown:
			// Draining runs in the background: it waits for live streams,
			// which this script closes at the end. Opens may legitimately
			// fail from here on.
			if !draining {
				draining = true
				// Anything not yet on the wire may arrive at the peer after
				// its GOAWAY and be refused.
				for _, ms := range streams {
					if !ms.announced {
						ms.mayRefuse = true
					}
				}
				shutdownDone = make(chan error, 1)
				go func() {
					sctx, scancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer scancel()
					shutdownDone <- client.Shutdown(sctx)
				}()
			}

		case opUseAfterClose:
			// Operating on a finished stream must fail cleanly, never
			// succeed and never touch another stream's state. This is the
			// property that recycling stream objects would put at risk, so
			// it is asserted before any such change rather than after.
			ms := pick(streams, cur)
			if ms == nil || !ms.closed {
				continue
			}
			if n, err := ms.st.Write([]byte("after close")); err == nil {
				t.Fatalf("stream %d: write succeeded after close (%d bytes)", ms.id, n)
			}
			// Half-closing an abandoned send side must report the
			// abandonment. Half-closing an already half-closed stream must
			// not: that is idempotent and returns nil, which is why the
			// check is on abandonment rather than on the stream merely
			// having seen an error.
			if ms.aborted {
				if err := ms.st.CloseWrite(); err == nil {
					t.Fatalf("stream %d: CloseWrite reported success after the send side was abandoned", ms.id)
				}
			}
			// These are defined as idempotent, so they must not panic or
			// wedge; nothing is asserted about their effect.
			ms.st.CancelWrite(CodeApp)
			ms.st.CancelRead(CodeApp)
			ms.st.Close()
			ms.st.SetDeadline(time.Now().Add(time.Second))

		case opNextStream:
			if len(streams) > 0 {
				cur = (cur + 1) % len(streams)
			}

		case opDrain:
			// Nothing to do: the peer drains continuously. Present so the
			// mutator can vary op alignment cheaply.
		}
	}

	// Writes left in flight must land before the model is compared, or a
	// prefix check would race the very thing it is checking.
	racing.Wait()

	// Finish every stream so the peer's drain can terminate.
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
		if ms.announced && !ms.mayRefuse {
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
			checkDelivery(t, ms, got.observed, draining)
		case <-time.After(modelWait):
			// Property 4: liveness.
			t.Fatal("a stream never finished draining at the peer")
		}
	}

	// Property 2: survival. Nothing above is a protocol violation or an
	// abuse of the peer, so a session must only end where the script asked
	// for it.
	if !draining {
		if err := client.closedErr(); err != nil {
			t.Fatalf("client session died during legal operations: %v", err)
		}
		if err := server.closedErr(); err != nil {
			t.Fatalf("server session died during legal operations: %v", err)
		}
	}

	// Property 3: accounting. Every stream is finished, so once both sides
	// reap them the receive budget must return to zero.
	for _, ms := range streams {
		ms.st.Close()
	}

	// Property 5: a drain finishes. Checked after the closes above, because
	// Shutdown waits for exactly those streams; waiting on it first is a
	// deadlock that only resolves on a timeout.
	if draining {
		select {
		case <-shutdownDone:
		case <-time.After(modelWait):
			t.Fatal("Shutdown never returned after every stream closed")
		}
	}

	if !draining {
		// A drained session closes its carrier, which reaps streams by a
		// different route; the accounting claim is about ordinary use.
		checkBudgetDrains(t, client, "client")
		checkBudgetDrains(t, server, "server")
	}
}

// checkDelivery compares what arrived against what the model says was sent.
func checkDelivery(t *testing.T, ms *modelStream, got observed, draining bool) {
	t.Helper()
	if ms.clean && !draining {
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
	if got.err == nil || errors.Is(got.err, io.EOF) {
		return
	}
	var se *StreamError
	if errors.As(got.err, &se) {
		return
	}
	// A session-level ending is legitimate only where the script asked for
	// one: draining closes the carrier, and every stream still being read
	// then ends with a session error rather than a stream error. Outside a
	// drain it would mean the session died on its own, which the survival
	// property forbids.
	var sess *SessionError
	if draining && errors.As(got.err, &sess) {
		return
	}
	t.Fatalf("stream %d: peer read ended with %v, want a *StreamError or EOF", ms.id, got.err)
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

// patternFor builds a payload whose bytes depend on the stream's position in
// its own byte sequence, so a delivery check catches reordering and
// duplication, not merely a wrong length.
func patternFor(ms *modelStream, size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(len(ms.written) + i)
	}
	return payload
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
