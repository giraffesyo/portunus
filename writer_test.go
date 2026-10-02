package portunus

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/giraffesyo/portunus/internal/frame"
)

// pinFlusher claims flusher duty without flushing, so appends stage and stay
// staged where a test can look at them. It waits for any flush the reader
// started (a PING ACK, a window update) to finish first. The returned
// function gives the duty back and drains what was staged.
func pinFlusher(t *testing.T, w *writer) (release func()) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		w.mu.Lock()
		if !w.flushing.Load() {
			break
		}
		w.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("flusher never went idle")
		}
		time.Sleep(time.Millisecond)
	}
	w.flushing.Store(true)
	w.mu.Unlock()
	return func() {
		w.mu.Lock()
		w.flushing.Store(false)
		flush := w.claimFlushLocked()
		w.mu.Unlock()
		if flush {
			w.flushLoop()
		}
	}
}

// settled waits for the peer's SETTINGS to be applied. Until then the session
// frames at the protocol floor, and a test that sizes its writes from the
// frame size would be reading a value about to change under it.
func settled(t *testing.T, s *NativeSession) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for s.peerMaxFrame.Load() == frame.FloorMaxFrameSize {
		if time.Now().After(deadline) {
			t.Fatal("peer SETTINGS never arrived")
		}
		time.Sleep(time.Millisecond)
	}
}

// announced opens a stream and gets its SYN onto the wire and accepted, so
// its later frames take the data lanes. It returns both ends.
func announced(t *testing.T, client, server *NativeSession) (*NativeStream, Stream) {
	t.Helper()
	settled(t, client)
	c := ctx(t)
	st, err := client.OpenStream(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	ss, err := server.AcceptStream(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(ss, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	return st.(*NativeStream), ss
}

// staged waits until st has put the wanted number of bytes, framing included,
// into the pending batch. It looks at the stream's own share rather than the
// batch as a whole, which the reader's control frames also add to.
func staged(t *testing.T, w *writer, st *NativeStream, bytes int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		w.mu.Lock()
		n := 0
		if st.batchSeq.Load() == w.seq+1 {
			n = st.batchBytes
		}
		w.mu.Unlock()
		if n == bytes {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream has %d bytes staged, want %d", n, bytes)
		}
		time.Sleep(time.Millisecond)
	}
}

// A write spanning several frames, with the batch to itself, must stage them
// together rather than paying a flush per frame.
func TestMultiFrameWriteSharesOneBatch(t *testing.T) {
	client, server := pair(t, nil)
	st, ss := announced(t, client, server)
	w := client.w
	maxf := int(client.peerMaxFrame.Load())

	release := pinFlusher(t, w)
	w.mu.Lock()
	w.burst = 3
	w.mu.Unlock()

	// Five frames' worth offered; the burst allows three.
	payload := make([]byte, 5*maxf)
	for i := range payload {
		payload[i] = byte(i * 7)
	}
	took := make(chan int, 1)
	go func() {
		n, err := w.appendData(st, payload, 0, false, nil, nil, false)
		if err != nil {
			t.Error(err)
		}
		took <- n
	}()
	staged(t, w, st, 3*(maxf+frame.HeaderSize))

	// The writer is parked on its referenced payload until the batch
	// resolves, and must then report exactly what was staged.
	select {
	case n := <-took:
		t.Fatalf("zero-copy write returned %d before its batch was flushed", n)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	if n := <-took; n != 3*maxf {
		t.Fatalf("took %d bytes, want %d", n, 3*maxf)
	}

	got := make([]byte, 3*maxf)
	if _, err := io.ReadFull(ss, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload[:3*maxf]) {
		t.Fatal("frames staged together did not arrive as the bytes written")
	}
}

// With another stream in the batch, a long write gets its configured share
// and no more, however large the uncontended burst has grown.
func TestMultiFrameWriteYieldsUnderContention(t *testing.T) {
	client, server := pair(t, nil)
	bulk, _ := announced(t, client, server)
	other, _ := announced(t, client, server)
	w := client.w
	maxf := int(client.peerMaxFrame.Load())

	release := pinFlusher(t, w)
	defer release()
	w.mu.Lock()
	w.burst = 6
	w.mu.Unlock()

	if _, err := w.appendData(other, make([]byte, 100), 0, true, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	go w.appendData(bulk, make([]byte, 3*maxf), 0, false, nil, nil, false)

	// The default share is two frames of payload, which with framing holds
	// one: the same single frame per batch a contended stream got before.
	staged(t, w, bulk, maxf+frame.HeaderSize)
}

// A control frame in the batch is not competition. The reader stages window
// updates and PING replies all the time on a busy session, and a lone bulk
// stream that fell back to its contended share whenever one was pending
// would rarely get its burst.
func TestMultiFrameWriteIgnoresPendingControl(t *testing.T) {
	client, server := pair(t, nil)
	st, _ := announced(t, client, server)
	w := client.w
	maxf := int(client.peerMaxFrame.Load())

	release := pinFlusher(t, w)
	defer release()
	w.mu.Lock()
	w.burst = 3
	w.mu.Unlock()

	var ping [frame.PingLen]byte
	w.appendControlAsync(frame.Header{Type: frame.TypePing, Flags: frame.FlagACK}, ping[:])
	go w.appendData(st, make([]byte, 3*maxf), 0, false, nil, nil, false)
	staged(t, w, st, 3*(maxf+frame.HeaderSize))
}

// The uncontended burst follows how fast the carrier absorbs flushes.
func TestBurstAdaptsToFlushDuration(t *testing.T) {
	client, _ := pair(t, nil)
	settled(t, client)
	w := client.w
	maxf := int(client.peerMaxFrame.Load())
	limit := w.bulkCap() / (maxf + frame.HeaderSize)

	w.mu.Lock()
	defer w.mu.Unlock()
	w.burst = 1

	// Fast flushes that exercised the current size double it, up to what a
	// batch can hold.
	for range 10 {
		w.adaptBurstLocked(w.burst*maxf, burstFast/2)
	}
	if w.burst != limit {
		t.Fatalf("burst %d after fast flushes, want the batch limit %d", w.burst, limit)
	}
	// A fast flush too small to have tested the burst proves nothing.
	w.burst = 4
	w.adaptBurstLocked(100, burstFast/2)
	if w.burst != 4 {
		t.Fatalf("burst %d after a small fast flush, want 4", w.burst)
	}
	// In between, it holds.
	w.adaptBurstLocked(4*maxf, (burstFast+burstSlow)/2)
	if w.burst != 4 {
		t.Fatalf("burst %d after a middling flush, want 4", w.burst)
	}
	// A slow carrier halves it, down to one bulk frame per flush.
	for range 5 {
		w.adaptBurstLocked(maxf, 2*burstSlow)
	}
	if w.burst != 1 {
		t.Fatalf("burst %d after slow flushes, want 1", w.burst)
	}
	// And the limit bulk is admitted against follows it, never dropping
	// below the one frame that must always be admissible.
	if got, want := w.bulkLimitLocked(), maxf+frame.HeaderSize; got != want {
		t.Fatalf("bulk limit %d on a slow carrier, want one frame (%d)", got, want)
	}
	w.burst = 1 << 20
	if got := w.bulkLimitLocked(); got != w.bulkCap() {
		t.Fatalf("bulk limit %d on a fast carrier, want the cap %d", got, w.bulkCap())
	}
	w.burst = 1
	if hint := int(w.takeHint.Load()); hint < maxf {
		t.Fatalf("take hint %d below one frame", hint)
	}
}

// A swap wakes waiters in arrival order and only as many as the new batch
// can hold; the rest keep their places.
func TestAdmissionWakeIsOrderedAndBounded(t *testing.T) {
	client, _ := pair(t, nil)
	settled(t, client)
	w := client.w
	need := int(client.peerMaxFrame.Load()) + frame.HeaderSize
	const fit = 4

	waiters := make([]*NativeStream, fit+5)
	w.mu.Lock()
	defer w.mu.Unlock()
	w.burst = fit // the batch currently takes four bulk frames
	for i := range waiters {
		st := &NativeStream{admitCh: make(chan struct{}, 1), admitNeed: need}
		waiters[i] = st
		w.enqueueAdmitLocked(st, false)
	}
	w.wakeAdmitLocked()

	for i, st := range waiters {
		woken := len(st.admitCh) == 1
		if want := i < fit; woken != want {
			t.Fatalf("waiter %d of %d: woken=%v, want %v (batch fits %d)", i, len(waiters), woken, want, fit)
		}
		if st.admitQueued == woken {
			t.Fatalf("waiter %d: queued=%v disagrees with woken=%v", i, st.admitQueued, woken)
		}
	}
	if len(w.admitQ) != 5 || w.admitQ[0] != waiters[fit] {
		t.Fatalf("queue holds %d after the wake, want the 5 unwoken in order", len(w.admitQ))
	}
	// A woken waiter that found no room goes back to the head, not the tail.
	w.enqueueAdmitLocked(waiters[0], true)
	if w.admitQ[0] != waiters[0] || w.admitQ[1] != waiters[fit] {
		t.Fatal("returning waiter did not rejoin at the head")
	}
	// One that gives up leaves the queue without disturbing the others.
	if !w.dequeueAdmitLocked(waiters[fit]) || w.dequeueAdmitLocked(waiters[fit]) {
		t.Fatal("dequeue did not report queued-then-gone")
	}
	if len(w.admitQ) != 5 || w.admitQ[1] != waiters[fit+1] {
		t.Fatal("dequeue disturbed the order of the remaining waiters")
	}
	w.admitQ = w.admitQ[:0]
}

// A wake handed to a writer that then stages nothing must be passed on.
// Waiters are only woken by a batch being swapped out, and a swap only
// happens with something staged, so a woken writer that leaves empty-handed
// — here because its stream was cancelled while it was parked — would
// otherwise strand everyone queued behind it with nothing left to wake them.
func TestAdmissionWakeIsPassedOn(t *testing.T) {
	// One frame per batch, so a swap wakes exactly one waiter.
	cfg := &Config{MaxBatchBytes: 64<<10 + frame.HeaderSize, InitialWindow: 4 << 20}
	client, server := pair(t, cfg)
	filler, _ := announced(t, client, server)
	quitter, _ := announced(t, client, server)
	behind, _ := announced(t, client, server)
	w := client.w
	payload := make([]byte, 64<<10)

	release := pinFlusher(t, w)
	if _, err := w.appendData(filler, payload, 0, true, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	queued := func(n int) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for {
			w.mu.Lock()
			got := len(w.admitQ)
			w.mu.Unlock()
			if got == n {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d writers queued in admission, want %d", got, n)
			}
			time.Sleep(time.Millisecond)
		}
	}
	quit := make(chan error, 1)
	go func() {
		_, err := w.appendData(quitter, payload, 0, false, nil, nil, false)
		quit <- err
	}()
	queued(1)
	done := make(chan error, 1)
	go func() {
		_, err := w.appendData(behind, payload, 0, false, nil, nil, false)
		done <- err
	}()
	queued(2)

	// The head of the queue will be admitted and find its stream abandoned.
	quitter.sendAborted.Store(true)
	release()

	if err := <-quit; err != ErrStreamClosed {
		t.Fatalf("abandoned writer returned %v, want ErrStreamClosed", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer behind an abandoned one was never woken")
	}
}

// Writers parked in admission must all finish while others around them are
// being cancelled and timing out of the queue.
func TestAdmissionSurvivesChurn(t *testing.T) {
	// One frame per batch, so nearly every write queues.
	cfg := &Config{MaxBatchBytes: 64<<10 + frame.HeaderSize, InitialWindow: 4 << 20}
	client, server := pair(t, cfg)
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

	const writers = 48
	var wg sync.WaitGroup
	for i := range writers {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			buf := make([]byte, 64<<10)
			for j := range 40 {
				switch {
				case i%3 == 0 && j == 10:
					// Cancelled, quite possibly while parked.
					go st.(*NativeStream).CancelWrite(CodeCanceled)
				case i%3 == 1:
					// A deadline short enough to expire in the queue.
					st.SetWriteDeadline(time.Now().Add(200 * time.Microsecond))
				}
				if _, err := st.Write(buf); err != nil && i%3 == 0 {
					return
				}
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("writers still parked in admission: a wake was lost")
	}
	if err := client.closedErr(); err != nil {
		t.Fatalf("session died: %v", err)
	}
}

// A small frame goes to its stream's shard while a flusher is running, and to
// the main lane once the stream has main-lane data in the same batch: shards
// are written first, so a shard frame staged after a main-lane one would
// overtake it on the wire.
func TestSmallFrameShardRespectsStreamOrder(t *testing.T) {
	client, server := pair(t, nil)
	st, _ := announced(t, client, server)
	w := client.w

	release := pinFlusher(t, w)
	defer release()
	shardBytes := func() int { return int(w.shardBytes.Load()) }
	stageBytes := func() int {
		w.mu.Lock()
		defer w.mu.Unlock()
		return len(w.stage)
	}

	small := []byte("small")
	if _, err := w.appendData(st, small, 0, true, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if got, want := shardBytes(), frame.HeaderSize+len(small); got != want {
		t.Fatalf("shards hold %d bytes after one small frame, want %d", got, want)
	}
	if got := stageBytes(); got != 0 {
		t.Fatalf("main lane holds %d bytes after a small frame, want none", got)
	}

	// A copied frame too large for a shard takes the main lane...
	big := make([]byte, 2*zeroCopyThreshold)
	if _, err := w.appendData(st, big, 0, true, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	// ...and the stream's next small frame must follow it there.
	before := shardBytes()
	if _, err := w.appendData(st, small, 0, true, nil, nil, false); err != nil {
		t.Fatal(err)
	}
	if got := shardBytes(); got != before {
		t.Fatalf("small frame went to a shard (%d -> %d bytes) behind main-lane data of its own stream", before, got)
	}
	if got, want := stageBytes(), 2*frame.HeaderSize+len(big)+len(small); got != want {
		t.Fatalf("main lane holds %d bytes, want %d", got, want)
	}
}

// Streams mixing small, copied and referenced writes must each arrive as the
// exact bytes written, while enough other traffic runs to keep a flusher
// busy and so keep the shard path in use. Every lane a frame can take is
// exercised against every other within single streams.
func TestMixedLaneWritesArriveInOrder(t *testing.T) {
	client, server := pair(t, &Config{InitialWindow: 1 << 20})
	c := ctx(t)

	const streams = 12
	sizes := []int{1, 300, 5000, 70, 100_000, 3, 4095, 4096, 9, 20_000, 1}
	want := make([][]byte, streams)
	for i := range want {
		for round := range 40 {
			for _, n := range sizes {
				for k := range n {
					want[i] = append(want[i], byte(i*31+round*7+k))
				}
			}
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2*streams)
	for i := range streams {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			defer st.CloseWrite()
			p := want[i]
			for round := 0; len(p) > 0; round++ {
				for j, n := range sizes {
					// Every third write of some streams carries a deadline,
					// which forces the copy path at any size.
					if i%2 == 0 && (round+j)%3 == 0 {
						st.SetWriteDeadline(time.Now().Add(time.Minute))
					} else {
						st.SetWriteDeadline(time.Time{})
					}
					if _, err := st.Write(p[:n]); err != nil {
						errs <- err
						return
					}
					p = p[n:]
				}
			}
		})
	}
	for range streams {
		ss, err := server.AcceptStream(c)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			got, err := io.ReadAll(ss)
			if err != nil {
				errs <- err
				return
			}
			// Streams are accepted in arrival order, not open order.
			i := int(ss.StreamID()-1) / 2
			if !bytes.Equal(got, want[i]) {
				at := 0
				for at < len(got) && at < len(want[i]) && got[at] == want[i][at] {
					at++
				}
				errs <- fmt.Errorf("stream %d: read %d bytes, want %d; first difference at %d", i, len(got), len(want[i]), at)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// A small frame staged in a shard has no flusher of its own. Request and
// response over many streams leaves each side repeatedly idle with one frame
// just staged, which is where a frame would be stranded if the writer and the
// exiting flusher could each miss the other.
func TestShardFramesAreNeverStranded(t *testing.T) {
	client, server := pair(t, nil)
	c := ctx(t)
	go func() {
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go io.Copy(st, st)
		}
	}()

	var wg sync.WaitGroup
	for range 32 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		wg.Go(func() {
			defer st.Close()
			msg := make([]byte, 64)
			for i := range 1500 {
				msg[0] = byte(i)
				if _, err := st.Write(msg); err != nil {
					t.Error(err)
					return
				}
				if _, err := io.ReadFull(st, msg); err != nil {
					t.Error(err)
					return
				}
				if msg[0] != byte(i) {
					t.Errorf("echo %d came back as %d", byte(i), msg[0])
					return
				}
			}
		})
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("round trips stalled: a staged frame was never flushed")
	}
}

// Buffers handed over together must arrive as the bytes they held, in order,
// whatever mix of sizes they are: buffers of a frame or less may share a
// batch, a larger one is split, and empty ones are skipped.
func TestWriteBuffersDeliversInOrder(t *testing.T) {
	client, server := pair(t, &Config{InitialWindow: 4 << 20})
	st, ss := announced(t, client, server)
	maxf := int(client.peerMaxFrame.Load())

	sizes := []int{maxf, maxf, 10, 0, maxf, 5000, 3 * maxf, 1, maxf - 1, maxf, maxf, 0, 300}
	var bufs [][]byte
	var want []byte
	for i, n := range sizes {
		b := make([]byte, n)
		for k := range b {
			b[k] = byte(i*13 + k)
		}
		bufs = append(bufs, b)
		want = append(want, b...)
	}
	n, err := st.writeBuffers(bufs)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(want)) {
		t.Fatalf("wrote %d bytes, want %d", n, len(want))
	}
	got := make([]byte, len(want))
	if _, err := io.ReadFull(ss, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("buffers written together did not arrive as the bytes they held")
	}
}

// Whole buffers that follow a frame share its batch, as far as the batch
// allows, and the write reports exactly what was sent.
func TestWriteBuffersSharesOneBatch(t *testing.T) {
	client, server := pair(t, &Config{InitialWindow: 4 << 20})
	st, _ := announced(t, client, server)
	w := client.w
	maxf := int(client.peerMaxFrame.Load())

	release := pinFlusher(t, w)
	w.mu.Lock()
	w.burst = 3
	w.takeHint.Store(int64(3 * maxf))
	w.mu.Unlock()

	bufs := [][]byte{make([]byte, maxf), make([]byte, 9000), make([]byte, maxf), make([]byte, maxf)}
	done := make(chan error, 1)
	go func() {
		_, err := st.writeBuffers(bufs)
		done <- err
	}()
	// Three frames fit the batch: the first three buffers, one frame each.
	staged(t, w, st, maxf+9000+maxf+3*frame.HeaderSize)
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// Closing a stream whose peer is still sending stages the FIN and the
// STOP_SENDING for one flush, not two.
func TestCloseStagesFinAndStopTogether(t *testing.T) {
	client, server := pair(t, nil)
	st, _ := announced(t, client, server)
	w := client.w

	release := pinFlusher(t, w)
	w.mu.Lock()
	before := w.frames + w.shards[st.shard].frames
	w.mu.Unlock()
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	after := w.frames + w.shards[st.shard].frames
	w.mu.Unlock()
	release()
	if after-before != 2 {
		t.Fatalf("Close staged %d frames with the flusher held, want FIN and STOP_SENDING", after-before)
	}
}

// A Close that sends no STOP_SENDING, because the peer has already finished,
// must still get its FIN to the peer: nothing else is coming to flush it.
func TestCloseFlushesFinWhenNothingFollows(t *testing.T) {
	client, server := pair(t, nil)
	st, ss := announced(t, client, server)

	if err := ss.(*NativeStream).CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("read %v, want EOF from the peer's FIN", err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	ss.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := ss.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("peer read %v after Close, want EOF: the FIN was never flushed", err)
	}
}
