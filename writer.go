package mux

import (
	"net"
	"os"
	"sync"
	"time"

	"github.com/giraffesyo/mux/internal/frame"
)

// The send path is group commit: writers append frames to a shared pending
// batch under one mutex, and whichever writer finds no flush in flight
// becomes the flusher — it swaps the batch out, releases the mutex, and
// issues a single writev for the whole batch. Under load one syscall carries
// many frames across many streams; idle, the flush happens inline in the
// calling goroutine, so latency equals a direct write.
//
// Completion semantics are split (DESIGN.md "Send path"):
//
//   - Small payloads are copied into staging at admission, so the io.Writer
//     contract is already satisfied and the writer returns as soon as its
//     bytes are queued. Errors latch and surface on a later call.
//   - Large payloads are referenced directly in the iovec (zero copy), so
//     that writer must park until the flush containing its bytes resolves —
//     the kernel is reading the caller's memory until then.
//
// A stream with an active write deadline always takes the copy path: a
// deadline cannot be honored once the caller's buffer is pinned in an iovec.
const (
	// zeroCopyThreshold is the payload size at or above which a write is
	// referenced rather than copied. Below it the memcpy is cheaper than
	// the goroutine park it would otherwise cost.
	zeroCopyThreshold = 4 << 10

	// tlsRecordSize is crypto/tls's maximum plaintext record. Coalesced
	// writes are chunked to it so no copy exceeds what one record carries.
	tlsRecordSize = 16 << 10

	// smallWriteReserve is batch space kept clear of bulk writes so small
	// messages always have somewhere to land. It bounds how long a small
	// write can be held out of the batch currently being assembled.
	smallWriteReserve = 64 << 10
)

// chunk is one entry in a pending batch: either a reference to caller memory
// (ref non-nil) or a range of the staging buffer, which may be reallocated
// as it grows and so cannot be sliced until flush time.
type chunk struct {
	ref []byte
	off int32
	n   int32
}

type writer struct {
	s   *NativeSession
	tcp *net.TCPConn // non-nil when net.Buffers becomes a real writev

	mu sync.Mutex

	// Pending batch. Control frames are staged separately and placed first
	// in the iovec so a window update is never trapped behind bulk data.
	ctl     []byte
	stage   []byte
	chunks  []chunk
	pending int

	// seq is the sequence number the pending batch will carry; doneSeq is
	// the highest completed one. A parked zero-copy writer waits for
	// doneSeq to reach the seq its bytes went into.
	seq     uint64
	doneSeq uint64
	err     error // sticky: any carrier error is session-fatal

	// flushed is closed when a batch completes and someone is waiting, then
	// replaced. It is selectable, unlike a sync.Cond, so write deadlines
	// stay enforceable while waiting. waiters counts parked goroutines so
	// the uncontended path — one writer, inline flush, nobody parked —
	// neither closes nor reallocates it.
	flushed chan struct{}
	waiters int

	flushing bool

	// pingStamp marks a batch as carrying the BDP probe, so the flusher can
	// timestamp it as it reaches the carrier rather than at enqueue.
	pingStamp  bool
	fpingStamp bool

	// Flusher-owned scratch, swapped with the pending buffers so neither
	// side allocates in steady state.
	fctl     []byte
	fstage   []byte
	fchunks  []chunk
	iov      net.Buffers
	coalesce []byte
}

func newWriter(s *NativeSession) *writer {
	w := &writer{
		s:       s,
		flushed: make(chan struct{}),
		ctl:     make([]byte, 0, 4<<10),
		stage:   make([]byte, 0, 64<<10),
		chunks:  make([]chunk, 0, 64),
		fctl:    make([]byte, 0, 4<<10),
		fstage:  make([]byte, 0, 64<<10),
		fchunks: make([]chunk, 0, 64),
	}
	// net.Buffers only becomes writev on an exact *net.TCPConn: the
	// enabling interface inside net is unexported, so no wrapper can
	// implement it. Anything else is coalesced into one contiguous write.
	if tcp, ok := s.conn.(*net.TCPConn); ok {
		w.tcp = tcp
	}
	return w
}

// appendControl queues a control frame and flushes inline if no flush is in
// flight. Callers must be application goroutines — the session reader uses
// appendControlAsync, since a reader that blocks on the send path is one
// half of the classic two-sided gridlock.
func (w *writer) appendControl(h frame.Header, payload []byte) {
	if w.stageControl(h, payload) {
		w.flushLoop()
	}
}

// appendControlAsync queues a control frame without ever blocking the
// caller: if a flush is needed, a goroutine performs it. Used by the reader
// goroutine for PING ACKs, refusal resets, and protocol GOAWAYs.
func (w *writer) appendControlAsync(h frame.Header, payload []byte) {
	if w.stageControl(h, payload) {
		go w.flushLoop()
	}
}

// appendProbe queues the BDP probe PING and marks its batch for flush-time
// stamping, so the measured RTT excludes our own queue delay.
//
// The flag is set in the same critical section as the append. Setting it
// separately lets a flush in between carry the flag away on a batch that
// does not contain the probe: the probe then reaches the wire unstamped and
// its sample is discarded, which stalls autotuning at the initial window.
func (w *writer) appendProbe(h frame.Header, payload []byte) {
	if w.stageControlStamped(h, payload) {
		go w.flushLoop()
	}
}

// stageControl appends the frame and reports whether the caller took
// ownership of flushing. Control frames never wait on the batch cap: they
// are small, bounded, and some originate on the reader goroutine.
func (w *writer) stageControl(h frame.Header, payload []byte) bool {
	return w.stage2(h, payload, false)
}

// stageControlStamped is stageControl plus marking the batch as carrying the
// BDP probe, both under one lock.
func (w *writer) stageControlStamped(h frame.Header, payload []byte) bool {
	return w.stage2(h, payload, true)
}

func (w *writer) stage2(h frame.Header, payload []byte, stamp bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return false
	}
	if stamp {
		w.pingStamp = true
	}
	h.Length = uint32(len(payload))
	var hdr [frame.HeaderSize]byte
	h.Encode(hdr[:])
	w.ctl = append(w.ctl, hdr[:]...)
	w.ctl = append(w.ctl, payload...)
	w.pending += frame.HeaderSize + len(payload)
	return w.claimFlushLocked()
}

// appendData queues one DATA frame for st. Small payloads return once
// staged; large ones park until their flush resolves.
//
// SYN is attached if this is the stream's first frame on the wire, decided
// under w.mu together with the append, so concurrent senders can neither
// duplicate the SYN nor let a later frame overtake it.
func (w *writer) appendData(st *NativeStream, payload []byte, flags frame.Flags, copyOnly bool, deadline <-chan struct{}) error {
	total := frame.HeaderSize + len(payload)

	w.mu.Lock()
	// Admission backpressure: bound the batch so flush duration, staging
	// occupancy, and every co-batched writer's latency floor stay bounded,
	// and bound each stream's share so a bulk transfer cannot monopolize
	// consecutive batches. Waiting here is pre-admission, so write
	// deadlines legally apply.
	for w.err == nil && w.admissionBlockedLocked(st, total) {
		if flush := w.claimFlushLocked(); flush {
			w.mu.Unlock()
			w.flushLoop()
			w.mu.Lock()
			continue
		}
		// Registered before unlocking, so the flusher cannot decide
		// "nobody is waiting" and skip the wakeup we are about to await.
		w.s.stats.admissionWaits.Add(1)
		w.waiters++
		ch := w.flushed
		w.mu.Unlock()
		var err error
		select {
		case <-ch:
		case <-deadline:
			err = os.ErrDeadlineExceeded
		case <-w.s.done:
			err = w.s.closedErr()
		}
		w.mu.Lock()
		w.waiters--
		if err != nil {
			w.mu.Unlock()
			return err
		}
	}
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return err
	}

	if st.batchSeq != w.seq+1 {
		st.batchSeq = w.seq + 1 // batchSeq 0 means "no batch", so offset by one
		st.batchBytes = 0
	}
	st.batchBytes += total

	if !st.synSent {
		flags |= frame.FlagSYN
		st.synSent = true
	}
	var hdr [frame.HeaderSize]byte
	frame.Header{
		Type:     frame.TypeData,
		Flags:    flags,
		StreamID: st.id,
		Length:   uint32(len(payload)),
	}.Encode(hdr[:])

	// The header always goes to staging; the payload is copied or
	// referenced depending on size and whether a deadline is active.
	zeroCopy := !copyOnly && len(payload) >= zeroCopyThreshold
	off := int32(len(w.stage))
	w.stage = append(w.stage, hdr[:]...)
	if !zeroCopy {
		w.stage = append(w.stage, payload...)
	}
	w.chunks = append(w.chunks, chunk{off: off, n: int32(len(w.stage)) - off})
	if zeroCopy {
		w.chunks = append(w.chunks, chunk{ref: payload})
	}
	w.pending += total

	mySeq := w.seq
	flush := w.claimFlushLocked()
	w.mu.Unlock()

	if flush {
		w.flushLoop()
	}
	if !zeroCopy {
		// Bytes are session-owned: the caller may reuse its buffer, and
		// any error surfaces on a later call.
		return nil
	}

	// Park until the flush carrying this payload resolves. There is no
	// deadline case here: a stream with an active write deadline took the
	// copy path above.
	w.mu.Lock()
	for w.doneSeq < mySeq && w.err == nil {
		w.waiters++
		ch := w.flushed
		w.mu.Unlock()
		var dead bool
		select {
		case <-ch:
		case <-w.s.done:
			dead = true
		}
		w.mu.Lock()
		w.waiters--
		if dead && w.err == nil {
			w.err = w.s.closedErr()
		}
	}
	err := w.err
	w.mu.Unlock()
	return err
}

// admissionBlockedLocked reports whether this write must wait for the
// pending batch to flush: either the batch is full, or this stream has
// already used its share of it. An empty batch always admits, so a single
// oversized write can never deadlock against its own cap.
func (w *writer) admissionBlockedLocked(st *NativeStream, total int) bool {
	if w.pending == 0 {
		return false
	}
	// Bulk writes must leave headroom that only small writes may use, so a
	// latency-sensitive message can always join the batch being assembled
	// instead of queueing behind a batch's worth of bulk data. Without the
	// reservation, small writers lose the admission race to a handful of
	// saturating bulk writers until the runtime's mutex starvation mode
	// forces a handoff — which showed up as a millisecond of added tail
	// latency, an order of magnitude worse than the batch itself.
	cap := w.s.cfg.MaxBatchBytes
	if total > zeroCopyThreshold {
		cap -= smallWriteReserve
	}
	if w.pending+total > cap {
		return true
	}
	return st.batchSeq == w.seq+1 && st.batchBytes+total > w.s.cfg.PerStreamBatchBytes
}

// claimFlushLocked makes the caller the flusher when none is running and
// there is work. Any enqueue may claim it, control included: a download-only
// session has no data writers, so window updates would otherwise never reach
// the wire and the transfer would deadlock once the initial window drained.
func (w *writer) claimFlushLocked() bool {
	if w.flushing || w.pending == 0 || w.err != nil {
		return false
	}
	w.flushing = true
	return true
}

// flushLoop drains the pending batch, re-checking after each writev so the
// carrier stays continuously fed with no goroutine wakeup between
// back-to-back flushes. The caller has already claimed flusher duty.
//
// Flusher duty is unconditional: this returns only with the batch drained or
// the session failed, never leaving a non-empty batch with no flusher.
func (w *writer) flushLoop() {
	// Like the reader, this goroutine must not take the host application
	// down with it: a panic here becomes a session failure, not a crash.
	defer w.s.recoverPanic("session flusher")
	for {
		w.mu.Lock()
		if w.pending == 0 || w.err != nil {
			w.flushing = false
			w.wakeLocked()
			w.mu.Unlock()
			return
		}
		seq := w.seq
		w.seq++

		// Swap the pending buffers with the flusher's scratch: neither
		// side allocates once both have grown to steady-state size.
		w.fctl, w.ctl = w.ctl, w.fctl[:0]
		w.fstage, w.stage = w.stage, w.fstage[:0]
		w.fchunks, w.chunks = w.chunks, w.fchunks[:0]
		w.fpingStamp, w.pingStamp = w.pingStamp, false
		w.pending = 0
		w.mu.Unlock()

		err := w.writeOut()

		w.mu.Lock()
		w.doneSeq = seq
		if err != nil && w.err == nil {
			w.err = err
		}
		w.wakeLocked()
		// The sticky error may already be set by fail() while this flush
		// succeeded — a session dying for an unrelated reason. Either way
		// flushing stops, but only our own carrier error kills the session.
		done := w.err != nil
		if done {
			w.flushing = false
		}
		w.mu.Unlock()

		if err != nil {
			// Outside the lock: fatal closes the carrier and destroys
			// streams, and calls back into fail().
			w.s.fatal(&SessionError{Code: CodeInternal, Reason: "carrier write: " + err.Error()})
			return
		}
		if done {
			return
		}
	}
}

// wakeLocked releases everyone parked on the current batch generation. With
// no waiters there is nobody to wake and the channel is left untouched, which
// keeps the common single-writer path allocation-free.
func (w *writer) wakeLocked() {
	if w.waiters == 0 {
		return
	}
	close(w.flushed)
	w.flushed = make(chan struct{})
}

// writeOut issues the swapped-out batch. Control bytes lead the iovec, so a
// window update is never delayed behind queued bulk data.
func (w *writer) writeOut() error {
	w.iov = w.iov[:0]
	if len(w.fctl) > 0 {
		w.iov = append(w.iov, w.fctl)
	}
	for _, c := range w.fchunks {
		if c.ref != nil {
			w.iov = append(w.iov, c.ref)
			continue
		}
		w.iov = append(w.iov, w.fstage[c.off:c.off+c.n])
	}
	if len(w.iov) == 0 {
		return nil
	}
	// Frames per flush is the ratio that says whether batching is working;
	// counted here, where a batch is fully assembled.
	var frames, bytes uint64
	for _, b := range w.iov {
		bytes += uint64(len(b))
	}
	frames = uint64(len(w.fchunks))
	if len(w.fctl) > 0 {
		frames++
	}
	w.s.stats.recordFlush(frames, bytes)

	if w.s.cfg.WriteTimeout > 0 {
		_ = w.s.conn.SetWriteDeadline(time.Now().Add(w.s.cfg.WriteTimeout))
		defer func() { _ = w.s.conn.SetWriteDeadline(time.Time{}) }()
	}

	if w.fpingStamp {
		// Stamped here, immediately before the syscall: any time this
		// batch spent queued is ours, not the network's.
		w.s.bdp.markSent(time.Now(), w.s.recvTotal.Load(), w.s.stalls.Load())
		w.fpingStamp = false
	}

	if w.tcp != nil {
		// net.Buffers.WriteTo issues writev, looping past the 1024-iovec
		// kernel limit, so a flush is not assumed to be one syscall.
		//
		// It writes through a *copy* of the slice header deliberately:
		// WriteTo consumes the Buffers it is given, advancing it as it
		// writes. Passing w.iov directly leaves it empty but pointing at
		// the end of its backing array, so the next flush's appends
		// reallocate — an allocation per flush, which profiling showed
		// was the single largest remaining allocation source.
		iov := w.iov
		_, err := iov.WriteTo(w.tcp)
		return err
	}

	// Any other carrier (TLS, wrappers, Windows) gets one contiguous write
	// instead of one write per buffer. Over TLS this is still split into
	// 16KB records by crypto/tls, each its own carrier write; chunking the
	// copy at that size keeps no memcpy larger than a record can carry.
	w.coalesce = w.coalesce[:0]
	for _, b := range w.iov {
		w.coalesce = append(w.coalesce, b...)
	}
	buf := w.coalesce
	for len(buf) > 0 {
		n := min(len(buf), tlsRecordSize)
		if _, err := w.s.conn.Write(buf[:n]); err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// fail releases every parked writer when the session dies for a reason the
// writer did not observe itself (carrier read error, local Close).
func (w *writer) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.wakeLocked()
	w.mu.Unlock()
}
