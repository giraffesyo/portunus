package portunus

import (
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/giraffesyo/portunus/internal/frame"
	"github.com/giraffesyo/portunus/internal/pool"
)

// NativeStream is one logical stream over a session. It implements Stream
// (and therefore net.Conn). Read and Write may be called concurrently with
// each other and with any control method; concurrent Reads (or Writes) are
// serialized.
type NativeStream struct {
	sess  *NativeSession
	id    uint32
	local bool // opened by this side

	// synSent is guarded by sess.writeMu: SYN must ride the first frame
	// this stream ever puts on the wire, atomically with that frame.
	synSent bool

	// synRecvd is guarded by sess.mu and set when the peer's SYN arrives,
	// so a duplicate SYN for a live stream is caught as a protocol error.
	synRecvd bool

	// batchSeq/batchBytes track this stream's share of the batch currently
	// accepting frames, enforcing per-stream fairness. Guarded by the
	// writer mutex. batchSeq is the batch sequence plus one, so the zero
	// value means "not in any batch".
	//
	// batchSeq is written under that mutex but read without it by the
	// writer's small-frame fast path, which must stay off any batch this
	// stream already has frames in the main lanes of.
	batchSeq   atomic.Uint64
	batchBytes int

	// announced mirrors synSent for the fast path: set once the SYN frame
	// has been staged (or at creation for a stream the peer opened), after
	// which a small frame no longer needs the writer mutex to decide SYN.
	announced atomic.Bool

	// shard is the staging shard this stream's small frames go to. Fixed
	// for the stream's life, which is what keeps them in order.
	shard uint32

	// Admission-queue state, guarded by the writer mutex. admitCh carries
	// the wake and is created the first time this stream has to wait;
	// admitNeed is the size of the frame it is waiting to stage.
	admitCh     chan struct{}
	admitNeed   int
	admitQueued bool

	// priority routes this stream's DATA frames into the batch's priority
	// lane, which the flusher places ahead of all bulk data. It is atomic
	// so it can be set from any goroutine and read on the write path without
	// taking the writer mutex.
	priority atomic.Bool

	rmu sync.Mutex // serializes Read callers
	wmu sync.Mutex // serializes Write/CloseWrite callers

	// mu guards all state below. It is a leaf lock: no session lock and no
	// carrier write ever happens while holding it.
	mu sync.Mutex

	// Receive side.
	rq         []segment // queued segments, oldest first
	drain      []segment // scratch for draining rq without allocating
	vec        [][]byte  // scratch for handing drained segments to a stream
	recvd      uint64    // cumulative bytes arrived
	consumed   uint64    // cumulative bytes delivered to the application
	recvLimit  uint64    // absolute limit we have advertised
	window     uint64    // our per-stream window size
	finRecvd   bool
	readErr    error // terminal read error (reset/cancel/session death)
	readClosed bool  // local CancelRead/Close: drop incoming silently

	// stalledSinceGrow records that the peer ran out of credit on this
	// stream since its window last grew — direct evidence the window, not
	// the path, is the constraint.
	stalledSinceGrow bool

	// Send side.
	sent      uint64 // cumulative bytes sent
	sendLimit uint64 // absolute limit granted by the peer
	finSent   bool
	writeErr  error // terminal write error

	// Lifecycle.
	appClosed    bool
	slotReleased bool

	readable chan struct{} // cap 1: receive state changed
	writable chan struct{} // cap 1: send state changed

	rd deadline
	wd deadline

	// wdSet reports whether a write deadline is currently armed, read on
	// the send hot path to choose the copy path over zero copy.
	wdSet atomic.Bool
	rdSet atomic.Bool

	// lastActive is the UnixNano of the most recent payload in either
	// direction, for the optional idle sweep. Atomic so the sweep never
	// takes a stream lock.
	lastActive atomic.Int64

	// sendAborted marks the send side as locally abandoned. It is atomic so
	// the send path can consult it while holding the writer mutex, which is
	// what makes the decision to announce a stream and the decision to
	// abandon it resolve against each other in a single order.
	sendAborted atomic.Bool
}

// hasWriteDeadline reports whether this stream's writes must be copied
// rather than referenced, so a deadline stays enforceable after admission.
func (s *NativeStream) hasWriteDeadline() bool { return s.wdSet.Load() }

// readDeadline and writeDeadline return the channel closed when the deadline
// expires, or nil when none is armed. Most streams never set one, and the
// flag spares every Read and Write the deadline's mutex in that case.
func (s *NativeStream) readDeadline() chan struct{} {
	if !s.rdSet.Load() {
		return nil
	}
	return s.rd.wait()
}

func (s *NativeStream) writeDeadline() chan struct{} {
	if !s.wdSet.Load() {
		return nil
	}
	return s.wd.wait()
}

// segment is one received payload held in a pooled buffer. buf is the whole
// pooled allocation and is what must be returned to the pool; data is the
// unread remainder. Exactly one owner releases a segment, on every exit path
// — consumed, discarded, or dropped at teardown — or the pool leaks and the
// peer's flow-control credit is never returned.
type segment struct {
	buf  pool.Buf
	data []byte
}

func (sg segment) release() { pool.Put(sg.buf) }

// coalesceMax is the largest payload that is copied into the spare room of
// the previous segment's buffer rather than queued in a buffer of its own.
//
// A pooled buffer is a whole size class however little of it a frame uses,
// while flow control counts only the payload. Queued one buffer per frame, a
// stream of one-byte frames pins a kilobyte per byte of window — measured at
// 194MB of heap for 200KB of unread payload, inside the default window and
// available to any peer on every stream it may open. Packing small payloads
// together bounds what a window can pin to a small multiple of the window.
// Above this size the frame already fills a useful share of its class and
// the extra copy would cost more than the memory it saves.
const coalesceMax = 4 << 10

// appendLocked adds p to the tail segment's buffer if it is small and fits,
// reporting whether it did. Only segments still in rq are extended, and only
// under s.mu, so no reader holds the buffer while it grows.
func (s *NativeStream) appendLocked(p []byte) bool {
	if len(p) > coalesceMax || len(s.rq) == 0 {
		return false
	}
	tail := &s.rq[len(s.rq)-1]
	if !tail.buf.Append(p) {
		return false
	}
	// Re-slice from the grown buffer: the old data slice is capped at the
	// buffer's previous length and cannot simply be extended.
	b := tail.buf.Bytes()
	tail.data = b[len(b)-len(tail.data)-len(p):]
	return true
}

func releaseAll(segs []segment) {
	for _, sg := range segs {
		sg.release()
	}
}

func newStream(sess *NativeSession, id uint32, local bool) *NativeStream {
	sess.budget.reserve(uint64(sess.cfg.InitialWindow))
	st := &NativeStream{
		sess:      sess,
		id:        id,
		local:     local,
		window:    uint64(sess.cfg.InitialWindow),
		recvLimit: uint64(sess.cfg.InitialWindow),
		sendLimit: sess.peerInitialWindow.Load(),
		readable:  make(chan struct{}, 1),
		writable:  make(chan struct{}, 1),
		// IDs of one parity are consecutive even or odd numbers; drop the
		// parity bit so they spread over every shard.
		shard: (id >> 1) & sess.w.shardMask,
	}
	st.touch()
	return st
}

// touch records activity for the idle sweep. The sweep is off by default, and
// then nothing reads the timestamp, so the clock read is skipped: it sits on
// the per-frame path in both directions.
func (s *NativeStream) touch() {
	if s.sess.cfg.StreamIdleTimeout > 0 {
		s.lastActive.Store(time.Now().UnixNano())
	}
}

// StreamID returns the wire stream ID (uint64 so QUIC's 62-bit IDs fit the
// shared Stream interface).
func (s *NativeStream) StreamID() uint64 { return uint64(s.id) }

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// Read implements io.Reader. It drains queued segments, returns io.EOF after
// a clean FIN, and a *StreamError after a reset.
func (s *NativeStream) Read(p []byte) (int, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if isClosedChan(s.readDeadline()) {
			return 0, os.ErrDeadlineExceeded
		}
		s.mu.Lock()
		if len(s.rq) > 0 {
			n := 0
			for n < len(p) && len(s.rq) > 0 {
				c := copy(p[n:], s.rq[0].data)
				n += c
				if c == len(s.rq[0].data) {
					s.rq[0].release()
					s.rq[0] = segment{}
					s.rq = s.rq[1:]
				} else {
					s.rq[0].data = s.rq[0].data[c:]
				}
			}
			s.consumed += uint64(n)
			limit := s.maybeGrowRecvLimitLocked()
			done := s.finRecvd && len(s.rq) == 0
			s.mu.Unlock()
			if limit > 0 {
				s.sess.sendWindowUpdate(s, limit)
			}
			if done {
				s.sess.maybeRemove(s)
			}
			return n, nil
		}
		if err := s.readErr; err != nil {
			s.mu.Unlock()
			return 0, err
		}
		if s.finRecvd {
			s.mu.Unlock()
			return 0, io.EOF
		}
		if s.readClosed {
			s.mu.Unlock()
			return 0, ErrStreamClosed
		}
		s.mu.Unlock()

		select {
		case <-s.readable:
		case <-s.readDeadline():
			return 0, os.ErrDeadlineExceeded
		}
	}
}

// maybeGrowRecvLimitLocked returns a new absolute limit to advertise, or 0.
//
// The trigger is a quarter of a window of headroom left, not a half. A fixed
// window caps a stream at window/RTT, and how close it gets to that ceiling
// depends on the phase of credit return: the update granting fresh credit
// takes a round trip to reach the sender, so if it is not sent until half the
// window has drained, the sender runs the second half down and stalls waiting
// for it. Refreshing after only a quarter drains keeps the sender's credit a
// round trip ahead of exhaustion. Measured single-stream at a fixed 256KB
// window over a 55ms path, this lifts throughput from 67% of the window/RTT
// ceiling to 87%, past yamux's 74% at the same window; at the large windows
// autotuning reaches it fires seldom enough to cost nothing (a quarter of
// 8MB is 2MB between updates). The quarter still guarantees the new limit
// strictly exceeds the old, so the peer never sees a no-op update.
//
// It is also where autotuning lands: the window is resized to the session's
// current BDP estimate, clamped by the session receive budget, at the moment
// we would have sent an update anyway. Growth costs no extra frame.
func (s *NativeStream) maybeGrowRecvLimitLocked() uint64 {
	if s.finRecvd || s.readErr != nil || s.readClosed {
		return 0
	}
	if s.recvLimit-s.consumed >= s.window-s.window/4 {
		return 0
	}

	// Two independent reasons to enlarge the window, taken at the moment an
	// update is due so growth never costs an extra frame.
	target := s.sess.bdp.target()
	if s.stalledSinceGrow {
		// This stream's sender ran out of credit since the last growth: a
		// bigger window is warranted whether or not a BDP sample has
		// landed. Growth deliberately does not depend on a probe
		// completing — a probe's ACK travels back through the same
		// direction as the bulk data it is measuring, so under load it can
		// be delayed indefinitely, and tying growth to it leaves the
		// window frozen exactly when it most needs to grow.
		s.stalledSinceGrow = false
		if doubled := s.window * 2; doubled > target {
			target = doubled
		}
	}
	if target > s.window {
		s.window = s.sess.budget.grow(s.window, target)
	}
	s.recvLimit = s.consumed + s.window
	return s.recvLimit
}

// WriteTo implements io.WriterTo: it hands received segments straight to dst
// instead of copying them into a caller buffer first, so io.Copy(dst, stream)
// finds it automatically and the relay path avoids one copy per frame.
//
// No lock is held across the write to dst: a slow destination would otherwise
// stall the entire session's receive path behind this stream's mutex.
func (s *NativeStream) WriteTo(dst io.Writer) (int64, error) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	var total int64
	for {
		s.mu.Lock()
		s.drain = append(s.drain[:0], s.rq...)
		clear(s.rq)
		s.rq = s.rq[:0]
		n := 0
		for _, sg := range s.drain {
			n += len(sg.data)
		}
		s.consumed += uint64(n)
		limit := s.maybeGrowRecvLimitLocked()
		readErr, fin, closed := s.readErr, s.finRecvd, s.readClosed
		s.mu.Unlock()

		if limit > 0 {
			s.sess.sendWindowUpdate(s, limit)
		}

		if ns, ok := dst.(*NativeStream); ok && len(s.drain) > 1 {
			// Stream to stream, the relay case: hand over everything
			// drained at once so the destination can batch the frames
			// instead of flushing each. The segments stay ours until it
			// returns, which is after the batches carrying them resolve.
			s.vec = s.vec[:0]
			for _, sg := range s.drain {
				s.vec = append(s.vec, sg.data)
			}
			written, err := ns.writeBuffers(s.vec)
			total += written
			clear(s.vec)
			releaseAll(s.drain)
			if err != nil {
				clear(s.drain)
				return total, err
			}
		} else {
			for i, sg := range s.drain {
				written, err := dst.Write(sg.data)
				total += int64(written)
				sg.release()
				if err != nil {
					releaseAll(s.drain[i+1:])
					clear(s.drain)
					return total, err
				}
			}
		}
		if len(s.drain) > 0 {
			clear(s.drain)
			// Only worth probing once the peer has finished sending;
			// otherwise this takes the stream lock every drain cycle on
			// the hot relay path to learn nothing.
			if fin {
				s.sess.maybeRemove(s)
			}
			continue
		}

		switch {
		case readErr != nil:
			return total, readErr
		case fin:
			return total, nil // clean EOF: io.Copy reports success
		case closed:
			return total, ErrStreamClosed
		}

		select {
		case <-s.readable:
		case <-s.readDeadline():
			return total, os.ErrDeadlineExceeded
		}
	}
}

// ReadFrom implements io.ReaderFrom: io.Copy(stream, src) uses it to move
// src's bytes in frame-sized chunks through a pooled buffer, skipping
// io.Copy's own intermediate buffer.
func (s *NativeStream) ReadFrom(src io.Reader) (int64, error) {
	pb := pool.Get(int(s.sess.peerMaxFrame.Load()))
	defer pool.Put(pb)
	buf := pb.Bytes()

	var total int64
	for {
		n, rerr := src.Read(buf)
		if n > 0 {
			// Write does not return until these bytes are staged or on
			// the wire, so reusing buf on the next iteration is safe.
			if _, werr := s.Write(buf[:n]); werr != nil {
				return total, werr
			}
			total += int64(n)
		}
		if rerr != nil {
			if rerr == io.EOF {
				return total, nil
			}
			return total, rerr
		}
	}
}

// Write implements io.Writer. It blocks for flow-control credit, chunking to
// the peer's maximum frame size.
func (s *NativeStream) Write(p []byte) (int, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	n, _, err := s.writeLocked(p, nil)
	return int(n), err
}

// writeBuffers writes each buffer in order, as Write would, but lets
// consecutive buffers that are each one frame or less share a batch. It is
// what a relay uses: received segments arrive one frame at a time, and
// writing them one Write at a time costs the destination a flush per frame
// however many are waiting.
func (s *NativeStream) writeBuffers(bufs [][]byte) (int64, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	var total int64
	for i := 0; i < len(bufs); {
		n, used, err := s.writeLocked(bufs[i], bufs[i+1:])
		total += n
		if err != nil {
			return total, err
		}
		i += 1 + used
	}
	return total, nil
}

// writeLocked writes all of p, and along with p's final frame as many of the
// buffers in rest, whole and in order, as credit and the batch allow. It
// returns the bytes written and how many of rest they included. The caller
// holds wmu.
func (s *NativeStream) writeLocked(p []byte, rest [][]byte) (total int64, used int, err error) {
	for len(p) > 0 {
		// An expired deadline fails the write even when the send path
		// could satisfy it immediately. net.Conn requires a deadline
		// already in the past to be honored — checking only where the
		// write would block lets a call slip through whenever the batch
		// happens to be empty, which is exactly when it is least expected.
		wd := s.writeDeadline()
		if isClosedChan(wd) {
			return total, used, os.ErrDeadlineExceeded
		}
		s.mu.Lock()
		for {
			if err := s.writeErr; err != nil {
				s.mu.Unlock()
				return total, used, err
			}
			if s.finSent {
				s.mu.Unlock()
				return total, used, ErrStreamClosed
			}
			if s.sendLimit > s.sent {
				break
			}
			s.mu.Unlock()
			select {
			case <-s.writable:
			case <-wd:
				return total, used, os.ErrDeadlineExceeded
			}
			wd = s.writeDeadline()
			s.mu.Lock()
		}
		credit := s.sendLimit - s.sent
		n := len(p)
		if uint64(n) > credit {
			n = int(credit)
		}
		// Offer the writer as much as it could stage in one batch, so a
		// write spanning several frames can share a flush with itself. It
		// takes at least one frame and reports how much.
		maxf := int(s.sess.peerMaxFrame.Load())
		hint := max(maxf, int(s.sess.w.takeHint.Load()))
		if n > maxf {
			n = min(n, hint)
		}
		// Buffers that follow are offered too, once this one's last frame
		// is the one being sent: whole, and only as far as credit and the
		// batch reach.
		var more [][]byte
		reserve := n
		if n == len(p) && n <= maxf {
			k := 0
			for _, b := range rest[used:] {
				if len(b) == 0 || len(b) > maxf || reserve+len(b) > hint || uint64(reserve+len(b)) > credit {
					break
				}
				reserve += len(b)
				k++
			}
			more = rest[used : used+k]
		}
		s.sent += uint64(reserve)
		s.mu.Unlock()
		s.touch()

		// A stream with an active write deadline takes the copy path: the
		// deadline could not be honored once the caller's buffer is
		// pinned in an iovec that the kernel is reading.
		took, err := s.sess.w.appendData(s, p[:n], 0, s.hasWriteDeadline(), wd, more, false)
		if took < reserve {
			// Give back credit for what was not taken. On an error that is
			// all of it: the bytes were reserved above but never reached
			// the wire — admission can fail on a write deadline while the
			// stream stays perfectly usable, and keeping the reservation
			// would shrink the window by n on every such expiry until the
			// stream could never send again.
			s.mu.Lock()
			s.sent -= uint64(reserve - took)
			s.mu.Unlock()
		}
		if err != nil {
			notify(s.writable)
			return total, used, err
		}
		total += int64(took)
		if took < n {
			p = p[took:]
			continue
		}
		// All of p's remainder went, and whatever else was taken is whole
		// buffers from the front of more.
		p = p[n:]
		for took -= n; took > 0; used++ {
			took -= len(rest[used])
		}
	}
	return total, used, nil
}

// SetPriority marks this stream latency-sensitive. Its DATA frames are then
// written ahead of bulk data in every flush they share, so a small message on
// this stream is not transmitted behind a batch's worth of another stream's
// bulk. It affects only the local send direction and may be set at any time.
//
// EXPERIMENTAL: this is a prototype for evaluating small-frame prioritization
// and is not part of the stable API.
func (s *NativeStream) SetPriority(on bool) {
	s.priority.Store(on)
}

// CloseWrite half-closes: FIN rides an empty DATA frame (with SYN if this
// stream never sent). The read side stays open.
func (s *NativeStream) CloseWrite() error {
	return s.closeWrite(false)
}

// closeWrite is CloseWrite with the option of leaving the FIN staged rather
// than flushed. A caller that holds it must follow with something that
// flushes; see Close.
func (s *NativeStream) closeWrite(hold bool) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	s.mu.Lock()
	if err := s.writeErr; err != nil {
		s.mu.Unlock()
		return err
	}
	if s.finSent {
		s.mu.Unlock()
		return nil
	}
	s.finSent = true
	s.mu.Unlock()
	// A writer already parked waiting for credit must observe that the send
	// side is now finished. Without this it keeps waiting for credit that
	// will never be useful, and once the stream is reaped nothing else will
	// ever wake it: session teardown only reaches streams still in the map.
	notify(s.writable)

	// Deliberately not subject to the stream's write deadline. finSent is
	// already set, so abandoning the FIN would leave this side believing it
	// half-closed while the peer waits for an end that never comes — which
	// stranded streams in proportion to how often deadlines expired. A FIN
	// is a few bytes on the copy path; the session write timeout still
	// bounds it.
	_, err := s.sess.w.appendData(s, nil, frame.FlagFIN, true, nil, nil, hold)
	s.sess.maybeRemove(s)
	return err
}

// CancelWrite aborts the send side with an application error code (RST). If
// the stream's SYN never reached the wire, the cancel is purely local — the
// peer must never see an RST for a stream it has not heard of.
func (s *NativeStream) CancelWrite(code uint64) {
	// Set before anything else: a write already in flight consults this
	// under the writer mutex and will decline to announce a stream that has
	// just been abandoned. Without that, a cancel racing a stream's first
	// write can skip the reset — correctly, since nothing had been sent yet
	// — while the write goes on to announce the stream a moment later. The
	// peer is then holding a stream it will never hear about again: no data,
	// no FIN, no reset, just a reader blocked forever.
	s.sendAborted.Store(true)
	s.mu.Lock()
	if s.writeErr != nil || s.finSent {
		// Already terminal; QUIC-style RST-after-FIN is a non-goal in v1.
		s.mu.Unlock()
		return
	}
	s.writeErr = &StreamError{Code: code}
	final := s.sent
	s.mu.Unlock()
	notify(s.writable)

	s.sess.writeRST(s, code, final)
	s.sess.maybeRemove(s)
}

// CancelRead abandons the read side: queued and future data are discarded
// and the peer is asked to stop (STOP_SENDING).
func (s *NativeStream) CancelRead(code uint64) {
	s.mu.Lock()
	if s.readClosed || s.readErr != nil || (s.finRecvd && len(s.rq) == 0) {
		s.mu.Unlock()
		return
	}
	s.readClosed = true
	s.readErr = &StreamError{Code: code}
	releaseAll(s.rq)
	s.rq = s.rq[:0]
	s.consumed = s.recvd // no further credit will be granted
	fin := s.finRecvd
	s.mu.Unlock()
	notify(s.readable)

	if !fin {
		s.sess.writeStopSending(s, code)
	}
	s.sess.maybeRemove(s)
}

// Close closes both directions: CloseWrite plus, if the peer has not already
// finished sending, CancelRead — mirroring quic-go's stream close.
//
// The FIN is staged and left for the STOP_SENDING that usually follows to
// flush, so closing a stream whose peer is still sending is one carrier
// write rather than two. When no STOP_SENDING is due the kick at the end
// flushes it.
func (s *NativeStream) Close() error {
	err := s.closeWrite(true)
	s.CancelRead(CodeCanceled)
	s.mu.Lock()
	s.appClosed = true
	s.mu.Unlock()
	s.sess.maybeRemove(s)
	s.sess.w.kick()
	return err
}

// destroy terminates both sides with err (session death or GOAWAY scrub).
func (s *NativeStream) destroy(err error) {
	s.mu.Lock()
	if s.writeErr == nil {
		s.writeErr = err
	}
	if s.readErr == nil && !(s.finRecvd && len(s.rq) == 0) {
		s.readErr = err
	}
	s.mu.Unlock()
	notify(s.readable)
	notify(s.writable)
}

// sendDoneLocked and recvDoneLocked report side completion for stream
// removal. Callers hold s.mu.
func (s *NativeStream) sendDoneLocked() bool { return s.finSent || s.writeErr != nil }
func (s *NativeStream) recvDoneLocked() bool {
	return s.readErr != nil || s.readClosed || (s.finRecvd && len(s.rq) == 0)
}

// net.Conn plumbing.

func (s *NativeStream) LocalAddr() net.Addr  { return s.sess.conn.LocalAddr() }
func (s *NativeStream) RemoteAddr() net.Addr { return s.sess.conn.RemoteAddr() }

func (s *NativeStream) SetDeadline(t time.Time) error {
	s.rd.set(t)
	s.rdSet.Store(!t.IsZero())
	s.setWriteDeadline(t)
	return nil
}

func (s *NativeStream) SetReadDeadline(t time.Time) error {
	s.rd.set(t)
	s.rdSet.Store(!t.IsZero())
	return nil
}

func (s *NativeStream) SetWriteDeadline(t time.Time) error {
	s.setWriteDeadline(t)
	return nil
}

func (s *NativeStream) setWriteDeadline(t time.Time) {
	s.wd.set(t)
	s.wdSet.Store(!t.IsZero())
}
