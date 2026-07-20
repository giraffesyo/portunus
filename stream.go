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
	batchSeq   uint64
	batchBytes int

	rmu sync.Mutex // serializes Read callers
	wmu sync.Mutex // serializes Write/CloseWrite callers

	// mu guards all state below. It is a leaf lock: no session lock and no
	// carrier write ever happens while holding it.
	mu sync.Mutex

	// Receive side.
	rq         []segment // queued segments, oldest first
	drain      []segment // scratch for draining rq without allocating
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
		rd:        makeDeadline(),
		wd:        makeDeadline(),
	}
	st.lastActive.Store(time.Now().UnixNano())
	return st
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
		if isClosedChan(s.rd.wait()) {
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
		case <-s.rd.wait():
			return 0, os.ErrDeadlineExceeded
		}
	}
}

// maybeGrowRecvLimitLocked returns a new absolute limit to advertise, or 0.
// The trigger (less than half a window of headroom) guarantees the new limit
// strictly exceeds the previous one, so the peer never sees a no-op update.
//
// It is also where autotuning lands: the window is resized to the session's
// current BDP estimate, clamped by the session receive budget, at the moment
// we would have sent an update anyway. Growth costs no extra frame.
func (s *NativeStream) maybeGrowRecvLimitLocked() uint64 {
	if s.finRecvd || s.readErr != nil || s.readClosed {
		return 0
	}
	if s.recvLimit-s.consumed >= s.window/2 {
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
		case <-s.rd.wait():
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
	total := 0
	for len(p) > 0 {
		// An expired deadline fails the write even when the send path
		// could satisfy it immediately. net.Conn requires a deadline
		// already in the past to be honored — checking only where the
		// write would block lets a call slip through whenever the batch
		// happens to be empty, which is exactly when it is least expected.
		if isClosedChan(s.wd.wait()) {
			return total, os.ErrDeadlineExceeded
		}
		s.mu.Lock()
		for {
			if err := s.writeErr; err != nil {
				s.mu.Unlock()
				return total, err
			}
			if s.finSent {
				s.mu.Unlock()
				return total, ErrStreamClosed
			}
			if s.sendLimit > s.sent {
				break
			}
			s.mu.Unlock()
			select {
			case <-s.writable:
			case <-s.wd.wait():
				return total, os.ErrDeadlineExceeded
			}
			s.mu.Lock()
		}
		n := len(p)
		if credit := s.sendLimit - s.sent; uint64(n) > credit {
			n = int(credit)
		}
		if maxf := int(s.sess.peerMaxFrame.Load()); n > maxf {
			n = maxf
		}
		s.sent += uint64(n)
		s.mu.Unlock()
		s.lastActive.Store(time.Now().UnixNano())

		// A stream with an active write deadline takes the copy path: the
		// deadline could not be honored once the caller's buffer is
		// pinned in an iovec that the kernel is reading.
		if err := s.sess.writeData(s, p[:n], 0, s.hasWriteDeadline(), s.wd.wait()); err != nil {
			// Give the credit back. These bytes were reserved above but
			// never reached the wire — admission can fail on a write
			// deadline while the stream stays perfectly usable, and
			// keeping the reservation would shrink the window by n on
			// every such expiry until the stream could never send again.
			s.mu.Lock()
			s.sent -= uint64(n)
			s.mu.Unlock()
			notify(s.writable)
			return total, err
		}
		total += n
		p = p[n:]
	}
	return total, nil
}

// CloseWrite half-closes: FIN rides an empty DATA frame (with SYN if this
// stream never sent). The read side stays open.
func (s *NativeStream) CloseWrite() error {
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
	err := s.sess.writeData(s, nil, frame.FlagFIN, true, nil)
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
func (s *NativeStream) Close() error {
	err := s.CloseWrite()
	s.CancelRead(CodeCanceled)
	s.mu.Lock()
	s.appClosed = true
	s.mu.Unlock()
	s.sess.maybeRemove(s)
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
	s.setWriteDeadline(t)
	return nil
}

func (s *NativeStream) SetReadDeadline(t time.Time) error {
	s.rd.set(t)
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
