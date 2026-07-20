package mux

import (
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/giraffesyo/mux/internal/frame"
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

	rmu sync.Mutex // serializes Read callers
	wmu sync.Mutex // serializes Write/CloseWrite callers

	// mu guards all state below. It is a leaf lock: no session lock and no
	// carrier write ever happens while holding it.
	mu sync.Mutex

	// Receive side.
	rq         [][]byte // queued payload chunks, oldest first
	recvd      uint64   // cumulative bytes arrived
	consumed   uint64   // cumulative bytes delivered to the application
	recvLimit  uint64   // absolute limit we have advertised
	window     uint64   // our per-stream window size
	finRecvd   bool
	readErr    error // terminal read error (reset/cancel/session death)
	readClosed bool  // local CancelRead/Close: drop incoming silently

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
}

// hasWriteDeadline reports whether this stream's writes must be copied
// rather than referenced, so a deadline stays enforceable after admission.
func (s *NativeStream) hasWriteDeadline() bool { return s.wdSet.Load() }

func newStream(sess *NativeSession, id uint32, local bool) *NativeStream {
	return &NativeStream{
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
		s.mu.Lock()
		if len(s.rq) > 0 {
			n := 0
			for n < len(p) && len(s.rq) > 0 {
				c := copy(p[n:], s.rq[0])
				n += c
				if c == len(s.rq[0]) {
					s.rq[0] = nil
					s.rq = s.rq[1:]
				} else {
					s.rq[0] = s.rq[0][c:]
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
func (s *NativeStream) maybeGrowRecvLimitLocked() uint64 {
	if s.finRecvd || s.readErr != nil || s.readClosed {
		return 0
	}
	if s.recvLimit-s.consumed < s.window/2 {
		s.recvLimit = s.consumed + s.window
		return s.recvLimit
	}
	return 0
}

// Write implements io.Writer. It blocks for flow-control credit, chunking to
// the peer's maximum frame size.
func (s *NativeStream) Write(p []byte) (int, error) {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	total := 0
	for len(p) > 0 {
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

		// A stream with an active write deadline takes the copy path: the
		// deadline could not be honored once the caller's buffer is
		// pinned in an iovec that the kernel is reading.
		if err := s.sess.writeData(s, p[:n], 0, s.hasWriteDeadline(), s.wd.wait()); err != nil {
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

	err := s.sess.writeData(s, nil, frame.FlagFIN, true, s.wd.wait())
	s.sess.maybeRemove(s)
	return err
}

// CancelWrite aborts the send side with an application error code (RST). If
// the stream's SYN never reached the wire, the cancel is purely local — the
// peer must never see an RST for a stream it has not heard of.
func (s *NativeStream) CancelWrite(code uint64) {
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
	s.rq = nil
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
