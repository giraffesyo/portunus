// Package quic adapts a QUIC connection to the portunus Session and Stream
// interfaces, so transport-agnostic code can hold either a native portunus session
// over TCP or a QUIC connection without knowing which.
//
// It lives in its own module: the portunus core has no dependencies outside the
// standard library and never will, so depending on quic-go is opt-in.
//
// Use QUIC when you need what a userspace multiplexer over TCP cannot give
// you — most importantly, freedom from head-of-line blocking, where one lost
// packet stalls every stream sharing a TCP carrier. Use the native session
// when UDP is blocked, when you are multiplexing over an existing byte stream
// (an SSH channel, a Unix socket, a TLS connection you already have), or when
// you want the throughput the native path is tuned for.
package quic

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/giraffesyo/portunus"
	quicgo "github.com/quic-go/quic-go"
)

// Session wraps a QUIC connection. It satisfies portunus.Session.
type Session struct {
	conn *quicgo.Conn

	// live counts streams handed to the application and not yet closed,
	// so Shutdown can drain. QUIC exposes no stream count of its own.
	live atomic.Int64
}

// Stream wraps a QUIC stream. It satisfies portunus.Stream, and therefore
// net.Conn: QUIC streams have no addresses of their own, so LocalAddr and
// RemoteAddr report the underlying connection's.
type Stream struct {
	st   *quicgo.Stream
	conn *quicgo.Conn

	sess     *Session
	closeOne sync.Once
}

var (
	_ portunus.Session = (*Session)(nil)
	_ portunus.Stream  = (*Stream)(nil)
)

// Wrap adapts an established QUIC connection. The caller retains
// responsibility for the transport that produced it.
func Wrap(conn *quicgo.Conn) *Session { return &Session{conn: conn} }

// Dial establishes a QUIC connection to addr and adapts it.
func Dial(ctx context.Context, addr string, tlsConf *tls.Config, conf *quicgo.Config) (*Session, error) {
	conn, err := quicgo.DialAddr(ctx, addr, tlsConf, conf)
	if err != nil {
		return nil, err
	}
	return Wrap(conn), nil
}

// Listener accepts QUIC connections and adapts each one.
type Listener struct {
	ln *quicgo.Listener
}

// Listen announces on addr and returns a listener whose Accept yields
// adapted sessions.
func Listen(addr string, tlsConf *tls.Config, conf *quicgo.Config) (*Listener, error) {
	ln, err := quicgo.ListenAddr(addr, tlsConf, conf)
	if err != nil {
		return nil, err
	}
	return &Listener{ln: ln}, nil
}

// Accept returns the next connection, already adapted.
func (l *Listener) Accept(ctx context.Context) (*Session, error) {
	conn, err := l.ln.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return Wrap(conn), nil
}

func (l *Listener) Addr() net.Addr { return l.ln.Addr() }
func (l *Listener) Close() error   { return l.ln.Close() }

// Conn exposes the wrapped connection for QUIC-specific operations the
// transport-agnostic interface deliberately does not cover.
func (s *Session) Conn() *quicgo.Conn { return s.conn }

// OpenStream opens a bidirectional stream, blocking until the peer's stream
// limit allows it or ctx is done.
func (s *Session) OpenStream(ctx context.Context) (portunus.Stream, error) {
	st, err := s.conn.OpenStreamSync(ctx)
	if err != nil {
		return nil, translate(err)
	}
	s.live.Add(1)
	return &Stream{st: st, conn: s.conn, sess: s}, nil
}

// AcceptStream returns the next stream opened by the peer.
func (s *Session) AcceptStream(ctx context.Context) (portunus.Stream, error) {
	st, err := s.conn.AcceptStream(ctx)
	if err != nil {
		return nil, translate(err)
	}
	s.live.Add(1)
	return &Stream{st: st, conn: s.conn, sess: s}, nil
}

// Shutdown drains gracefully: it waits for the streams the application still
// holds to be closed, then closes the connection. It returns ctx.Err() if the
// drain does not finish in time, matching the native session.
//
// QUIC has no GOAWAY equivalent in the transport itself — draining is an
// application-layer concern there, which is why HTTP/3 defines its own — so
// this cannot tell the peer to stop opening new streams. What it can do, and
// does, is not cut off the ones already running: closing on a fixed timer
// instead would truncate an in-flight transfer, and would make the same
// interface method behave differently depending on the transport underneath.
func (s *Session) Shutdown(ctx context.Context) error {
	t := time.NewTicker(2 * time.Millisecond)
	defer t.Stop()
	for {
		if s.live.Load() == 0 {
			return s.Close()
		}
		select {
		case <-ctx.Done():
			_ = s.Close()
			return ctx.Err()
		case <-s.conn.Context().Done():
			return translate(context.Cause(s.conn.Context()))
		case <-t.C:
		}
	}
}

// CloseWithError terminates the connection, reporting code and msg to the
// peer.
func (s *Session) CloseWithError(code uint64, msg string) error {
	return s.conn.CloseWithError(quicgo.ApplicationErrorCode(code), msg)
}

// Close terminates the connection and all its streams.
func (s *Session) Close() error {
	return s.conn.CloseWithError(0, "")
}

func (s *Session) LocalAddr() net.Addr  { return s.conn.LocalAddr() }
func (s *Session) RemoteAddr() net.Addr { return s.conn.RemoteAddr() }

// Stream returns the wrapped QUIC stream for QUIC-specific operations, such
// as Peek or SetReliableBoundary, that the shared interface does not cover.
func (s *Stream) Stream() *quicgo.Stream { return s.st }

func (s *Stream) Read(p []byte) (int, error) {
	n, err := s.st.Read(p)
	return n, translate(err)
}

func (s *Stream) Write(p []byte) (int, error) {
	n, err := s.st.Write(p)
	return n, translate(err)
}

// CloseWrite half-closes the send side; the peer reads io.EOF while this
// side keeps reading. This is what quic-go's Stream.Close does.
func (s *Stream) CloseWrite() error { return translate(s.st.Close()) }

// Close closes both directions: CloseWrite plus cancelling the read side,
// matching the native session's documented semantics.
func (s *Stream) Close() error {
	err := s.st.Close()
	s.st.CancelRead(quicgo.StreamErrorCode(portunus.CodeCanceled))
	// Counted once however often Close is called, so Shutdown's drain
	// cannot be driven negative by a caller that closes twice.
	s.closeOne.Do(func() {
		if s.sess != nil {
			s.sess.live.Add(-1)
		}
	})
	return translate(err)
}

// CancelRead asks the peer to stop sending (STOP_SENDING) and discards
// anything already buffered.
func (s *Stream) CancelRead(code uint64) {
	s.st.CancelRead(quicgo.StreamErrorCode(code))
}

// CancelWrite aborts the send side (RESET_STREAM); the peer's Read fails
// with a *portunus.StreamError carrying code.
func (s *Stream) CancelWrite(code uint64) {
	s.st.CancelWrite(quicgo.StreamErrorCode(code))
}

// StreamID returns the QUIC stream identifier. The shared interface uses
// uint64 precisely so QUIC's 62-bit IDs fit.
func (s *Stream) StreamID() uint64 { return uint64(s.st.StreamID()) }

func (s *Stream) SetDeadline(t time.Time) error      { return s.st.SetDeadline(t) }
func (s *Stream) SetReadDeadline(t time.Time) error  { return s.st.SetReadDeadline(t) }
func (s *Stream) SetWriteDeadline(t time.Time) error { return s.st.SetWriteDeadline(t) }

// LocalAddr and RemoteAddr report the connection's addresses: QUIC streams
// have none of their own, and net.Conn requires them.
func (s *Stream) LocalAddr() net.Addr  { return s.conn.LocalAddr() }
func (s *Stream) RemoteAddr() net.Addr { return s.conn.RemoteAddr() }

// translate converts quic-go's error types into the mux equivalents, so code
// written against the interfaces sees one error vocabulary regardless of
// transport. Errors with no equivalent pass through unchanged.
func translate(err error) error {
	if err == nil {
		return nil
	}
	var se *quicgo.StreamError
	if errors.As(err, &se) {
		return &portunus.StreamError{Code: uint64(se.ErrorCode), Remote: se.Remote}
	}
	var ae *quicgo.ApplicationError
	if errors.As(err, &ae) {
		return &portunus.SessionError{
			Code:   uint64(ae.ErrorCode),
			Reason: ae.ErrorMessage,
			Remote: ae.Remote,
		}
	}
	var te *quicgo.TransportError
	if errors.As(err, &te) {
		return &portunus.SessionError{
			Code:   uint64(te.ErrorCode),
			Reason: te.ErrorMessage,
			Remote: te.Remote,
		}
	}
	return err
}
