// Package mux multiplexes many logical streams over a single reliable
// byte-stream carrier — a TCP or TLS connection, a Unix socket, or anything
// else satisfying net.Conn.
//
// Streams implement net.Conn, so they drop into existing code, and add the
// QUIC-shaped controls that net.Conn lacks: half-close, and read/write
// cancellation carrying an application error code.
//
//	sess, err := mux.Client(conn, nil)
//	st, err := sess.OpenStream(ctx)
//	st.Write(request)
//	st.CloseWrite()          // peer sees EOF; we can still read the reply
//	io.Copy(os.Stdout, st)
//
// The core has no dependencies outside the standard library. The Session and
// Stream interfaces are also satisfied by the QUIC adapter in
// adapters/quic, so transport-agnostic code can hold either.
package portunus

import (
	"context"
	"net"
)

// Session multiplexes streams over one carrier. It is satisfied by
// *NativeSession and by the QUIC adapter.
type Session interface {
	// OpenStream opens a new stream. It does not touch the carrier: the
	// stream announces itself on its first frame.
	OpenStream(ctx context.Context) (Stream, error)

	// AcceptStream returns the next stream opened by the peer.
	AcceptStream(ctx context.Context) (Stream, error)

	// Shutdown drains gracefully: refuse new streams, let live ones finish,
	// then close. It returns ctx.Err() if the drain does not complete.
	Shutdown(ctx context.Context) error

	// CloseWithError terminates the session, reporting code and msg to the
	// peer. msg is truncated to the protocol's reason cap.
	CloseWithError(code uint64, msg string) error

	// Close terminates the session and every stream on it.
	Close() error

	LocalAddr() net.Addr
	RemoteAddr() net.Addr
}

// Stream is one logical stream. Beyond net.Conn it offers half-close and
// cancellation with error codes, mirroring QUIC stream semantics.
type Stream interface {
	net.Conn

	// CloseWrite half-closes the send side: the peer reads io.EOF while
	// this side keeps reading.
	CloseWrite() error

	// CancelRead abandons the receive side, discarding buffered and future
	// data and asking the peer to stop sending.
	CancelRead(code uint64)

	// CancelWrite aborts the send side; the peer's Read fails with a
	// *StreamError carrying code.
	CancelWrite(code uint64)

	// StreamID returns the wire identifier of the stream.
	StreamID() uint64
}

var (
	_ Session = (*NativeSession)(nil)
	_ Stream  = (*NativeStream)(nil)
)
