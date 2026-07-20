package mux

import (
	"errors"
	"fmt"
)

// Wire-level error codes carried on RST, STOP_SENDING, and GOAWAY frames.
// Values at and above CodeApp are free for application use.
const (
	CodeNone        uint64 = 0
	CodeProtocol    uint64 = 1
	CodeInternal    uint64 = 2
	CodeFlowControl uint64 = 3
	CodeFrameSize   uint64 = 4
	CodeRefused     uint64 = 5
	CodeCanceled    uint64 = 6
	CodeCalm        uint64 = 7 // peer exceeded an abuse limit
	CodeVersion     uint64 = 8

	CodeApp uint64 = 0x100
)

var (
	// ErrSessionClosed is returned after a clean local or remote session
	// close. It deliberately does not satisfy net.Error, so an accept loop
	// (e.g. http.Server on a session used as a net.Listener) terminates
	// instead of hot-looping on a "temporary" error.
	ErrSessionClosed = errors.New("mux: session closed")

	// ErrStreamClosed is returned for operations on a locally closed stream.
	ErrStreamClosed = errors.New("mux: stream closed")

	// ErrGoAway is a retriable error: the session is draining (GOAWAY seen
	// or Shutdown started) and cannot carry new streams. Callers should
	// retry on a fresh session.
	ErrGoAway = errors.New("mux: session draining")

	// ErrStreamsExhausted is a retriable error: the session's stream ID
	// space is nearly exhausted. Callers should rotate to a fresh session.
	ErrStreamsExhausted = errors.New("mux: stream IDs exhausted")

	// ErrRefused reports that the peer refused a stream (accept overflow or
	// draining). Retriable, typically on a different or later session.
	ErrRefused = errors.New("mux: stream refused by peer")
)

// StreamError is a stream reset: either the peer aborted (Remote true, via
// RST or STOP_SENDING) or the local side canceled. It intentionally does not
// implement net.Error.
type StreamError struct {
	Code   uint64
	Remote bool
}

func (e *StreamError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	return fmt.Sprintf("mux: stream reset (%s, code %d)", side, e.Code)
}

// Is makes REFUSED resets match ErrRefused via errors.Is.
func (e *StreamError) Is(target error) bool {
	return target == ErrRefused && e.Code == CodeRefused && e.Remote
}

// SessionError is a fatal session failure. Remote reports whether the peer
// signaled it (GOAWAY) or it was detected locally. The carrier's underlying
// error, if any, is flattened into Reason rather than wrapped: exposing it
// via Unwrap would let net.Error identities (Temporary, Timeout) leak
// through errors.As and hot-loop accept loops.
type SessionError struct {
	Code   uint64
	Reason string
	Remote bool
}

func (e *SessionError) Error() string {
	side := "local"
	if e.Remote {
		side = "remote"
	}
	if e.Reason == "" {
		return fmt.Sprintf("mux: session error (%s, code %d)", side, e.Code)
	}
	return fmt.Sprintf("mux: session error (%s, code %d): %s", side, e.Code, e.Reason)
}
