package mux

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/giraffesyo/mux/internal/frame"
)

// NativeSession multiplexes streams over a reliable byte-stream carrier
// (TCP, TLS, Unix socket, or anything else satisfying net.Conn).
//
// M2 note: writes are serialized under a single mutex and issued directly.
// The group-commit batching path (DESIGN.md M3) replaces writeFrame's
// internals without changing this file's structure.
type NativeSession struct {
	conn net.Conn
	cfg  Config

	client   bool
	nextID   uint32 // next locally-initiated stream ID
	idsSpent atomic.Bool

	// Peer settings, read on hot paths without locking.
	peerInitialWindow atomic.Uint64
	peerMaxFrame      atomic.Uint32
	peerMaxStreams    atomic.Uint64

	writeMu sync.Mutex // serializes carrier writes; held across one frame
	hdrBuf  [frame.HeaderSize]byte
	ctlBuf  []byte

	mu       sync.Mutex // guards the fields below
	streams  map[uint32]*NativeStream
	incoming int    // live peer-initiated streams (slot accounting)
	maxSeen  uint32 // highest peer-initiated ID observed
	lastLoc  uint32 // highest locally-initiated ID sent
	goneAway bool   // peer sent GOAWAY or we sent one
	peerLast uint32 // lastID from the peer's GOAWAY
	closed   bool
	closeErr error

	accept   chan *NativeStream
	acceptMu sync.Mutex // serializes AcceptStream callers' backlog draining

	done      chan struct{} // closed once the session is dead
	closeOnce sync.Once
	readDone  chan struct{}
}

// Client starts a session on the dialing side (odd stream IDs).
//
// It writes the SETTINGS handshake before returning, which any buffered
// carrier absorbs immediately. On a synchronous carrier such as net.Pipe the
// peer must already be reading, or the call blocks until WriteTimeout.
func Client(conn net.Conn, cfg *Config) (*NativeSession, error) {
	return newSession(conn, cfg, true)
}

// Server starts a session on the accepting side (even stream IDs). It writes
// the SETTINGS handshake before returning; see Client.
func Server(conn net.Conn, cfg *Config) (*NativeSession, error) {
	return newSession(conn, cfg, false)
}

func newSession(conn net.Conn, cfg *Config, client bool) (*NativeSession, error) {
	c, err := buildConfig(cfg)
	if err != nil {
		return nil, err
	}
	s := &NativeSession{
		conn:     conn,
		cfg:      c,
		client:   client,
		streams:  make(map[uint32]*NativeStream),
		accept:   make(chan *NativeStream, c.AcceptBacklog),
		done:     make(chan struct{}),
		readDone: make(chan struct{}),
		ctlBuf:   make([]byte, 0, 64),
	}
	if client {
		s.nextID = 1
	} else {
		s.nextID = 2
	}
	// Conservative until the peer's SETTINGS arrives: protocol floors.
	s.peerInitialWindow.Store(frame.FloorInitialWindow)
	s.peerMaxFrame.Store(frame.FloorMaxFrameSize)
	s.peerMaxStreams.Store(math.MaxUint32)

	// Group commit does Nagle's job in userspace; the kernel's version
	// would only serialize our writes against delayed ACKs.
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	if err := s.sendSettings(); err != nil {
		return nil, err
	}
	go s.readLoop()
	return s, nil
}

func (s *NativeSession) sendSettings() error {
	settings := []frame.Setting{
		{ID: frame.SettingVersion, Value: frame.ProtocolVersion},
		{ID: frame.SettingInitialWindow, Value: uint64(s.cfg.InitialWindow)},
		{ID: frame.SettingMaxFrameSize, Value: uint64(s.cfg.MaxFrameSize)},
		{ID: frame.SettingMaxConcurrentStreams, Value: uint64(s.cfg.MaxIncomingStreams)},
	}
	payload, err := frame.AppendSettings(nil, settings)
	if err != nil {
		return err
	}
	return s.writeFrame(frame.Header{
		Type:   frame.TypeSettings,
		Length: uint32(len(payload)),
	}, payload)
}

// writeFrame writes one header (+payload) to the carrier under writeMu. A
// carrier write error or timeout is session-fatal: a partially written frame
// has already desynced the wire.
func (s *NativeSession) writeFrame(h frame.Header, payload []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.writeFrameLocked(h, payload)
}

func (s *NativeSession) writeFrameLocked(h frame.Header, payload []byte) error {
	if err := s.closedErr(); err != nil {
		return err
	}
	if s.cfg.WriteTimeout > 0 {
		_ = s.conn.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
	}
	h.Encode(s.hdrBuf[:])
	var err error
	if len(payload) == 0 {
		_, err = s.conn.Write(s.hdrBuf[:])
	} else {
		// M2 keeps this simple and correct: two writes. M3's group commit
		// replaces it with one batched writev across streams.
		if _, err = s.conn.Write(s.hdrBuf[:]); err == nil {
			_, err = s.conn.Write(payload)
		}
	}
	if s.cfg.WriteTimeout > 0 {
		_ = s.conn.SetWriteDeadline(time.Time{})
	}
	if err != nil {
		s.fatal(&SessionError{Code: CodeInternal, Reason: "carrier write: " + err.Error()})
		return s.closedErr()
	}
	return nil
}

// writeData sends one DATA frame for s, attaching SYN if this is the
// stream's first frame on the wire. synSent is flipped under writeMu so SYN
// cannot race ahead of, or duplicate across, concurrent senders.
func (s *NativeSession) writeData(st *NativeStream, payload []byte, flags frame.Flags) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !st.synSent {
		flags |= frame.FlagSYN
		st.synSent = true
	}
	return s.writeFrameLocked(frame.Header{
		Type:     frame.TypeData,
		Flags:    flags,
		StreamID: st.id,
		Length:   uint32(len(payload)),
	}, payload)
}

// writeRST resets st's send side. A stream whose SYN never reached the wire
// is canceled purely locally: the peer must never see an RST for a stream it
// has not heard of.
func (s *NativeSession) writeRST(st *NativeStream, code, finalSize uint64) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !st.synSent {
		return
	}
	s.ctlBuf = frame.AppendRST(s.ctlBuf[:0], code, finalSize)
	_ = s.writeFrameLocked(frame.Header{
		Type:     frame.TypeRST,
		StreamID: st.id,
		Length:   frame.RSTLen,
	}, s.ctlBuf)
}

func (s *NativeSession) writeStopSending(st *NativeStream, code uint64) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !st.synSent {
		return
	}
	s.ctlBuf = frame.AppendStopSending(s.ctlBuf[:0], code)
	_ = s.writeFrameLocked(frame.Header{
		Type:     frame.TypeStopSending,
		StreamID: st.id,
		Length:   frame.StopSendingLen,
	}, s.ctlBuf)
}

func (s *NativeSession) sendWindowUpdate(st *NativeStream, limit uint64) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !st.synSent {
		// Nothing sent yet on a locally opened stream: the peer has no
		// state for this ID. Our SYN will ride the first DATA frame.
		return
	}
	s.ctlBuf = frame.AppendWindowUpdate(s.ctlBuf[:0], limit)
	_ = s.writeFrameLocked(frame.Header{
		Type:     frame.TypeWindowUpdate,
		StreamID: st.id,
		Length:   frame.WindowUpdateLen,
	}, s.ctlBuf)
}

// OpenStream opens a stream. It is local and costs no syscall: SYN rides the
// stream's first frame.
func (s *NativeSession) OpenStream(ctx context.Context) (Stream, error) {
	st, err := s.openStream(ctx)
	if err != nil {
		return nil, err
	}
	return st, nil
}

func (s *NativeSession) openStream(ctx context.Context) (*NativeStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, s.closeErrLocked()
	}
	if s.goneAway {
		return nil, ErrGoAway
	}
	// Reserve two IDs of headroom so the increment below cannot wrap.
	if s.nextID > math.MaxUint32-2 {
		s.idsSpent.Store(true)
		return nil, ErrStreamsExhausted
	}
	id := s.nextID
	s.nextID += 2
	s.lastLoc = id
	st := newStream(s, id, true)
	s.streams[id] = st
	return st, nil
}

// AcceptStream returns the next stream opened by the peer.
func (s *NativeSession) AcceptStream(ctx context.Context) (Stream, error) {
	select {
	case st := <-s.accept:
		return st, nil
	default:
	}
	select {
	case st := <-s.accept:
		return st, nil
	case <-s.done:
		// Drain anything already accepted before reporting death.
		select {
		case st := <-s.accept:
			return st, nil
		default:
		}
		return nil, s.closedErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// readLoop is the single reader goroutine. It never blocks on a send-side
// resource and never parks holding a stream lock.
func (s *NativeSession) readLoop() {
	defer close(s.readDone)
	br := bufio.NewReaderSize(s.conn, s.cfg.ReadBufferSize)
	var hdr [frame.HeaderSize]byte
	for {
		if _, err := io.ReadFull(br, hdr[:]); err != nil {
			s.readEnded(err)
			return
		}
		h := frame.ParseHeader(hdr[:])
		if h.Length > s.cfg.MaxFrameSize && h.Type == frame.TypeData {
			s.fatalProtocol(CodeFrameSize, "DATA frame exceeds MaxFrameSize")
			return
		}
		if h.Length > frame.MaxLength {
			s.fatalProtocol(CodeFrameSize, "frame too large")
			return
		}
		var payload []byte
		if h.Length > 0 {
			payload = make([]byte, h.Length)
			if _, err := io.ReadFull(br, payload); err != nil {
				s.readEnded(err)
				return
			}
		}
		if err := s.dispatch(h, payload); err != nil {
			return // dispatch already terminated the session
		}
	}
}

func (s *NativeSession) readEnded(err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		s.fatal(&SessionError{Code: CodeNone, Reason: "carrier closed by peer", Remote: true})
		return
	}
	s.fatal(&SessionError{Code: CodeInternal, Reason: "carrier read: " + err.Error()})
}

var errSessionTerminated = errors.New("session terminated")

func (s *NativeSession) fatalProtocol(code uint64, reason string) {
	s.sendGoAway(code, reason)
	s.fatal(&SessionError{Code: code, Reason: reason})
}

// dispatch routes one frame. A non-nil return means the session is dead.
func (s *NativeSession) dispatch(h frame.Header, payload []byte) error {
	switch h.Type {
	case frame.TypeSettings:
		if h.StreamID != 0 {
			s.fatalProtocol(CodeProtocol, "SETTINGS on non-zero stream")
			return errSessionTerminated
		}
		if h.Flags&frame.FlagACK != 0 {
			return nil
		}
		settings, err := frame.ParseSettings(payload)
		if err != nil {
			s.fatalProtocol(CodeFrameSize, "malformed SETTINGS")
			return errSessionTerminated
		}
		return s.applySettings(settings)

	case frame.TypePing:
		if h.StreamID != 0 {
			s.fatalProtocol(CodeProtocol, "PING on non-zero stream")
			return errSessionTerminated
		}
		opaque, err := frame.ParsePing(payload)
		if err != nil {
			s.fatalProtocol(CodeFrameSize, "malformed PING")
			return errSessionTerminated
		}
		if h.Flags&frame.FlagACK == 0 {
			var buf [frame.PingLen]byte
			frame.AppendPing(buf[:0], opaque)
			_ = s.writeFrame(frame.Header{
				Type:   frame.TypePing,
				Flags:  frame.FlagACK,
				Length: frame.PingLen,
			}, buf[:])
		}
		return nil

	case frame.TypeGoAway:
		lastID, code, reason, err := frame.ParseGoAway(payload)
		if err != nil {
			s.fatalProtocol(CodeFrameSize, "malformed GOAWAY")
			return errSessionTerminated
		}
		s.handleGoAway(lastID, code, string(reason))
		return nil

	case frame.TypeData, frame.TypeWindowUpdate, frame.TypeRST, frame.TypeStopSending:
		return s.dispatchStream(h, payload)

	default:
		// Type 7 (PADDING) is reserved for v2; 8-15 are unused. Both are
		// protocol errors in v1.
		s.fatalProtocol(CodeProtocol, "unknown frame type")
		return errSessionTerminated
	}
}

func (s *NativeSession) applySettings(settings []frame.Setting) error {
	for _, set := range settings {
		switch set.ID {
		case frame.SettingVersion:
			if set.Value != frame.ProtocolVersion {
				s.fatalProtocol(CodeVersion, "unsupported protocol version")
				return errSessionTerminated
			}
		case frame.SettingInitialWindow:
			if set.Value < frame.FloorInitialWindow || set.Value > math.MaxUint32 {
				s.fatalProtocol(CodeFlowControl, "peer initial window out of range")
				return errSessionTerminated
			}
			old := s.peerInitialWindow.Swap(set.Value)
			// Streams created before SETTINGS arrived were seeded with the
			// floor; raise their granted limit to the negotiated value.
			if set.Value > old {
				s.raiseInitialLimits(set.Value)
			}
		case frame.SettingMaxFrameSize:
			if set.Value < frame.FloorMaxFrameSize || set.Value > frame.MaxLength {
				s.fatalProtocol(CodeFrameSize, "peer max frame size out of range")
				return errSessionTerminated
			}
			s.peerMaxFrame.Store(uint32(set.Value))
		case frame.SettingMaxConcurrentStreams:
			s.peerMaxStreams.Store(set.Value)
		}
		// Unknown IDs are ignored: forward compatibility.
	}
	return nil
}

// raiseInitialLimits lifts the granted send limit of streams that were
// created before the peer's SETTINGS arrived.
func (s *NativeSession) raiseInitialLimits(window uint64) {
	s.mu.Lock()
	sts := make([]*NativeStream, 0, len(s.streams))
	for _, st := range s.streams {
		sts = append(sts, st)
	}
	s.mu.Unlock()
	for _, st := range sts {
		st.mu.Lock()
		if window > st.sendLimit {
			st.sendLimit = window
			st.mu.Unlock()
			notify(st.writable)
			continue
		}
		st.mu.Unlock()
	}
}

// dispatchStream handles the per-stream frame types, applying the receiver
// rules from DESIGN.md.
func (s *NativeSession) dispatchStream(h frame.Header, payload []byte) error {
	if h.StreamID == 0 {
		s.fatalProtocol(CodeProtocol, "stream frame on session ID 0")
		return errSessionTerminated
	}
	remoteOdd := !s.client // peer-initiated IDs are odd iff we are the server
	isRemote := (h.StreamID%2 == 1) == remoteOdd

	s.mu.Lock()
	st, live := s.streams[h.StreamID]
	switch {
	case live:
		if h.Flags&frame.FlagSYN != 0 && h.Type != frame.TypeData {
			s.mu.Unlock()
			s.fatalProtocol(CodeProtocol, "SYN on a control frame")
			return errSessionTerminated
		}
		if h.Flags&frame.FlagSYN != 0 && st.synRecvd {
			s.mu.Unlock()
			s.fatalProtocol(CodeProtocol, "SYN for an existing stream")
			return errSessionTerminated
		}
		s.mu.Unlock()

	case h.Flags&frame.FlagSYN != 0 && isRemote:
		// SYN order is not ID order: the peer assigns IDs locally at open
		// and announces each stream on its first frame, so a higher ID can
		// legitimately arrive first. Only currently-live IDs collide.
		if h.StreamID > s.maxSeen {
			s.maxSeen = h.StreamID
		}
		refuse := s.closed || s.goneAway ||
			s.incoming >= s.cfg.MaxIncomingStreams ||
			len(s.accept) >= s.cfg.AcceptBacklog
		if refuse {
			s.mu.Unlock()
			// No state is created for a refused stream; the payload is
			// dropped and never counted against any budget.
			s.refuse(h.StreamID)
			return nil
		}
		st = newStream(s, h.StreamID, false)
		// The peer has heard of this stream by definition, so our control
		// frames for it may go out immediately.
		st.synSent = true
		st.synRecvd = true
		s.streams[h.StreamID] = st
		s.incoming++
		s.mu.Unlock()
		s.accept <- st

	case isRemote && h.StreamID <= s.maxSeen:
		// Straggler for a stream we already tore down: drop silently. The
		// payload is never counted against any receive budget.
		s.mu.Unlock()
		return nil

	case !isRemote && h.StreamID <= s.lastLoc:
		// Straggler for a locally opened stream that is gone.
		s.mu.Unlock()
		return nil

	default:
		s.mu.Unlock()
		s.fatalProtocol(CodeProtocol, "frame for unopened stream")
		return errSessionTerminated
	}

	switch h.Type {
	case frame.TypeData:
		return s.onData(st, h, payload)
	case frame.TypeWindowUpdate:
		limit, err := frame.ParseWindowUpdate(payload)
		if err != nil {
			s.fatalProtocol(CodeFrameSize, "malformed WINDOW_UPDATE")
			return errSessionTerminated
		}
		return s.onWindowUpdate(st, limit)
	case frame.TypeRST:
		code, _, err := frame.ParseRST(payload)
		if err != nil {
			s.fatalProtocol(CodeFrameSize, "malformed RST")
			return errSessionTerminated
		}
		s.onRST(st, code)
	case frame.TypeStopSending:
		code, err := frame.ParseStopSending(payload)
		if err != nil {
			s.fatalProtocol(CodeFrameSize, "malformed STOP_SENDING")
			return errSessionTerminated
		}
		s.onStopSending(st, code)
	}
	return nil
}

// refuse rejects a stream we will not accept. RST is safe here: the peer's
// SYN proves it knows the ID.
func (s *NativeSession) refuse(id uint32) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.ctlBuf = frame.AppendRST(s.ctlBuf[:0], CodeRefused, 0)
	_ = s.writeFrameLocked(frame.Header{
		Type:     frame.TypeRST,
		StreamID: id,
		Length:   frame.RSTLen,
	}, s.ctlBuf)
}

func (s *NativeSession) onData(st *NativeStream, h frame.Header, payload []byte) error {
	st.mu.Lock()
	if st.finRecvd && (len(payload) > 0 || h.Flags&frame.FlagFIN != 0) {
		st.mu.Unlock()
		s.fatalProtocol(CodeProtocol, "DATA after FIN")
		return errSessionTerminated
	}
	st.recvd += uint64(len(payload))
	if st.recvd > st.recvLimit {
		st.mu.Unlock()
		s.fatalProtocol(CodeFlowControl, "peer exceeded stream window")
		return errSessionTerminated
	}
	drop := st.readClosed || st.readErr != nil
	if !drop && len(payload) > 0 {
		st.rq = append(st.rq, payload)
	}
	if h.Flags&frame.FlagFIN != 0 {
		st.finRecvd = true
	}
	// A discarded payload still consumed advertised credit; return it so a
	// peer sending into a canceled read side is not stalled forever.
	var limit uint64
	if drop && len(payload) > 0 {
		st.consumed = st.recvd
		limit = st.maybeGrowRecvLimitLocked()
	}
	st.mu.Unlock()
	notify(st.readable)
	if limit > 0 {
		s.sendWindowUpdate(st, limit)
	}
	s.maybeRemove(st)
	return nil
}

func (s *NativeSession) onWindowUpdate(st *NativeStream, limit uint64) error {
	st.mu.Lock()
	if limit < st.sendLimit {
		st.mu.Unlock()
		s.fatalProtocol(CodeFlowControl, "WINDOW_UPDATE limit moved backwards")
		return errSessionTerminated
	}
	if limit == st.sendLimit {
		st.mu.Unlock()
		s.fatalProtocol(CodeFlowControl, "WINDOW_UPDATE grants nothing")
		return errSessionTerminated
	}
	st.sendLimit = limit
	st.mu.Unlock()
	notify(st.writable)
	return nil
}

func (s *NativeSession) onRST(st *NativeStream, code uint64) {
	err := &StreamError{Code: code, Remote: true}
	st.mu.Lock()
	if st.readErr == nil {
		st.readErr = err
	}
	st.rq = nil
	st.readClosed = true
	st.mu.Unlock()
	notify(st.readable)
	s.maybeRemove(st)
}

func (s *NativeSession) onStopSending(st *NativeStream, code uint64) {
	st.mu.Lock()
	if st.writeErr == nil {
		st.writeErr = &StreamError{Code: code, Remote: true}
	}
	st.mu.Unlock()
	notify(st.writable)
	s.maybeRemove(st)
}

// maybeRemove drops a stream once both directions are finished, releasing
// its accept-slot. An incoming stream holds its slot until the application
// closes it: freeing on peer reset alone is the Rapid Reset hole.
func (s *NativeSession) maybeRemove(st *NativeStream) {
	st.mu.Lock()
	done := st.sendDoneLocked() && st.recvDoneLocked()
	if !st.local && !st.appClosed {
		done = false
	}
	if !done || st.slotReleased {
		st.mu.Unlock()
		return
	}
	st.slotReleased = true
	st.mu.Unlock()

	s.mu.Lock()
	if _, ok := s.streams[st.id]; ok {
		delete(s.streams, st.id)
		if !st.local {
			s.incoming--
		}
	}
	s.mu.Unlock()
}

func (s *NativeSession) handleGoAway(lastID uint32, code uint64, reason string) {
	s.mu.Lock()
	s.goneAway = true
	s.peerLast = lastID
	var doomed []*NativeStream
	for id, st := range s.streams {
		// Locally opened streams above the peer's last processed ID were
		// never seen: fail them retriably.
		if st.local && id > lastID {
			doomed = append(doomed, st)
		}
	}
	s.mu.Unlock()

	for _, st := range doomed {
		st.destroy(ErrGoAway)
	}
	if code != CodeNone {
		s.fatal(&SessionError{Code: code, Reason: reason, Remote: true})
	}
}

func (s *NativeSession) sendGoAway(code uint64, reason string) {
	s.mu.Lock()
	if s.goneAway && code == CodeNone {
		s.mu.Unlock()
		return
	}
	s.goneAway = true
	last := s.maxSeen
	s.mu.Unlock()

	if len(reason) > frame.MaxGoAwayReason {
		reason = reason[:frame.MaxGoAwayReason]
	}
	payload, err := frame.AppendGoAway(nil, last, code, []byte(reason))
	if err != nil {
		return
	}
	_ = s.writeFrame(frame.Header{
		Type:   frame.TypeGoAway,
		Length: uint32(len(payload)),
	}, payload)
}

// Shutdown drains gracefully: GOAWAY, stop accepting new streams, then wait
// for live streams to finish or ctx to expire.
func (s *NativeSession) Shutdown(ctx context.Context) error {
	s.sendGoAway(CodeNone, "")
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	for {
		s.mu.Lock()
		n := len(s.streams)
		s.mu.Unlock()
		if n == 0 {
			return s.Close()
		}
		select {
		case <-ctx.Done():
			_ = s.Close()
			return ctx.Err()
		case <-s.done:
			return s.closedErr()
		case <-tick.C:
		}
	}
}

// CloseWithError terminates the session, informing the peer with a code and
// an optional reason (truncated to the wire cap).
func (s *NativeSession) CloseWithError(code uint64, msg string) error {
	s.sendGoAway(code, msg)
	s.fatal(&SessionError{Code: code, Reason: msg})
	return nil
}

// Close terminates the session and all its streams.
func (s *NativeSession) Close() error {
	s.sendGoAway(CodeNone, "")
	s.fatal(&SessionError{Code: CodeNone, Reason: "closed locally"})
	return nil
}

// fatal terminates the session exactly once. Every stream is failed, so no
// caller can park forever, and the carrier is closed to unblock the reader.
func (s *NativeSession) fatal(err error) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.closeErr = err
		sts := make([]*NativeStream, 0, len(s.streams))
		for _, st := range s.streams {
			sts = append(sts, st)
		}
		s.mu.Unlock()

		close(s.done)
		for _, st := range sts {
			st.destroy(err)
		}
		_ = s.conn.Close()
	})
}

func (s *NativeSession) closedErr() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErrLocked()
}

func (s *NativeSession) closeErrLocked() error {
	if !s.closed {
		return nil
	}
	if s.closeErr != nil {
		return s.closeErr
	}
	return ErrSessionClosed
}

// Wait blocks until the session has terminated and returns the reason.
func (s *NativeSession) Wait() error {
	<-s.done
	<-s.readDone
	return s.closedErr()
}

func (s *NativeSession) LocalAddr() net.Addr  { return s.conn.LocalAddr() }
func (s *NativeSession) RemoteAddr() net.Addr { return s.conn.RemoteAddr() }
