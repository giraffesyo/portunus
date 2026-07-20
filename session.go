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
	"github.com/giraffesyo/mux/internal/pool"
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

	w *writer // group-commit send path

	bdp       *bdpEstimator
	budget    *budget
	recvTotal atomic.Uint64 // cumulative payload bytes received
	stalls    atomic.Uint64 // times a sender exhausted its granted credit
	nextProbe atomic.Uint64 // PING opaque counter
	lastRecv  atomic.Int64  // UnixNano of the last frame from the peer

	// ctlScratch is reader-owned: control payloads are parsed immediately
	// and never retained, so one reusable buffer serves them all.
	ctlScratch []byte

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
	}
	s.w = newWriter(s)
	s.bdp = newBDPEstimator(uint64(c.InitialWindow), uint64(c.MaxWindow))
	s.budget = newBudget(uint64(c.MaxReceiveBudget), uint64(c.MaxWindow))
	s.lastRecv.Store(time.Now().UnixNano())
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
	if c.NotSentLowat > 0 {
		// Best effort: an unsupported kernel just keeps deeper queues.
		_ = applyNotSentLowat(conn, c.NotSentLowat)
	}

	if err := s.sendSettings(); err != nil {
		return nil, err
	}
	go s.readLoop()
	if c.KeepaliveInterval > 0 {
		go s.keepaliveLoop()
	}
	return s, nil
}

// keepaliveLoop probes the peer on a jittered interval. The PING doubles as
// the BDP sample and as the liveness check; liveness is judged on receive-side
// silence, never on our ability to send, because both peers can be
// send-stalled at once.
func (s *NativeSession) keepaliveLoop() {
	interval := s.cfg.KeepaliveInterval
	// Deterministic per-session jitter spreads probes across many sessions
	// without pulling in math/rand.
	jitter := time.Duration(uint64(time.Now().UnixNano()) % uint64(interval/4))
	t := time.NewTimer(interval - interval/8 + jitter)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
		}
		if s.cfg.KeepaliveTimeout > 0 {
			silent := time.Since(time.Unix(0, s.lastRecv.Load()))
			if silent > s.cfg.KeepaliveTimeout {
				s.fatal(&SessionError{
					Code:   CodeInternal,
					Reason: "peer silent for " + silent.Round(time.Millisecond).String(),
				})
				return
			}
		}
		s.sendProbe()
		t.Reset(interval - interval/8 + jitter)
	}
}

// sendProbe emits a PING that serves as both keepalive and BDP sample.
func (s *NativeSession) sendProbe() {
	opaque := s.nextProbe.Add(1)
	var buf [frame.PingLen]byte
	payload := frame.AppendPing(buf[:0], opaque)
	if s.bdp.shouldProbe(s.recvTotal.Load(), opaque, time.Now()) {
		s.w.appendProbe(frame.Header{Type: frame.TypePing}, payload)
		return
	}
	s.w.appendControlAsync(frame.Header{Type: frame.TypePing}, payload)
}

// maybeProbe starts a BDP sample from the receive path, so probing is paced
// by data arrival rather than by the keepalive timer. Because a probe stays
// in flight for exactly one round trip, this settles at one sample per RTT —
// the fastest honest cadence, and the one that lets the window converge in a
// handful of round trips instead of tens of timer ticks.
func (s *NativeSession) maybeProbe() {
	opaque := s.nextProbe.Add(1)
	if !s.bdp.shouldProbe(s.recvTotal.Load(), opaque, time.Now()) {
		return
	}
	var buf [frame.PingLen]byte
	// Async: this runs on the reader goroutine, which must never block on
	// the send path.
	s.w.appendProbe(frame.Header{Type: frame.TypePing}, frame.AppendPing(buf[:0], opaque))
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
	s.w.appendControl(frame.Header{Type: frame.TypeSettings}, payload)
	return nil
}

// writeData queues one DATA frame for st through the group-commit path.
// copyOnly forces the staging path for streams with an active write
// deadline, whose caller cannot be pinned in an iovec.
func (s *NativeSession) writeData(st *NativeStream, payload []byte, flags frame.Flags, copyOnly bool, deadline <-chan struct{}) error {
	return s.w.appendData(st, payload, flags, copyOnly, deadline)
}

// streamControl queues a per-stream control frame. A stream whose SYN never
// reached the wire is skipped: the peer must never see a frame for a stream
// it has not heard of.
func (s *NativeSession) streamControl(st *NativeStream, h frame.Header, payload []byte) {
	s.w.mu.Lock()
	seen := st.synSent
	s.w.mu.Unlock()
	if !seen {
		return
	}
	h.StreamID = st.id
	s.w.appendControl(h, payload)
}

// The control payloads below are built in stack arrays: stageControl copies
// them into the batch and never retains them, so nothing escapes and the
// control path stays allocation-free.

func (s *NativeSession) writeRST(st *NativeStream, code, finalSize uint64) {
	var buf [frame.RSTLen]byte
	s.streamControl(st, frame.Header{Type: frame.TypeRST},
		frame.AppendRST(buf[:0], code, finalSize))
}

func (s *NativeSession) writeStopSending(st *NativeStream, code uint64) {
	var buf [frame.StopSendingLen]byte
	s.streamControl(st, frame.Header{Type: frame.TypeStopSending},
		frame.AppendStopSending(buf[:0], code))
}

func (s *NativeSession) sendWindowUpdate(st *NativeStream, limit uint64) {
	var buf [frame.WindowUpdateLen]byte
	s.streamControl(st, frame.Header{Type: frame.TypeWindowUpdate},
		frame.AppendWindowUpdate(buf[:0], limit))
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
		s.lastRecv.Store(time.Now().UnixNano())
		if h.Length > s.cfg.MaxFrameSize && h.Type == frame.TypeData {
			s.fatalProtocol(CodeFrameSize, "DATA frame exceeds MaxFrameSize")
			return
		}
		if h.Length > frame.MaxLength {
			s.fatalProtocol(CodeFrameSize, "frame too large")
			return
		}
		// DATA payloads land in pooled segments; when a payload runs past
		// what the (deliberately small) parse buffer holds, bufio hands
		// the remainder straight to ReadFull, so bulk bytes go
		// kernel-to-segment with no intermediate copy. Control payloads
		// are parsed immediately and never retained, so they use a
		// reusable scratch buffer.
		var payload []byte
		var seg []byte
		if h.Length > 0 {
			if h.Type == frame.TypeData {
				seg = pool.Get(int(h.Length))
				payload = seg
			} else {
				if cap(s.ctlScratch) < int(h.Length) {
					s.ctlScratch = make([]byte, h.Length)
				}
				payload = s.ctlScratch[:h.Length]
			}
			if _, err := io.ReadFull(br, payload); err != nil {
				if seg != nil {
					pool.Put(seg)
				}
				s.readEnded(err)
				return
			}
		}
		if err := s.dispatch(h, payload, seg); err != nil {
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

// dispatch routes one frame. seg is the pooled backing buffer for a DATA
// payload and is released here on every path that does not hand it to a
// stream. A non-nil return means the session is dead.
func (s *NativeSession) dispatch(h frame.Header, payload, seg []byte) error {
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
		if h.Flags&frame.FlagACK != 0 {
			// Our probe came home: close out the BDP sample. A larger
			// window target is adopted by streams as they grow.
			s.bdp.onACK(opaque, s.recvTotal.Load(), s.stalls.Load(), time.Now())
			return nil
		}
		// Async: the reader must never block on the send path.
		var buf [frame.PingLen]byte
		s.w.appendControlAsync(
			frame.Header{Type: frame.TypePing, Flags: frame.FlagACK},
			frame.AppendPing(buf[:0], opaque))
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
		return s.dispatchStream(h, payload, seg)

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
// rules from DESIGN.md. seg is released on every path that does not hand it
// to a stream's receive queue.
func (s *NativeSession) dispatchStream(h frame.Header, payload, seg []byte) error {
	if seg != nil {
		// Released unless onData takes ownership below.
		defer func() {
			if seg != nil {
				pool.Put(seg)
			}
		}()
	}
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
			// dropped and never counted against any budget. Async: this
			// runs on the reader goroutine.
			var buf [frame.RSTLen]byte
			s.w.appendControlAsync(
				frame.Header{Type: frame.TypeRST, StreamID: h.StreamID},
				frame.AppendRST(buf[:0], CodeRefused, 0))
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
		err := s.onData(st, h, payload, seg)
		seg = nil // ownership passed to onData
		return err
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

// onData takes ownership of seg (the pooled buffer backing payload) and is
// responsible for releasing it on every path where it is not queued.
func (s *NativeSession) onData(st *NativeStream, h frame.Header, payload, seg []byte) error {
	queued := false
	defer func() {
		if seg != nil && !queued {
			pool.Put(seg)
		}
	}()

	st.mu.Lock()
	if st.finRecvd && (len(payload) > 0 || h.Flags&frame.FlagFIN != 0) {
		st.mu.Unlock()
		s.fatalProtocol(CodeProtocol, "DATA after FIN")
		return errSessionTerminated
	}
	s.recvTotal.Add(uint64(len(payload)))
	st.recvd += uint64(len(payload))
	if st.recvd > st.recvLimit {
		st.mu.Unlock()
		s.fatalProtocol(CodeFlowControl, "peer exceeded stream window")
		return errSessionTerminated
	}
	// A sender with less than a frame of credit left is window-limited: it
	// cannot send a full frame without waiting for us. That is the signal
	// autotuning grows on. Testing for exactly zero credit would almost
	// never fire, since the limit is not frame-aligned.
	if st.recvLimit-st.recvd < uint64(s.cfg.MaxFrameSize) {
		s.stalls.Add(1)
		st.stalledSinceGrow = true
	}
	drop := st.readClosed || st.readErr != nil
	if !drop && len(payload) > 0 {
		st.rq = append(st.rq, segment{buf: seg, data: payload})
		queued = true
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
	if len(payload) > 0 {
		s.maybeProbe()
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
	_, ok := s.streams[st.id]
	if ok {
		delete(s.streams, st.id)
		if !st.local {
			s.incoming--
		}
	}
	s.mu.Unlock()

	if ok {
		st.mu.Lock()
		window := st.window
		st.mu.Unlock()
		s.budget.release(window)
	}
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
	// Async: sendGoAway is reached both from application goroutines and
	// from the reader's protocol-error path, which must never block.
	s.w.appendControlAsync(frame.Header{Type: frame.TypeGoAway}, payload)
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
		// Release parked writers before closing the carrier, so nobody is
		// left waiting on a flush that will never complete.
		s.w.fail(err)
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
