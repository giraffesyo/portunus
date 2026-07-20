package mux

import (
	"fmt"
	"time"

	"github.com/giraffesyo/mux/internal/frame"
)

// Config configures a session. The zero value (or nil) selects defaults.
// Fields set to zero take their default; durations set to a negative value
// disable the mechanism where that is meaningful.
type Config struct {
	// InitialWindow is the per-stream receive window advertised to the
	// peer. Minimum frame.FloorInitialWindow (64KB): senders use the floor
	// until SETTINGS arrives, so advertising less would let a conformant
	// peer overrun us. Default 256KB.
	InitialWindow uint32

	// MaxWindow caps how far BDP autotuning may grow a single stream's
	// receive window. A window of at least bandwidth × RTT is what keeps a
	// long path from capping at window/RTT. Default 16MB.
	MaxWindow uint32

	// MaxReceiveBudget bounds the total receive credit outstanding across
	// the session, so autotuning cannot turn many briefly-fast streams into
	// an OOM. It only ever declines to grow a window; granted credit is
	// never revoked. Default 128MB.
	MaxReceiveBudget int

	// KeepaliveInterval is the period between PINGs, which double as the
	// BDP probe and the liveness check. Zero selects 20s; negative
	// disables keepalive (and with it, autotuning's RTT samples).
	KeepaliveInterval time.Duration

	// KeepaliveTimeout declares the peer dead after this much silence on
	// the receive side — never on our ability to send, since both peers can
	// be send-stalled at once. Zero selects three intervals; negative
	// disables the liveness check.
	KeepaliveTimeout time.Duration

	// NotSentLowat caps how much unsent data the kernel holds in the socket
	// send buffer (TCP_NOTSENT_LOWAT, Linux and macOS). Without it a small
	// message queued behind bulk writers waits for the whole kernel buffer
	// to drain, so tail latency reflects kernel bufferbloat rather than our
	// own queue — the one queue our batching and fairness caps cannot
	// reach. Zero selects 128KB on TCP carriers; negative leaves the
	// kernel default. Ignored where the option does not exist.
	NotSentLowat int

	// StreamIdleTimeout resets streams that have carried no data in either
	// direction for this long. It is off by default: a quiet stream is
	// normal for a tunnel holding a connection open, and reaping those
	// would break more than it protects.
	//
	// Turn it on for a server facing untrusted peers. Without it, a peer
	// can open MaxIncomingStreams streams, go silent, and hold every slot
	// indefinitely — the application cannot tell that apart from a slow
	// client, so only a timeout can.
	StreamIdleTimeout time.Duration

	// MaxResetsPerSecond optionally bounds how fast a peer may reset
	// streams. It is off by default: closing a stream whose peer is still
	// sending emits a STOP_SENDING, so a proxy resets as fast as it churns
	// connections, and any threshold tight enough to matter breaks that.
	//
	// The structural Rapid Reset defense is always on regardless — an
	// incoming stream holds its slot until the application closes it, never
	// freed early by a peer reset.
	MaxResetsPerSecond int

	// MaxFrameSize is the largest DATA payload we accept. Bounds between
	// frame.FloorMaxFrameSize (16KB) and frame.MaxLength. Default 64KB.
	MaxFrameSize uint32

	// MaxIncomingStreams caps concurrently live peer-initiated streams. A
	// stream occupies its slot until the application closes it (never freed
	// early by peer reset — the Rapid Reset lesson). Default 1024.
	MaxIncomingStreams int

	// AcceptBacklog caps streams opened by the peer but not yet returned by
	// AcceptStream. Overflow refuses the stream (RST, CodeRefused).
	// Default 128.
	AcceptBacklog int

	// WriteTimeout bounds each carrier write. Expiry is session-fatal: a
	// partially written frame has already desynced the wire. Zero selects
	// the 30s default; negative disables.
	//
	// Disabling it removes the only bound on a large Write whose payload is
	// referenced rather than copied: such a writer parks until its flush
	// resolves, and a per-stream write deadline cannot release it, since a
	// stream with a deadline takes the copy path instead. With both
	// disabled, a peer that stops reading parks that goroutine until the
	// session ends.
	WriteTimeout time.Duration

	// ReadBufferSize is the parse buffer for the session reader. It is
	// deliberately small: a large buffer would slurp bulk payloads into
	// itself, so the direct kernel-to-segment read for large frames would
	// almost never fire. Zero selects 16KB.
	ReadBufferSize int

	// MaxBatchBytes bounds one group-commit batch. It caps flush duration
	// — every co-batched writer's latency floor — along with staging
	// occupancy and the coalesce buffer for non-writev carriers. Writers
	// beyond it block at admission, where write deadlines still apply.
	// Zero selects 512KB.
	MaxBatchBytes int

	// PerStreamBatchBytes bounds how much one stream may place in a single
	// batch, so a bulk transfer cannot monopolize consecutive batches and
	// push small messages on other streams behind it. Zero selects a
	// quarter of MaxBatchBytes.
	PerStreamBatchBytes int
}

const (
	defaultInitialWindow  = 256 << 10
	defaultMaxFrameSize   = 64 << 10
	defaultMaxIncoming    = 1024
	defaultAcceptBacklog  = 128
	defaultWriteTimeout   = 30 * time.Second
	defaultReadBufferSize = 16 << 10
	defaultMaxBatchBytes  = 512 << 10
	defaultMaxWindow      = 16 << 20
	defaultRecvBudget     = 128 << 20
	defaultKeepalive      = 20 * time.Second
	defaultNotSentLowat   = 128 << 10
)

func buildConfig(in *Config) (Config, error) {
	var c Config
	if in != nil {
		c = *in
	}
	if c.InitialWindow == 0 {
		c.InitialWindow = defaultInitialWindow
	}
	if c.MaxFrameSize == 0 {
		c.MaxFrameSize = defaultMaxFrameSize
	}
	if c.MaxIncomingStreams == 0 {
		c.MaxIncomingStreams = defaultMaxIncoming
	}
	if c.AcceptBacklog == 0 {
		c.AcceptBacklog = defaultAcceptBacklog
	}
	switch {
	case c.WriteTimeout == 0:
		c.WriteTimeout = defaultWriteTimeout
	case c.WriteTimeout < 0:
		c.WriteTimeout = 0
	}
	if c.ReadBufferSize == 0 {
		c.ReadBufferSize = defaultReadBufferSize
	}
	if c.MaxBatchBytes == 0 {
		c.MaxBatchBytes = defaultMaxBatchBytes
	}
	switch {
	case c.NotSentLowat == 0:
		c.NotSentLowat = defaultNotSentLowat
	case c.NotSentLowat < 0:
		c.NotSentLowat = 0
	}
	if c.PerStreamBatchBytes == 0 {
		c.PerStreamBatchBytes = c.MaxBatchBytes / 4
	}
	if c.MaxWindow == 0 {
		c.MaxWindow = defaultMaxWindow
	}
	if c.MaxWindow < c.InitialWindow {
		c.MaxWindow = c.InitialWindow
	}
	if c.MaxReceiveBudget == 0 {
		c.MaxReceiveBudget = defaultRecvBudget
	}
	switch {
	case c.KeepaliveInterval == 0:
		c.KeepaliveInterval = defaultKeepalive
	case c.KeepaliveInterval < 0:
		c.KeepaliveInterval = 0
	}
	switch {
	case c.KeepaliveTimeout == 0:
		c.KeepaliveTimeout = 3 * c.KeepaliveInterval
	case c.KeepaliveTimeout < 0:
		c.KeepaliveTimeout = 0
	}

	if c.InitialWindow < frame.FloorInitialWindow {
		return c, fmt.Errorf("mux: InitialWindow %d below protocol floor %d", c.InitialWindow, frame.FloorInitialWindow)
	}
	if c.MaxFrameSize < frame.FloorMaxFrameSize || c.MaxFrameSize > frame.MaxLength {
		return c, fmt.Errorf("mux: MaxFrameSize %d outside [%d, %d]", c.MaxFrameSize, frame.FloorMaxFrameSize, frame.MaxLength)
	}
	if c.MaxIncomingStreams < 1 {
		return c, fmt.Errorf("mux: MaxIncomingStreams %d < 1", c.MaxIncomingStreams)
	}
	if c.AcceptBacklog < 1 {
		return c, fmt.Errorf("mux: AcceptBacklog %d < 1", c.AcceptBacklog)
	}
	// A batch must be able to hold at least one maximum-size frame, or a
	// full-size write could never be admitted.
	minBatch := int(c.MaxFrameSize) + frame.HeaderSize
	if c.MaxBatchBytes < minBatch {
		return c, fmt.Errorf("mux: MaxBatchBytes %d below one max frame (%d)", c.MaxBatchBytes, minBatch)
	}
	// Each stream must be able to admit one full frame, or a max-size write
	// could never make progress.
	if c.PerStreamBatchBytes < minBatch {
		c.PerStreamBatchBytes = minBatch
	}
	// A liveness timeout at or below the probe interval would declare a
	// healthy peer dead between probes — the incoherent-defaults bug that
	// has bitten several tunnel projects.
	if c.KeepaliveInterval > 0 && c.KeepaliveTimeout > 0 && c.KeepaliveTimeout <= c.KeepaliveInterval {
		return c, fmt.Errorf("mux: KeepaliveTimeout %v must exceed KeepaliveInterval %v",
			c.KeepaliveTimeout, c.KeepaliveInterval)
	}
	return c, nil
}
