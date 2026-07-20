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
	if c.PerStreamBatchBytes == 0 {
		c.PerStreamBatchBytes = c.MaxBatchBytes / 4
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
	return c, nil
}
