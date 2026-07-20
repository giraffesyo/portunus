package mux

import (
	"sync/atomic"
	"time"
)

// Stats is a snapshot of one session's counters.
//
// Batching, flow control, and abuse limits are all adaptive, which means
// their behavior in production is an empirical question. A session that
// cannot report how large its batches are, how often senders stall, or what
// round-trip time the window is sized from cannot be tuned — only guessed at.
type Stats struct {
	// Traffic.
	FramesSent     uint64
	FramesReceived uint64
	BytesSent      uint64 // payload bytes, excluding framing
	BytesReceived  uint64

	// Group commit. FramesPerFlush is the ratio that says whether batching
	// is actually batching: near 1 means every frame paid for its own
	// syscall, which is the cost this library exists to avoid.
	Flushes        uint64
	FramesPerFlush float64
	MaxFlushFrames uint64
	MaxFlushBytes  uint64

	// Backpressure. AdmissionWaits counts writers that blocked because a
	// batch was full; WindowStalls counts senders that ran out of
	// flow-control credit — the signal autotuning grows on.
	AdmissionWaits uint64
	WindowStalls   uint64

	// Flow control, as currently estimated.
	RTT          time.Duration // min-filtered
	WindowTarget uint64        // bytes, the BDP estimate
	GrantedBytes uint64        // total receive credit outstanding

	// Streams.
	StreamsOpened   uint64 // locally initiated
	StreamsAccepted uint64 // peer initiated and accepted
	StreamsRefused  uint64 // peer initiated and rejected
	StreamsLive     uint64
	StreamsReaped   uint64 // closed by the linger timeout, not the peer

	// Abuse limiting. AbuseTrips is nonzero only for a session that was
	// closed for exceeding a limit.
	AbuseTrips uint64
}

// sessionStats holds the live counters.
//
// Fields are grouped by which goroutine writes them and padded apart, so the
// reader's counters and the flusher's counters never share a cache line.
// Cross-core ping-pong on a shared line costs more than the counters
// themselves, which is the usual way "cheap atomic stats" quietly become
// expensive.
type sessionStats struct {
	// Written by the reader goroutine.
	framesReceived atomic.Uint64
	bytesReceived  atomic.Uint64
	windowStalls   atomic.Uint64
	streamsRefused atomic.Uint64
	abuseTrips     atomic.Uint64
	streamsReaped  atomic.Uint64

	_ [64]byte // keep the reader's line clear of the flusher's

	// Written by whichever goroutine is flushing.
	framesSent     atomic.Uint64
	bytesSent      atomic.Uint64
	flushes        atomic.Uint64
	maxFlushFrames atomic.Uint64
	maxFlushBytes  atomic.Uint64
	admissionWaits atomic.Uint64

	_ [64]byte

	// Written by application goroutines opening and accepting streams.
	streamsOpened   atomic.Uint64
	streamsAccepted atomic.Uint64
}

// recordFlush notes one completed batch.
func (st *sessionStats) recordFlush(frames, bytes uint64) {
	st.flushes.Add(1)
	st.framesSent.Add(frames)
	st.bytesSent.Add(bytes)
	updateMax(&st.maxFlushFrames, frames)
	updateMax(&st.maxFlushBytes, bytes)
}

func updateMax(dst *atomic.Uint64, v uint64) {
	for {
		cur := dst.Load()
		if v <= cur || dst.CompareAndSwap(cur, v) {
			return
		}
	}
}

// Stats returns a snapshot. It is safe to call concurrently and takes no
// locks on the hot paths it reports.
func (s *NativeSession) Stats() Stats {
	out := Stats{
		FramesSent:      s.stats.framesSent.Load(),
		FramesReceived:  s.stats.framesReceived.Load(),
		BytesSent:       s.stats.bytesSent.Load(),
		BytesReceived:   s.stats.bytesReceived.Load(),
		Flushes:         s.stats.flushes.Load(),
		MaxFlushFrames:  s.stats.maxFlushFrames.Load(),
		MaxFlushBytes:   s.stats.maxFlushBytes.Load(),
		AdmissionWaits:  s.stats.admissionWaits.Load(),
		WindowStalls:    s.stats.windowStalls.Load(),
		RTT:             s.bdp.rtt(),
		WindowTarget:    s.bdp.target(),
		StreamsOpened:   s.stats.streamsOpened.Load(),
		StreamsAccepted: s.stats.streamsAccepted.Load(),
		StreamsRefused:  s.stats.streamsRefused.Load(),
		StreamsReaped:   s.stats.streamsReaped.Load(),
		AbuseTrips:      s.stats.abuseTrips.Load(),
	}
	if out.Flushes > 0 {
		out.FramesPerFlush = float64(out.FramesSent) / float64(out.Flushes)
	}

	s.mu.Lock()
	out.StreamsLive = uint64(len(s.streams))
	s.mu.Unlock()

	out.GrantedBytes = s.budget.outstanding()
	return out
}
