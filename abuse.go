package mux

import (
	"sync"
	"time"
)

// Abuse limits.
//
// Every frame type that costs the receiver work while consuming little or no
// flow-control credit is a denial-of-service primitive, and each of the ones
// policed here has a CVE behind it: PING floods (CVE-2019-9512, and the
// rust-yamux unbounded-pong OOM CVE-2024-32984), empty-frame floods
// (CVE-2019-9515/9516/9518), and stream reset churn (HTTP/2 Rapid Reset,
// CVE-2023-39325, and its MadeYouReset sequel CVE-2025-8671).
//
// Byte-based budgets never catch these: the frames are small or empty by
// construction. They need their own counters.
//
// The limits are deliberately loose. A conformant peer sends one PING per
// keepalive interval and resets streams only when its application cancels
// work, so it will not come close; an attacker generating frames at line rate
// trips them immediately. Exceeding one is not a protocol error — it is a
// judgment about behavior — so the session closes with CodeCalm, telling the
// peer to back off rather than accusing it of speaking the protocol wrong.

// rateLimiter is a token bucket. Tokens refill continuously up to burst, so
// a peer may be bursty without being abusive but cannot sustain a flood.
type rateLimiter struct {
	mu     sync.Mutex
	tokens float64
	burst  float64
	perSec float64
	last   time.Time
}

func newRateLimiter(perSec, burst float64, now time.Time) *rateLimiter {
	return &rateLimiter{tokens: burst, burst: burst, perSec: perSec, last: now}
}

// allow consumes one token, reporting whether the caller stayed within the
// limit. A limiter with perSec <= 0 is disabled and always allows.
func (r *rateLimiter) allow(now time.Time) bool {
	if r == nil || r.perSec <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if elapsed := now.Sub(r.last); elapsed > 0 {
		r.tokens = min(r.burst, r.tokens+elapsed.Seconds()*r.perSec)
		r.last = now
	}
	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}

// Default limits, chosen so a conformant peer never approaches them.
const (
	// PINGs serve two purposes — keepalive and BDP probing — and probing
	// legitimately runs at up to one per round trip, which on a fast local
	// link is thousands per second. The limit is therefore set well above
	// any honest cadence rather than derived from the keepalive interval,
	// which describes only one of the two uses. A flood arrives at line
	// rate, orders of magnitude above this.
	defaultPingPerSec = 2000
	defaultPingBurst  = 500

	// Stream resets track legitimate application cancellation, which is
	// bursty (a client navigating away cancels many at once) but not
	// sustained at hundreds per second.
	defaultResetPerSec = 200
	defaultResetBurst  = 500

	// No-op frames — empty DATA that neither opens, closes, nor carries
	// anything — have no legitimate use at volume.
	defaultNoopPerSec = 100
	defaultNoopBurst  = 200
)

// initAbuseLimits builds the per-session limiters.
func (s *NativeSession) initAbuseLimits(now time.Time) {
	s.pingLimit = newRateLimiter(defaultPingPerSec, defaultPingBurst, now)
	s.resetLimit = newRateLimiter(defaultResetPerSec, defaultResetBurst, now)
	s.noopLimit = newRateLimiter(defaultNoopPerSec, defaultNoopBurst, now)
}

// pingMinInterval is the minimum inter-PING gap we advertise, in
// milliseconds. A peer that honors it will never trip the ping limiter.
func (s *NativeSession) pingMinInterval() uint64 {
	if s.cfg.KeepaliveInterval <= 0 {
		return uint64(defaultKeepalive / time.Millisecond)
	}
	return uint64(s.cfg.KeepaliveInterval / time.Millisecond / 4)
}

// tooMuch ends the session with CodeCalm after an abuse limit is exceeded.
// Called from the reader goroutine, so it must not block: sendGoAway stages
// asynchronously.
func (s *NativeSession) tooMuch(what string) error {
	s.stats.abuseTrips.Add(1)
	s.sendGoAway(CodeCalm, what)
	s.fatal(&SessionError{Code: CodeCalm, Reason: what})
	return errSessionTerminated
}
