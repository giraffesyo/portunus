package mux

import (
	"sync"
	"time"
)

// bdpEstimator sizes the receive window from the measured bandwidth-delay
// product, so a stream is not capped at window/RTT on a long path. A static
// 256KB window over a 200ms link tops out near 1.3 MB/s no matter how fast
// the link is; that ceiling is the single biggest real-world reason to build
// this library.
//
// The estimate is receiver-driven, following gRPC's design: send a PING,
// count the bytes that arrive before its ACK, and treat that count as the
// in-flight sample. Two disciplines keep it from running away:
//
//   - RTT is min-filtered. Measuring RTT through our own queues inflates it
//     under load, and "grow because RTT rose" is a bufferbloat spiral.
//   - Growth requires evidence that the window is what is limiting the
//     sender — a flow-control stall — and stops when delay says the path,
//     not the window, is the constraint.
//
// The PING is timestamped by the flusher as the batch reaches the carrier,
// never at enqueue: queue delay would otherwise be counted as network RTT.
type bdpEstimator struct {
	mu sync.Mutex

	probing    bool
	opaque     uint64
	sentAt     time.Time
	bytesAt    uint64 // recvTotal when the probe reached the carrier
	probeFrom  uint64 // recvTotal when the probe was enqueued (pacing only)
	stallsAt   uint64 // stall count when the probe reached the carrier
	reservedAt time.Time
	lastProbe  time.Time
	sampleWait bool // enqueued, not yet stamped by the flusher

	minRTT time.Duration
	bwMax  float64 // bytes per second, best ever observed
	bdp    uint64  // current estimate, bytes

	maxWindow uint64
}

// bdpProbeBytes is how much data must arrive before another probe is worth
// sending: probing more often than this measures queueing, not capacity.
const bdpProbeBytes = 64 << 10

func newBDPEstimator(initial, maxWindow uint64) *bdpEstimator {
	return &bdpEstimator{bdp: initial, maxWindow: maxWindow}
}

// shouldProbe reports whether enough data has arrived to justify a new
// sample, and reserves the probe slot if so.
func (b *bdpEstimator) shouldProbe(recvTotal uint64, opaque uint64, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	// A probe that never completes must not wedge the estimator. Anything
	// outstanding well past a plausible round trip is abandoned so a new
	// sample can start; without this, one lost probe disables autotuning
	// for the life of the session — observed as a window frozen at its
	// initial size on roughly half of otherwise identical runs.
	if b.probing && now.Sub(b.reservedAt) > b.probeTimeout() {
		b.probing = false
		b.sampleWait = false
	}
	// Probing is bounded on two axes, and both are needed.
	//
	// By bytes: the threshold scales with the current estimate, so a fixed
	// 64KB threshold does not mean a probe per frame at 64KB frames.
	//
	// By time: no more than one probe per round trip, which is the fastest
	// cadence that yields an independent sample anyway. Without the time
	// bound, a fast link turns the byte bound into thousands of probes per
	// second — enough for a correctly-implemented peer to classify us as a
	// ping flood and close the session, which is exactly what happened
	// under open-loop load before this existed.
	need := max(uint64(bdpProbeBytes), b.bdp)
	if b.probing || recvTotal < b.probeFrom+need {
		return false
	}
	if !b.lastProbe.IsZero() && now.Sub(b.lastProbe) < b.minProbeGap() {
		return false
	}
	b.lastProbe = now
	b.reservedAt = now
	b.probing = true
	b.sampleWait = true
	b.opaque = opaque
	b.probeFrom = recvTotal
	return true
}

// markSent stamps the in-flight probe at the moment its batch reaches the
// carrier. Called by the flusher, so queue delay is excluded from the RTT.
//
// The byte counter starts here too, not at enqueue: bytes already in flight
// before the probe hit the wire were not caused by it, and counting them
// inflates the measured bandwidth. One such sample sets a bandwidth peak no
// honest sample can match, and the plateau gate then blocks growth forever.
func (b *bdpEstimator) markSent(now time.Time, recvTotal, stalls uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sampleWait {
		b.sentAt = now
		b.bytesAt = recvTotal
		b.stallsAt = stalls
		b.sampleWait = false
	}
}

// onACK closes out a probe and returns the new window target, or 0 if the
// sample did not justify growth. stalls is the session's running count of
// flow-control stalls (senders that exhausted their granted credit).
func (b *bdpEstimator) onACK(opaque, recvTotal, stalls uint64, now time.Time) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.probing || opaque != b.opaque {
		return 0 // not our probe (a plain keepalive PING, or a straggler)
	}
	// Ours: release the slot on every path from here, so an unusable
	// sample costs one round trip rather than all future ones.
	b.probing = false
	if b.sampleWait {
		b.sampleWait = false
		return 0 // never reached the carrier: no honest timestamp
	}

	rtt := now.Sub(b.sentAt)
	if rtt <= 0 {
		return 0
	}
	if b.minRTT == 0 || rtt < b.minRTT {
		b.minRTT = rtt
	}

	sample := recvTotal - b.bytesAt
	b.probeFrom = recvTotal
	if bw := float64(sample) / rtt.Seconds(); bw > b.bwMax {
		b.bwMax = bw
	}

	// Grow on either sign that the window is the binding constraint:
	//
	//   - the peer ran out of credit during this probe (measured directly:
	//     the receiver knows when a sender has consumed what it was
	//     granted), or
	//   - a meaningful fraction of the window arrived within one round
	//     trip, so a larger window would plausibly carry more.
	//
	// Both are needed. A pure stall signal under-triggers: credit is
	// refreshed at half-window, so a sender in the resulting sawtooth
	// rarely hits exactly zero and growth halts around 60% utilization —
	// measured, with the window frozen while throughput sat well below
	// what the path allowed. A pure rate signal is circular in the other
	// direction: a window too small to fill the pipe depresses the very
	// rate the test reads, so the window never grows at all.
	//
	// The utilization threshold is deliberately low (40%) because that
	// sawtooth caps steady-state utilization well under 100%; requiring
	// two thirds stalls the climb at the first sawtooth.
	stalled := stalls != b.stallsAt
	utilized := sample*5 >= b.bdp*2
	if !stalled && !utilized {
		return 0
	}
	// Unless the path is visibly congested — then the window is not what is
	// limiting throughput, and growing it only deepens queues.
	if b.minRTT > 0 && rtt > 2*b.minRTT {
		return 0
	}
	// Cap growth at a doubling per RTT so the window climbs geometrically
	// rather than jumping to one optimistic sample.
	target := min(b.bdp*2, b.maxWindow)
	if target <= b.bdp {
		return 0
	}
	b.bdp = target
	return target
}

// minProbeGap is the shortest interval between probes: one round trip, since
// samples taken closer together are not independent, with a floor for links
// whose round trip is too short to pace anything usefully.
func (b *bdpEstimator) minProbeGap() time.Duration {
	if b.minRTT > 0 {
		return max(b.minRTT, time.Millisecond)
	}
	return time.Millisecond
}

// probeTimeout bounds how long a probe may stay outstanding: several round
// trips once one is known, and a fixed few seconds before that.
func (b *bdpEstimator) probeTimeout() time.Duration {
	if b.minRTT > 0 {
		return max(8*b.minRTT, 250*time.Millisecond)
	}
	return 5 * time.Second
}

// rtt returns the min-filtered round-trip estimate (0 before any sample).
func (b *bdpEstimator) rtt() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.minRTT
}

// target returns the current window target in bytes.
func (b *bdpEstimator) target() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bdp
}

// budget bounds the total receive credit the session may have outstanding,
// so autotuning cannot turn "many briefly-fast streams" into an OOM. It only
// ever declines to grow a window — granted credit is never revoked — so it
// adds no deadlock surface and needs no wire change.
type budget struct {
	mu        sync.Mutex
	granted   uint64
	max       uint64
	streamCap uint64
}

func newBudget(max, streamCap uint64) *budget {
	return &budget{max: max, streamCap: streamCap}
}

// reserve accounts a new stream's initial window. It is unconditional: the
// initial window was already advertised to the peer in SETTINGS, so it is
// owed regardless of budget. The budget's job is to stop autotuned growth on
// top of that, which is where unbounded memory would actually come from.
func (b *budget) reserve(window uint64) {
	b.mu.Lock()
	b.granted += window
	b.mu.Unlock()
}

// grow asks to raise one stream's window from have to want, returning the
// window actually permitted.
func (b *budget) grow(have, want uint64) uint64 {
	if want <= have {
		return have
	}
	if want > b.streamCap {
		want = b.streamCap
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.granted+(want-have) > b.max {
		// Give whatever headroom is left rather than nothing.
		if b.granted >= b.max {
			return have
		}
		want = have + (b.max - b.granted)
	}
	if want <= have {
		return have
	}
	b.granted += want - have
	return want
}

// release returns a finished stream's window to the session budget.
func (b *budget) release(window uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if window > b.granted {
		b.granted = 0
		return
	}
	b.granted -= window
}
