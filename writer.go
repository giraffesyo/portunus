package portunus

import (
	"context"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/giraffesyo/portunus/internal/frame"
)

// The send path is group commit: writers append frames to a shared pending
// batch under one mutex, and whichever writer finds no flush in flight
// becomes the flusher — it swaps the batch out, releases the mutex, and
// issues a single writev for the whole batch. Under load one syscall carries
// many frames across many streams; idle, the flush happens inline in the
// calling goroutine, so latency equals a direct write.
//
// Completion semantics are split (DESIGN.md "Send path"):
//
//   - Small payloads are copied into staging at admission, so the io.Writer
//     contract is already satisfied and the writer returns as soon as its
//     bytes are queued. Errors latch and surface on a later call.
//   - Large payloads are referenced directly in the iovec (zero copy), so
//     that writer must park until the flush containing its bytes resolves —
//     the kernel is reading the caller's memory until then.
//
// A stream with an active write deadline always takes the copy path: a
// deadline cannot be honored once the caller's buffer is pinned in an iovec.
const (
	// zeroCopyThreshold is the payload size at or above which a write is
	// referenced rather than copied. Below it the memcpy is cheaper than
	// the goroutine park it would otherwise cost.
	zeroCopyThreshold = 4 << 10

	// tlsRecordSize is crypto/tls's maximum plaintext record. Coalesced
	// writes are chunked to it so no copy exceeds what one record carries.
	tlsRecordSize = 16 << 10

	// smallWriteReserve is batch space kept clear of bulk writes so small
	// messages always have somewhere to land. It bounds how long a small
	// write can be held out of the batch currently being assembled.
	smallWriteReserve = 64 << 10

	// burstFast and burstSlow are the flush durations that grow and shrink
	// the bulk batch. Between them it holds. A 64KB frame takes about 5ms to
	// leave on a 100mbit link and 50µs on 10GbE, so links where a frame is
	// already a noticeable wait stay at one bulk frame per flush and only
	// carriers that absorb a batch in well under a millisecond get one.
	burstFast = 500 * time.Microsecond
	burstSlow = 2 * time.Millisecond
)

// chunk is one entry in a pending batch: either a reference to caller memory
// (ref non-nil) or a range of the staging buffer, which may be reallocated
// as it grows and so cannot be sliced until flush time.
type chunk struct {
	ref []byte
	off int32
	n   int32
}

// shard is one of the staging areas small DATA frames are copied into
// without the writer mutex. See appendSmall.
type shard struct {
	mu      sync.Mutex
	buf     []byte // whole frames, header and payload, in append order
	frames  uint64
	payload uint64

	// Flusher-owned: swapped with buf so neither side allocates in steady
	// state.
	fbuf []byte

	_ [64]byte // keep neighboring shards' locks off one cache line
}

// maxShards bounds the staging shards. Past this, a batch's iovec grows and
// the flusher's swap lengthens for contention that is already gone.
const maxShards = 16

// shardCount is the number of staging shards a session gets: enough for one
// per core up to maxShards, rounded to a power of two so a stream's shard is
// a mask of its ID.
func shardCount() uint32 {
	n := uint32(1)
	for int(n) < runtime.GOMAXPROCS(0) && n < maxShards {
		n <<= 1
	}
	return n
}

type writer struct {
	s   *NativeSession
	tcp *net.TCPConn // non-nil when net.Buffers becomes a real writev

	mu sync.Mutex

	// Pending batch. Control frames are staged separately and placed first
	// in the iovec so a window update is never trapped behind bulk data.
	// The priority lane sits just after control and ahead of bulk staging,
	// carrying copied DATA frames from streams marked latency-sensitive.
	ctl     []byte
	prio    []byte
	stage   []byte
	chunks  []chunk
	pending int

	// shards stage small DATA frames outside mu, and shardBytes counts what
	// they hold. Everything that asks "is there anything to flush" must add
	// it to pending. shardCap is how full one shard may get; past it, or
	// past a full batch overall, writers take the locked path, where
	// backpressure is applied.
	shards     []shard
	shardMask  uint32
	shardCap   int
	shardBytes atomic.Int64

	// dataPending is the part of pending that is DATA frames, framing
	// included. Compared with one stream's share of the batch it says
	// whether that stream has the batch's data to itself; control frames
	// are left out because a window update or a PING reply in the batch is
	// not another stream competing for it.
	dataPending int

	// frames and payload are counted as the batch is assembled, because a
	// zero-copy frame contributes two chunks and the iovec carries framing
	// bytes: deriving either from the assembled iovec double-counts frames
	// and reports headers as payload.
	frames  uint64
	payload uint64

	// seq is the sequence number the pending batch will carry; doneSeq is
	// the count of batches that have completed, so a parked zero-copy
	// writer waits for doneSeq to exceed the seq its bytes went into.
	//
	// A count rather than "the last completed seq": with both starting at
	// zero the two are indistinguishable for the very first batch, and a
	// writer that joined it would evaluate its wait condition as already
	// satisfied and return while the kernel was still reading its buffer.
	seq     uint64
	doneSeq uint64
	err     error // sticky: any carrier error is session-fatal

	// seqA, pendingA and failed mirror seq, pending and err != nil for the
	// fast path, which reads them without mu. All are written only under mu.
	seqA     atomic.Uint64
	pendingA atomic.Int64
	failed   atomic.Bool

	// batchDone is closed when the pending batch has been written, and
	// fbatchDone is the same for the batch in flight. Each exists only if a
	// zero-copy writer is parked on that batch, so the uncontended path — one
	// writer, inline flush, nobody parked — allocates nothing. A channel
	// rather than a sync.Cond so a dying session can be selected against.
	batchDone  chan struct{}
	fbatchDone chan struct{}

	// admitQ holds the streams whose writers are parked in admission, in
	// arrival order; admitting counts writers anywhere in the admission
	// wait, queued or woken and not yet staged. See admitLocked.
	admitQ    []*NativeStream
	admitting int

	// burst is how many frames of bulk a batch may currently hold, sized
	// from flush duration; see bulkLimitLocked. takeHint is the most a write
	// can currently expect to have taken in one call, in bytes, published
	// for Write to size its credit reservation without the lock.
	burst    int
	takeHint atomic.Int64

	// flushing is written only under mu. It is atomic so the fast path can
	// tell, without mu, that a flusher is running and will pick its bytes up.
	flushing atomic.Bool

	// pingStamp marks a batch as carrying the BDP probe, so the flusher can
	// timestamp it as it reaches the carrier rather than at enqueue.
	pingStamp  bool
	fpingStamp bool
	fframes    uint64
	fpayload   uint64

	// Flusher-owned scratch, swapped with the pending buffers so neither
	// side allocates in steady state.
	fctl     []byte
	fprio    []byte
	fstage   []byte
	fchunks  []chunk
	fshards  uint32 // bit i set: shard i's fbuf is part of the batch in flight
	iov      net.Buffers
	iovOut   net.Buffers // the copy handed to WriteTo; see writeOut
	coalesce []byte
}

func newWriter(s *NativeSession) *writer {
	w := &writer{
		s:       s,
		burst:   1,
		ctl:     make([]byte, 0, 4<<10),
		prio:    make([]byte, 0, 4<<10),
		stage:   make([]byte, 0, 64<<10),
		chunks:  make([]chunk, 0, 64),
		fctl:    make([]byte, 0, 4<<10),
		fprio:   make([]byte, 0, 4<<10),
		fstage:  make([]byte, 0, 64<<10),
		fchunks: make([]chunk, 0, 64),
	}
	n := shardCount()
	w.shards = make([]shard, n)
	w.shardMask = n - 1
	// One shard may hold what one stream may put in a batch: it serves a
	// few streams, and should not take more of the batch than one could.
	w.shardCap = s.cfg.PerStreamBatchBytes
	// net.Buffers only becomes writev on an exact *net.TCPConn: the
	// enabling interface inside net is unexported, so no wrapper can
	// implement it. Anything else is coalesced into one contiguous write.
	if tcp, ok := s.conn.(*net.TCPConn); ok {
		w.tcp = tcp
	}
	// One frame until the first flush has measured the carrier.
	w.takeHint.Store(int64(frame.FloorMaxFrameSize))
	return w
}

// appendControl queues a control frame and flushes inline if no flush is in
// flight. Callers must be application goroutines — the session reader uses
// appendControlAsync, since a reader that blocks on the send path is one
// half of the classic two-sided gridlock.
func (w *writer) appendControl(h frame.Header, payload []byte) {
	if w.stageControl(h, payload) {
		w.flushLoop()
	}
}

// appendControlAsync queues a control frame without ever blocking the
// caller: if a flush is needed, a goroutine performs it. Used by the reader
// goroutine for PING ACKs, refusal resets, and protocol GOAWAYs.
func (w *writer) appendControlAsync(h frame.Header, payload []byte) {
	if w.stageControl(h, payload) {
		go w.flushLoop()
	}
}

// appendProbe queues the BDP probe PING and marks its batch for flush-time
// stamping, so the measured RTT excludes our own queue delay.
//
// The flag is set in the same critical section as the append. Setting it
// separately lets a flush in between carry the flag away on a batch that
// does not contain the probe: the probe then reaches the wire unstamped and
// its sample is discarded, which stalls autotuning at the initial window.
func (w *writer) appendProbe(h frame.Header, payload []byte) {
	if w.stageControlStamped(h, payload) {
		go w.flushLoop()
	}
}

// stageControl appends the frame and reports whether the caller took
// ownership of flushing. Control frames never wait on the batch cap: they
// are small, bounded, and some originate on the reader goroutine.
func (w *writer) stageControl(h frame.Header, payload []byte) bool {
	return w.stage2(h, payload, false)
}

// stageControlStamped is stageControl plus marking the batch as carrying the
// BDP probe, both under one lock.
func (w *writer) stageControlStamped(h frame.Header, payload []byte) bool {
	return w.stage2(h, payload, true)
}

func (w *writer) stage2(h frame.Header, payload []byte, stamp bool) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return false
	}
	if stamp {
		w.pingStamp = true
	}
	h.Length = uint32(len(payload))
	var hdr [frame.HeaderSize]byte
	h.Encode(hdr[:])
	w.ctl = append(w.ctl, hdr[:]...)
	w.ctl = append(w.ctl, payload...)
	w.frames++
	w.pending += frame.HeaderSize + len(payload)
	w.pendingA.Store(int64(w.pending))
	return w.claimFlushLocked()
}

// appendStreamControl queues a control frame belonging to one stream: a
// reset, a stop-sending, or a window update.
//
// Control frames normally lead the iovec so a window update is never trapped
// behind queued bulk data. That ordering is safe across streams but not
// within one: a stream announces itself with SYN riding its first DATA frame,
// so a reset issued before that batch flushes would reach the peer first, as
// a frame for a stream the peer has never heard of. The peer is then obliged
// to treat it as a protocol error and kill the session — which is what
// happened, roughly one run in ten, whenever a stream was opened and
// cancelled in quick succession. That pattern is not exotic; it is what a
// proxy does every time a client hangs up immediately.
//
// Ordering is preserved by putting the SYN-bearing frame in the control area
// as well (see appendData). Entries there are written in append order and the
// whole area precedes the data chunks, so a reset staged after a SYN can
// never be written before it, whether they share a batch or not.
// async must be set by the session reader, which may never block on the send
// path: a reader parked in a carrier write stops draining its socket, and two
// peers doing that at once deadlock permanently.
func (w *writer) appendStreamControl(st *NativeStream, h frame.Header, payload []byte, async bool) {
	w.mu.Lock()
	if w.err != nil || !st.synSent {
		// Not announced yet: the peer has no state for this stream, so
		// there is nothing to tell it about.
		w.mu.Unlock()
		return
	}
	h.Length = uint32(len(payload))
	var hdr [frame.HeaderSize]byte
	h.Encode(hdr[:])

	w.ctl = append(w.ctl, hdr[:]...)
	w.ctl = append(w.ctl, payload...)
	w.frames++
	w.pending += frame.HeaderSize + len(payload)
	w.pendingA.Store(int64(w.pending))
	flush := w.claimFlushLocked()
	w.mu.Unlock()

	if flush {
		if async {
			go w.flushLoop()
		} else {
			w.flushLoop()
		}
	}
}

// appendData queues payload for st as one or more DATA frames and reports how
// many bytes it took: always at least one frame's worth, and more only when
// burstFramesLocked allows a write spanning several frames to stage them
// together. The caller sends the remainder with another call. Small payloads
// return once staged; large ones park until their flush resolves.
//
// SYN is attached if this is the stream's first frame on the wire, decided
// under w.mu together with the append, so concurrent senders can neither
// duplicate the SYN nor let a later frame overtake it.
//
// more lists whole buffers that may follow payload as frames of their own in
// the same batch, when payload is a single frame taking the referenced path;
// how many were taken shows in the count returned. hold stages a copied
// frame without claiming the flush: the caller undertakes to flush, with
// another frame or with kick.
func (w *writer) appendData(st *NativeStream, payload []byte, flags frame.Flags, copyOnly bool, deadline <-chan struct{}, more [][]byte, hold bool) (int, error) {
	if len(payload) < zeroCopyThreshold && w.appendSmall(st, payload, flags, hold) {
		return len(payload), nil
	}

	maxf := int(w.s.peerMaxFrame.Load())
	first := min(len(payload), maxf)

	w.mu.Lock()
	// Admission is decided on the first frame alone, so a long write waits
	// for exactly what a single frame would and is never held out for
	// wanting more.
	woken, err := w.admitLocked(st, frame.HeaderSize+first, deadline)
	if err != nil {
		return 0, err
	}

	if st.sendAborted.Load() {
		// Abandoned while this write was in flight. Staging now would
		// announce a stream the peer can never be told about.
		if woken {
			w.passAdmitLocked()
		}
		w.mu.Unlock()
		return 0, ErrStreamClosed
	}

	syn := !st.synSent
	if syn {
		flags |= frame.FlagSYN
		st.synSent = true
		st.announced.Store(true)
	}
	var hdr [frame.HeaderSize]byte
	frame.Header{
		Type:     frame.TypeData,
		Flags:    flags,
		StreamID: st.id,
		Length:   uint32(first),
	}.Encode(hdr[:])

	// The frame announcing a stream goes in the control area rather than
	// with the data. Control leads the iovec, so a reset or window update
	// staged later would otherwise be written before the SYN it refers to,
	// and the peer is obliged to treat a frame for a stream it has never
	// heard of as a protocol error. Keeping both in the control area, which
	// preserves append order, makes that reordering impossible by
	// construction. It costs one copy on the first frame of each stream.
	//
	// A priority stream's later frames go in the priority lane, which the
	// flusher writes after control and ahead of bulk. Like control it is
	// copied, so the writer returns as soon as its bytes are staged rather
	// than parking on a zero-copy reference; a small latency-sensitive frame
	// was going to be copied anyway. SYN still rides control, which leads
	// the priority lane, so the announcement cannot be overtaken by the
	// stream's own later frames. In-stream order holds because every one of
	// this stream's non-SYN frames takes this same lane, in append order.
	prio := !syn && st.priority.Load()
	zeroCopy := !syn && !prio && !copyOnly && first >= zeroCopyThreshold

	// A small frame that reached the locked path — its shard was full, the
	// batch was, or no flusher was running when it looked — still goes to
	// its shard if a flusher is running now and the stream has nothing in
	// this batch's main lane. Staging it in the main lane instead would
	// commit the stream to the locked path for the rest of the batch, and
	// under contention the wait for this mutex outlasts a batch, so the
	// next write would arrive to find itself committed again: measured,
	// half of all small writes were stuck here that way.
	inBatch := st.batchSeq.Load() == w.seq+1
	if !syn && !prio && !inBatch && first < zeroCopyThreshold && w.flushing.Load() &&
		w.shardAppend(st, &hdr, payload[:first]) {
		// The flusher re-reads shardBytes under mu before it may exit, and
		// we hold mu, so these bytes cannot be stranded.
		w.mu.Unlock()
		return first, nil
	}
	if !inBatch {
		st.batchSeq.Store(w.seq + 1) // batchSeq 0 means "no batch", so offset by one
		st.batchBytes = 0
	}

	take, frames := first, 1
	switch {
	case syn:
		w.ctl = append(w.ctl, hdr[:]...)
		w.ctl = append(w.ctl, payload[:first]...)
	case prio:
		w.prio = append(w.prio, hdr[:]...)
		w.prio = append(w.prio, payload[:first]...)
	case !zeroCopy:
		off := int32(len(w.stage))
		w.stage = append(w.stage, hdr[:]...)
		w.stage = append(w.stage, payload[:first]...)
		w.chunks = append(w.chunks, chunk{off: off, n: int32(len(w.stage)) - off})
	case len(more) > 0 && flags == 0:
		// A frame followed by other buffers, each a frame of its own.
		frames = w.burstFramesLocked(st, (1+len(more))*maxf, maxf)
		off := int32(len(w.stage))
		w.stage = append(w.stage, hdr[:]...)
		w.chunks = append(w.chunks, chunk{off: off, n: frame.HeaderSize}, chunk{ref: payload[:first]})
		for _, b := range more[:frames-1] {
			frame.Header{
				Type:     frame.TypeData,
				StreamID: st.id,
				Length:   uint32(len(b)),
			}.Encode(hdr[:])
			off := int32(len(w.stage))
			w.stage = append(w.stage, hdr[:]...)
			w.chunks = append(w.chunks, chunk{off: off, n: frame.HeaderSize}, chunk{ref: b})
			take += len(b)
		}
	default:
		// Flags describe the write as a whole, so a flagged payload stays
		// one frame; Write never passes any.
		if len(payload) > maxf && flags == 0 {
			frames = w.burstFramesLocked(st, len(payload), maxf)
			take = min(len(payload), frames*maxf)
		}
		for sent := 0; sent < take; sent += maxf {
			part := payload[sent:min(sent+maxf, take)]
			if sent > 0 {
				frame.Header{
					Type:     frame.TypeData,
					StreamID: st.id,
					Length:   uint32(len(part)),
				}.Encode(hdr[:])
			}
			off := int32(len(w.stage))
			w.stage = append(w.stage, hdr[:]...)
			w.chunks = append(w.chunks, chunk{off: off, n: frame.HeaderSize}, chunk{ref: part})
		}
	}
	total := take + frames*frame.HeaderSize
	st.batchBytes += total
	w.frames += uint64(frames)
	w.payload += uint64(take)
	w.pending += total
	w.pendingA.Store(int64(w.pending))
	w.dataPending += total

	mySeq := w.seq
	flush := !hold && w.claimFlushLocked()
	// A referenced payload that someone else will flush is waited for on the
	// batch's own channel, so the writer wakes when its batch resolves and
	// not on every flush before it.
	var done chan struct{}
	if zeroCopy && !flush {
		if w.batchDone == nil {
			w.batchDone = make(chan struct{})
		}
		done = w.batchDone
	}
	w.mu.Unlock()

	if flush {
		w.flushLoop()
	}
	if !zeroCopy {
		// Bytes are session-owned: the caller may reuse its buffer, and
		// any error surfaces on a later call.
		return take, nil
	}

	// Park until the flush carrying this payload resolves. There is no
	// deadline case here: a stream with an active write deadline took the
	// copy path above.
	if done != nil {
		select {
		case <-done:
		case <-w.s.done:
		}
	}
	w.mu.Lock()
	if w.doneSeq <= mySeq && w.err == nil {
		// Only a dying session leaves a batch unresolved: either it was
		// seen above, or our own flushLoop returned early on it.
		if w.err = w.s.closedErr(); w.err == nil {
			w.err = ErrSessionClosed
		}
		w.failed.Store(true)
	}
	err = w.err
	w.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return take, nil
}

// appendSmall is the fast path for a small DATA frame: it copies the frame
// into the stream's staging shard under that shard's lock alone, and reports
// false if the frame has to take the locked path instead.
//
// With many streams sending small messages the writer mutex was the whole
// cost: a profile of 64 streams on 18 cores put half of all CPU in
// appendData, nearly all of it acquiring and handing off that one lock. A
// small frame needs almost nothing the mutex protects. Its bytes are copied,
// so it waits on no flush; once the stream is announced there is no SYN to
// decide; and admission only matters when the batch is full. So it is staged
// in one of a handful of shards, each with its own lock, which the flusher
// gathers when it swaps the batch out. Writers on different shards no longer
// meet at all.
//
// What keeps that correct:
//
//   - Order within a stream. A stream always uses the same shard, and shards
//     are written after control and priority and before the main data lane.
//     So a small frame may not go to a shard while the stream has frames in
//     the main lane of the same pending batch, or it would overtake them;
//     that is the batchSeq test. The reverse — main lane after shard — is
//     already in order. The flusher gathers the shards while holding mu, in
//     the same critical section that swaps the main lanes and before it
//     publishes the new batch number, so a stream's earlier frame can never
//     be left behind for a later batch than its next, and a stream held to
//     the main lane stays held until its shard has been emptied.
//   - No stranded bytes. A frame staged here has no flusher of its own. The
//     writer publishes its bytes and then looks for a flusher; the flusher
//     clears its flag and then looks for bytes (see flushLoop). Whichever
//     runs second sees the other.
//   - Backpressure. A shard holds a bounded amount and the batch as a whole
//     is bounded; past either, writers take the locked path, where a full
//     batch makes them wait.
func (w *writer) appendSmall(st *NativeStream, payload []byte, flags frame.Flags, hold bool) bool {
	// With no flush in flight this writer would become the flusher anyway,
	// and the locked path stages and claims in one acquisition; the detour
	// through a shard would only add locks to the idle round trip.
	if !w.flushing.Load() {
		return false
	}
	if !st.announced.Load() || st.priority.Load() || st.sendAborted.Load() || w.failed.Load() {
		return false
	}
	if st.batchSeq.Load() == w.seqA.Load()+1 {
		return false
	}
	// The batch bound, read without the lock: it can be overshot by the
	// few frames that race past it, never by more.
	if int(w.pendingA.Load()+w.shardBytes.Load())+frame.HeaderSize+len(payload) > w.s.cfg.MaxBatchBytes {
		return false
	}
	var hdr [frame.HeaderSize]byte
	frame.Header{
		Type:     frame.TypeData,
		Flags:    flags,
		StreamID: st.id,
		Length:   uint32(len(payload)),
	}.Encode(hdr[:])
	if !w.shardAppend(st, &hdr, payload) {
		return false
	}

	if !hold && !w.flushing.Load() {
		w.mu.Lock()
		flush := w.claimFlushLocked()
		w.mu.Unlock()
		if flush {
			w.flushLoop()
		}
	}
	return true
}

// shardAppend copies one encoded frame into st's shard, reporting false if
// the shard is full. It takes only the shard's lock; callers may or may not
// hold mu.
func (w *writer) shardAppend(st *NativeStream, hdr *[frame.HeaderSize]byte, payload []byte) bool {
	total := frame.HeaderSize + len(payload)
	sh := &w.shards[st.shard]
	sh.mu.Lock()
	if len(sh.buf)+total > w.shardCap {
		sh.mu.Unlock()
		return false
	}
	sh.buf = append(sh.buf, hdr[:]...)
	sh.buf = append(sh.buf, payload...)
	sh.frames++
	sh.payload += uint64(len(payload))
	// Published inside the shard lock, where the flusher also takes it
	// back, so the count never disagrees with what the shards hold.
	w.shardBytes.Add(int64(total))
	sh.mu.Unlock()
	return true
}

// gatherShardsLocked moves what the shards hold into the flusher's buffers
// and returns the byte count. Called with mu held, in the same critical
// section that swaps the main lanes; appendSmall's ordering argument depends
// on that.
func (w *writer) gatherShardsLocked() int {
	w.fshards = 0
	if w.shardBytes.Load() == 0 {
		// The common case on a bulk-only session: not one shard lock taken.
		return 0
	}
	n := 0
	for i := range w.shards {
		sh := &w.shards[i]
		sh.mu.Lock()
		if len(sh.buf) > 0 {
			n += len(sh.buf)
			w.fshards |= 1 << i
			sh.fbuf, sh.buf = sh.buf, sh.fbuf[:0]
			w.frames += sh.frames
			w.payload += sh.payload
			sh.frames, sh.payload = 0, 0
		}
		sh.mu.Unlock()
	}
	w.shardBytes.Add(int64(-n))
	return n
}

// admitLocked waits until the pending batch can take total more bytes from
// st. It is called with w.mu held and returns with it held, except on error,
// when it has been released. woken reports that this caller was handed room
// by wakeAdmitLocked, so a caller that then stages nothing must pass it on.
//
// Admission backpressure bounds the batch so flush duration, staging
// occupancy, and every co-batched writer's latency floor stay bounded, and
// bounds each stream's share so a bulk transfer cannot monopolize consecutive
// batches. Waiting here is pre-admission, so write deadlines legally apply.
//
// Waiters queue in arrival order and are woken when a batch is swapped out
// for flushing — the moment room appears — and only as many as the new batch
// can hold. Waking everyone when a flush completed had both parts wrong:
// room had been free for the whole writev with the waiters still asleep, so
// the flusher's next swap raced them and took a nearly empty batch; and all
// of them woke to contend for room a few could use, measured at 3.5 parks
// per frame with 64 bulk writers.
func (w *writer) admitLocked(st *NativeStream, total int, deadline <-chan struct{}) (woken bool, err error) {
	waiting := false
	for w.err == nil && w.admissionBlockedLocked(st, total) {
		if w.claimFlushLocked() {
			w.mu.Unlock()
			w.flushLoop()
			w.mu.Lock()
			continue
		}
		if !waiting {
			waiting = true
			w.admitting++
		}
		w.s.stats.admissionWaits.Add(1)
		if st.admitCh == nil {
			st.admitCh = make(chan struct{}, 1)
		}
		st.admitNeed = total
		// Queued before unlocking, so the flusher's next swap cannot miss
		// it. A waiter that was woken and then lost its room to a newcomer
		// keeps its place at the head rather than starting over.
		w.enqueueAdmitLocked(st, woken)
		w.mu.Unlock()
		var werr error
		select {
		case <-st.admitCh:
		case <-deadline:
			werr = os.ErrDeadlineExceeded
		case <-w.s.done:
			werr = w.s.closedErr()
		}
		w.mu.Lock()
		if werr != nil {
			if !w.dequeueAdmitLocked(st) {
				// Woken and gave up in the same instant: the room it was
				// handed must not be lost with it.
				select {
				case <-st.admitCh:
				default:
				}
				w.passAdmitLocked()
			}
			w.admitting--
			w.mu.Unlock()
			return false, werr
		}
		woken = true
	}
	if waiting {
		w.admitting--
	}
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return false, err
	}
	return woken, nil
}

// enqueueAdmitLocked parks st in the admission queue, at the head if it is
// returning after a wake that yielded nothing.
func (w *writer) enqueueAdmitLocked(st *NativeStream, front bool) {
	st.admitQueued = true
	if !front {
		w.admitQ = append(w.admitQ, st)
		return
	}
	w.admitQ = append(w.admitQ, nil)
	copy(w.admitQ[1:], w.admitQ)
	w.admitQ[0] = st
}

// dequeueAdmitLocked removes st from the admission queue, reporting false if
// it was no longer there — that is, if it had already been woken.
func (w *writer) dequeueAdmitLocked(st *NativeStream) bool {
	if !st.admitQueued {
		return false
	}
	st.admitQueued = false
	for i, q := range w.admitQ {
		if q == st {
			copy(w.admitQ[i:], w.admitQ[i+1:])
			w.admitQ[len(w.admitQ)-1] = nil
			w.admitQ = w.admitQ[:len(w.admitQ)-1]
			break
		}
	}
	return true
}

// wakeAdmitLocked releases waiters from the head of the queue until their
// first frames would fill a batch. Called when the pending batch has just
// been emptied. At least one is always woken, since an empty batch admits
// anything.
func (w *writer) wakeAdmitLocked() {
	room := w.bulkLimitLocked()
	n := 0
	for n < len(w.admitQ) {
		st := w.admitQ[n]
		if n > 0 && st.admitNeed > room {
			break
		}
		room -= st.admitNeed
		st.admitQueued = false
		// Never blocks: the channel holds one token and a stream has one
		// writer in admission at a time.
		select {
		case st.admitCh <- struct{}{}:
		default:
		}
		n++
	}
	if n == 0 {
		return
	}
	k := copy(w.admitQ, w.admitQ[n:])
	clear(w.admitQ[k:])
	w.admitQ = w.admitQ[:k]
}

// passAdmitLocked hands on a wake its holder could not use. Waiters are only
// woken by a swap, and a swap only happens with something staged, so a woken
// writer that leaves without staging would otherwise strand everyone queued
// behind it with nothing left to trigger the next wake.
func (w *writer) passAdmitLocked() {
	if w.pending == 0 && w.shardBytes.Load() == 0 {
		w.wakeAdmitLocked()
	}
}

// bulkCap is the most bulk data a batch may ever hold: the configured batch
// size less the room kept for small writes.
//
// Bulk writes must leave headroom that only small writes may use, so a
// latency-sensitive message can always join the batch being assembled
// instead of queueing behind a batch's worth of bulk data. Without the
// reservation, small writers lose the admission race to a handful of
// saturating bulk writers until the runtime's mutex starvation mode forces a
// handoff — which showed up as a millisecond of added tail latency, an order
// of magnitude worse than the batch itself.
//
// It never reserves so much that bulk cannot batch at all: a small
// MaxBatchBytes (permitted down to one max frame) minus a fixed reserve goes
// negative, which would block every bulk write behind a full drain and
// reduce group commit to one frame per syscall.
func (w *writer) bulkCap() int {
	return max(w.s.cfg.MaxBatchBytes-smallWriteReserve, int(w.s.cfg.MaxFrameSize)+frame.HeaderSize)
}

// bulkLimitLocked is the batch size bulk writes are admitted against right
// now: w.burst frames, within bulkCap.
//
// The configured batch size is a bound on flush duration — every co-batched
// writer's latency floor — but it is stated in bytes, and what a byte costs
// in time is the carrier's business: 448KB is a third of a millisecond on
// 10GbE and eighteen on a 200mbit path. So bulk is admitted against a limit
// that adaptBurstLocked sizes from how long flushes are actually taking. On
// a fast carrier it sits at the cap and batches fill; on a slow one it comes
// down to a single frame, and a small write arriving mid-flush waits for one
// frame to leave rather than a batch of them. Small writes are not subject
// to it: they are what it protects.
func (w *writer) bulkLimitLocked() int {
	per := int(w.s.peerMaxFrame.Load()) + frame.HeaderSize
	return max(per, min(w.bulkCap(), w.burst*per))
}

// burstFramesLocked decides how many frames of an n-byte write may be staged
// together, given that the first has been admitted.
//
// A write spanning several frames used to cost a syscall per frame even with
// the session to itself: each frame was staged, flushed inline and waited
// for before the next was looked at, so a lone bulk stream never batched with
// itself. Staging them together is the same lever as a larger frame size
// (about a quarter more throughput on writes of several frames) without
// asking the peer to accept larger frames.
//
// With other streams' data in the batch or waiting for it, the stream gets
// its configured share and no more, exactly as if its frames had arrived one
// at a time. With the batch to itself it may fill it. Either way the batch
// is bounded by bulkLimitLocked, so on a carrier too slow to absorb a burst
// quickly this comes to one frame and behavior is unchanged.
func (w *writer) burstFramesLocked(st *NativeStream, n, maxf int) int {
	per := maxf + frame.HeaderSize
	limit := w.bulkLimitLocked()
	share := w.s.cfg.PerStreamBatchBytes
	if w.admitting == 0 && w.dataPending == st.batchBytes && w.shardBytes.Load() == 0 {
		share = limit
	}
	room := min(limit-w.dataPending, share-st.batchBytes)
	return max(1, min(room/per, (n+maxf-1)/maxf))
}

// adaptBurstLocked resizes the bulk batch after a flush of n bytes that took
// d. A flush the carrier absorbed quickly doubles it, provided the flush was
// large enough to have tested the current size; a slow one halves it. The
// thresholds are in time rather than bytes because time is what a writer
// arriving mid-flush pays.
func (w *writer) adaptBurstLocked(n int, d time.Duration) {
	maxf := int(w.s.peerMaxFrame.Load())
	per := maxf + frame.HeaderSize
	switch {
	case d > burstSlow:
		w.burst = max(1, w.burst/2)
	case d <= burstFast && n >= w.burst*maxf/2:
		w.burst = min(w.burst*2, max(1, w.bulkCap()/per))
	}
	// Republished every time, not only on a change: the peer's frame size
	// arrives with its SETTINGS, after the first flush has already run.
	w.takeHint.Store(int64(w.bulkLimitLocked() / per * maxf))
}

// admissionBlockedLocked reports whether this write must wait for the
// pending batch to flush: either the batch is full, or this stream has
// already used its share of it. A batch with no data in it always admits, so
// a single oversized write can never deadlock against its own cap.
func (w *writer) admissionBlockedLocked(st *NativeStream, total int) bool {
	shards := int(w.shardBytes.Load())
	if w.pending == 0 && shards == 0 {
		return false
	}
	// Bulk is held to a smaller batch than small writes, sized from how fast
	// the carrier is taking flushes; see bulkLimitLocked.
	//
	// The bulk limit counts data only. Control frames never wait on a cap
	// and are not what it bounds; counting them would hold a bulk frame out
	// of a one-frame batch for a whole flush whenever a window update
	// happened to be staged first.
	if total > zeroCopyThreshold {
		if w.dataPending > 0 && w.dataPending+total > w.bulkLimitLocked() {
			return true
		}
	} else if w.pending+shards+total > w.s.cfg.MaxBatchBytes {
		return true
	}
	return st.batchSeq.Load() == w.seq+1 && st.batchBytes+total > w.s.cfg.PerStreamBatchBytes
}

// kick flushes whatever is staged if nobody is. It is the second half of a
// held append.
func (w *writer) kick() {
	// Staged bytes are already published, so a flusher seen running here
	// will see them before it may exit.
	if w.flushing.Load() {
		return
	}
	w.mu.Lock()
	flush := w.claimFlushLocked()
	w.mu.Unlock()
	if flush {
		w.flushLoop()
	}
}

// claimFlushLocked makes the caller the flusher when none is running and
// there is work. Any enqueue may claim it, control included: a download-only
// session has no data writers, so window updates would otherwise never reach
// the wire and the transfer would deadlock once the initial window drained.
func (w *writer) claimFlushLocked() bool {
	if w.flushing.Load() || w.err != nil || (w.pending == 0 && w.shardBytes.Load() == 0) {
		return false
	}
	w.flushing.Store(true)
	return true
}

// flushLoop drains the pending batch, re-checking after each writev so the
// carrier stays continuously fed with no goroutine wakeup between
// back-to-back flushes. The caller has already claimed flusher duty.
//
// Flusher duty is unconditional: this returns only with the batch drained or
// the session failed, never leaving a non-empty batch with no flusher.
func (w *writer) flushLoop() {
	// Like the reader, this goroutine must not take the host application
	// down with it: a panic here becomes a session failure, not a crash.
	defer w.s.recoverPanic("session flusher")
	for {
		w.mu.Lock()
		if w.err != nil {
			w.flushing.Store(false)
			w.mu.Unlock()
			return
		}
		if w.pending == 0 && w.shardBytes.Load() == 0 {
			// Clear the flag first and look again second. A fast-path writer
			// publishes its bytes and then checks the flag, so one of us is
			// bound to see the other: either it finds no flusher and claims
			// the duty itself, or its bytes are visible here.
			w.flushing.Store(false)
			if w.shardBytes.Load() == 0 {
				w.mu.Unlock()
				return
			}
			w.flushing.Store(true)
		}
		// Gather the shards before the batch number moves on. A stream with
		// main-lane data in this batch is kept off its shard by that number;
		// publishing the new one first would let its next small frame into
		// a shard that is still to be gathered, and so into this batch
		// ahead of the main-lane data it follows.
		n := w.gatherShardsLocked()
		seq := w.seq
		w.seq++
		w.seqA.Store(w.seq)

		// Swap the pending buffers with the flusher's scratch: neither
		// side allocates once both have grown to steady-state size.
		w.fctl, w.ctl = w.ctl, w.fctl[:0]
		w.fprio, w.prio = w.prio, w.fprio[:0]
		w.fstage, w.stage = w.stage, w.fstage[:0]
		w.fchunks, w.chunks = w.chunks, w.fchunks[:0]
		w.fpingStamp, w.pingStamp = w.pingStamp, false
		w.fbatchDone, w.batchDone = w.batchDone, nil
		n += w.pending
		w.fframes, w.frames = w.frames, 0
		w.fpayload, w.payload = w.payload, 0
		w.pending = 0
		w.pendingA.Store(0)
		w.dataPending = 0
		// The new batch is empty: this, not the end of the flush, is when
		// writers held out by a full batch can be let in.
		w.wakeAdmitLocked()
		w.mu.Unlock()

		start := time.Now()
		err := w.writeOut(start)
		elapsed := time.Since(start)

		w.mu.Lock()
		w.doneSeq = seq + 1 // batches completed, not last completed seq
		if err != nil && w.err == nil {
			w.err = err
			w.failed.Store(true)
		}
		if w.fbatchDone != nil {
			close(w.fbatchDone)
			w.fbatchDone = nil
		}
		if err == nil {
			w.adaptBurstLocked(n, elapsed)
		}
		// The sticky error may already be set by fail() while this flush
		// succeeded — a session dying for an unrelated reason. Either way
		// flushing stops, but only our own carrier error kills the session.
		done := w.err != nil
		if done {
			w.flushing.Store(false)
		}
		w.mu.Unlock()

		if err != nil {
			// Outside the lock: fatal closes the carrier and destroys
			// streams, and calls back into fail().
			w.s.fatal(&SessionError{Code: CodeInternal, Reason: "carrier write: " + err.Error()})
			return
		}
		if done {
			return
		}
	}
}

// writeOut issues the swapped-out batch. Control bytes lead the iovec, so a
// window update is never delayed behind queued bulk data.
func (w *writer) writeOut(now time.Time) error {
	w.iov = w.iov[:0]
	if len(w.fctl) > 0 {
		w.iov = append(w.iov, w.fctl)
	}
	if len(w.fprio) > 0 {
		w.iov = append(w.iov, w.fprio)
	}
	// Shards go ahead of the main data lane: a stream may follow a shard
	// frame with a main-lane one in the same batch, never the reverse.
	for i := range w.shards {
		if w.fshards&(1<<i) != 0 {
			w.iov = append(w.iov, w.shards[i].fbuf)
		}
	}
	for _, c := range w.fchunks {
		if c.ref != nil {
			w.iov = append(w.iov, c.ref)
			continue
		}
		w.iov = append(w.iov, w.fstage[c.off:c.off+c.n])
	}
	if len(w.iov) == 0 {
		return nil
	}
	w.s.stats.recordFlush(w.fframes, w.fpayload)

	if w.s.cfg.WriteTimeout > 0 {
		_ = w.s.conn.SetWriteDeadline(now.Add(w.s.cfg.WriteTimeout))
		defer func() { _ = w.s.conn.SetWriteDeadline(time.Time{}) }()
	}

	if w.fpingStamp {
		// Stamped here, immediately before the syscall: any time this
		// batch spent queued is ours, not the network's.
		w.s.bdp.markSent(time.Now(), w.s.recvTotal.Load(), w.s.stalls.Load())
		w.fpingStamp = false
	}

	if w.tcp != nil {
		// net.Buffers.WriteTo issues writev, looping past the 1024-iovec
		// kernel limit, so a flush is not assumed to be one syscall.
		//
		// It writes through a *copy* of the slice header deliberately:
		// WriteTo consumes the Buffers it is given, advancing it as it
		// writes. Passing w.iov directly leaves it empty but pointing at
		// the end of its backing array, so the next flush's appends
		// reallocate.
		//
		// The copy lives in a field rather than a local because WriteTo
		// has a pointer receiver: taking the address of a local makes it
		// escape, which is an allocation on every flush.
		w.iovOut = w.iov
		_, err := w.iovOut.WriteTo(w.tcp)
		return err
	}

	// Any other carrier (TLS, wrappers, Windows) gets one contiguous write
	// instead of one write per buffer. Over TLS this is still split into
	// 16KB records by crypto/tls, each its own carrier write; chunking the
	// copy at that size keeps no memcpy larger than a record can carry.
	w.coalesce = w.coalesce[:0]
	for _, b := range w.iov {
		w.coalesce = append(w.coalesce, b...)
	}
	buf := w.coalesce
	for len(buf) > 0 {
		n := min(len(buf), tlsRecordSize)
		if _, err := w.s.conn.Write(buf[:n]); err != nil {
			return err
		}
		buf = buf[n:]
	}
	return nil
}

// drain blocks until everything staged has reached the carrier.
//
// A graceful shutdown must not close the connection with frames still in the
// batch. Streams are reaped as soon as both their directions finish, which
// can happen while the frames announcing and ending them are still queued, so
// a drain that waited only for streams would close the carrier and discard
// them — the peer never learns the streams existed. Found by the protocol
// model fuzzer, which noticed the peer being told about fewer streams than
// were sent.
func (w *writer) drain(ctx context.Context) error {
	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for {
		w.mu.Lock()
		if w.err != nil {
			err := w.err
			w.mu.Unlock()
			return err
		}
		if w.pending == 0 && w.shardBytes.Load() == 0 && !w.flushing.Load() {
			w.mu.Unlock()
			return nil
		}
		// Nothing staged is guaranteed a flusher, so take the duty rather
		// than waiting for one that may never arrive.
		flush := w.claimFlushLocked()
		w.mu.Unlock()
		if flush {
			w.flushLoop()
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.s.done:
			return w.s.closedErr()
		case <-t.C:
		}
	}
}

// fail latches the error when the session dies for a reason the writer did
// not observe itself (carrier read error, local Close). Parked writers are
// released by the session's done channel, which every one of them selects on
// and which is closed before this is called.
func (w *writer) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.failed.Store(true)
	w.mu.Unlock()
}
