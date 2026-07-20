# mux — Design Document

A stream multiplexer for Go that outperforms yamux while remaining idiomatic,
cleanly separated, and dependency-free (standard library only in the core module).

Status: pre-implementation design. Decisions made 2026-07-20; red-teamed the
same day by three independent review passes (performance claims verified
against the Go 1.26 stdlib source, concurrency-hazard review, fresh-eyes
design diff), with all surviving amendments folded in.

## Goals

- Beat yamux measurably: ≥2× single-stream bulk throughput, ≥4× many-stream
  small-message throughput, lower p99 latency under load, 0 amortized
  allocations per frame at steady state.
- Zero dependencies in the core module. Comparisons (yamux, smux) and the QUIC
  adapter live in separate Go modules.
- Idiomatic API: streams are `net.Conn`s; sessions have context-aware
  Open/Accept; errors follow `net.Error` conventions.
- Transport-agnostic surface: the same `Session`/`Stream` interfaces are
  satisfied by the native TCP/byte-stream implementation and by a thin
  quic-go adapter, both shipping in v1.

## Non-goals

- Implementing QUIC itself (loss recovery, congestion control, packetization).
  `x/net/quic` is slowly becoming the standard answer; not our fight.
- Fixing TCP head-of-line blocking. One lost packet stalls all streams on a
  TCP carrier; that is physics and QUIC's genuine advantage. The README says
  so honestly.
- Unidirectional streams, unreliable datagrams, stream priorities: deferred
  to v2. The wire format reserves room for them.
- Wire compatibility with yamux (decided against; clean slate).
- Polyfilling QUIC's actual wire format over TCP (the "QUIC on Streams"
  draft; HAProxy has an experimental implementation): our clean-slate
  framing is leaner than a QUIC polyfill while stealing the same semantics.
  Revisit only if the QUIC WG adopts it.

## Known ceilings (stated, not hidden)

- TCP head-of-line blocking: one lost packet stalls every stream on a TCP
  carrier. QUIC's genuine advantage; no userspace mux fixes it.
- A session's receive side is one goroutine, bounded by a single core's
  parse + memcpy throughput (tens of Gb/s). Scale past it with multiple
  sessions; the splice relay path bypasses userspace entirely.
- One carrier means one TCP flow's congestion window. Striping is a v2
  candidate (see SETTINGS reserved bits).

## Considered and rejected

- **io_uring** — the Go netpoller doesn't use it and stdlib gives no access;
  chasing it means unsafe scaffolding and losing goroutine-native I/O. The
  upstream proposal (golang/go#57701) is closed as not planned — settled,
  not pending.
- **MSG_ZEROCOPY** — requires MSG_ERRQUEUE completion handling, pays off only
  for large writes on real NICs, and is invisible on loopback benches.
- **kTLS** — `crypto/tls` has no kernel-TLS support today; splice-through-TLS
  is impossible from stdlib. The upstream proposal (golang/go#44506) is
  accepted but backlogged and unimplemented — if it ever lands, splice
  passthrough surviving a TLS carrier becomes strategically interesting.
  Revisit then.
- **Custom timer wheel** — modern Go timers are per-P and cheap; lazy
  deadline timers suffice.
- **Connection striping** (N carriers per session) — real HOL and
  single-flow-cwnd benefits, but requires cross-path sequencing in the wire
  format. v2 candidate; a SETTINGS feature bit is reserved.
- **Hand-rolled spinlocks / runtime hacks** — `sync.Mutex` already spins
  adaptively; `procPin` tricks are unexported for good reason. `sync.Pool`
  is our per-P sharding.

## Why yamux is beatable

Concrete costs in yamux's hot paths, in order of impact:

1. **Channel round-trip per frame.** Every send allocates a `sendReady`,
   pushes through `sendCh` to a single writer goroutine, and blocks on a done
   channel: two channel ops, an allocation, two goroutine wakeups per frame.
2. **Two syscalls per data frame** (header write, body write), and no batching
   of frames across streams into one write.
3. **`bytes.Buffer` receive path**: payload copied into a growing buffer under
   a per-stream lock; allocation churn plus an extra copy.
4. **Naive flow control**: fixed 256KB window, no BDP awareness, and every
   window update is its own frame and syscall.

The wins, in order: syscall batching → copy elimination → wakeup elimination →
allocation elimination → adaptive flow control.

## Architecture

### Send path: group commit (no writer goroutine)

A mutex guards a pending batch. `Stream.Write`:

1. Chunks data to the negotiated max frame size and reserves flow-control
   credit per chunk, **outside the batch mutex** (blocking if the window is
   empty — never while holding the mutex).
2. Appends header + payload references to the pending batch, subject to the
   fairness caps below.
3. If no flush is in flight, this writer becomes the flusher: it swaps the
   batch out, releases the lock, and issues **one `net.Buffers` writev** for
   the entire batch. Under load, one syscall carries many frames across many
   streams; idle, latency equals a direct write.

**Completion semantics are split.** Small frames are copied into a
per-session staging ring at admission; the `io.Writer` contract is satisfied
at copy time, so **staged writers return immediately** — write errors latch
on the stream/session and surface on the next call (bufio-style). Only
zero-copy writers — large payloads referenced directly in the iovec — park
until the flush containing their bytes resolves. Blocking everyone would
make every small message pay full flush latency, capping exactly the
many-stream small-message workload the ≥4× target lives in.

**The flusher loops.** After completing a writev, the flusher re-checks the
pending batch and flushes again, up to a bounded number of rounds before
handing off. This keeps the socket continuously fed (no goroutine-wakeup
gap between back-to-back flushes) and is required for correctness: with
staged writers returning immediately, there may be no parked goroutine
available to become the next flusher.

Invariants (each one's absence is a deadlock or a stranded batch):

- **Flush trigger**: any enqueue — data or control — that observes no flush
  in flight becomes (or designates) the flusher. Control frames never wait
  for a data writer to happen along; a download-only session's window
  updates must flush themselves or the transfer deadlocks permanently.
- **Flusher duty is unconditional**: a designated flusher flushes (or hands
  off under the mutex) before acting on its own stream's cancellation,
  deadline, or error.
- **Flusher exit protocol**, on every path (success, carrier error, session
  teardown), under the mutex: complete this batch's waiters (with the
  error, if any); then take the pending batch, designate an awake
  successor, or — on session death — fail all pending waiters.
  Flush-in-flight is never false while a non-empty batch has no flusher.
- Batch-completion wakeup is a broadcast (intrusive waiter list or
  generation-counted cond), never a per-batch channel — that would be an
  allocation per batch.

Fairness and bounds:

- The pending batch is **byte-capped** (order 256KB–1MB, benchmarked);
  writers beyond the cap block at admission — which is also where write
  deadlines legally apply. The cap bounds flush duration (every co-batched
  writer's latency floor), staging occupancy, and the coalesce buffer size.
- **Per-stream per-batch byte cap with round-robin** among waiting streams,
  so one bulk stream cannot monopolize consecutive batches — the userspace
  analogue of HTTP/2's write scheduler. Control frames sort first in the
  iovec; this cap protects other streams' DATA behind them. A newly
  waiting stream joins the ring **at the tail**: Go's h2 scheduler starved
  long-lived streams for years precisely because newly-writable streams
  enqueued ahead of existing ones (go#58804).
- `net.Buffers` issues at most 1024 iovecs per writev and loops beyond
  that. Flat staging keeps small-frame iovec counts low, but a flush is not
  assumed to be exactly one syscall.

Write deadlines and stalled peers:

- A stream with an **active write deadline never uses the zero-copy path**:
  its payloads are copied to staging, so a deadline firing after admission
  can release the writer immediately — the bytes are session-owned.
  (Mid-writev the kernel is reading a zero-copy caller's memory; releasing
  that writer early would violate the buffer-reuse contract. Bulk paths
  that want zero-copy don't set deadlines.)
- A **session-level carrier write timeout** bounds every flush (deadline
  set on the carrier per flush). Expiry is session-fatal — correct for a
  mux, since a partially written batch has already desynced the wire. This
  is what prevents one dead peer from wedging every writer forever.

Carrier realities:

- `net.Buffers` becomes writev **only on an exact `*net.TCPConn`** — the
  enabling interface inside `net` is unexported, so no wrapper can
  implement it; wrapped carriers (and Windows) degrade to one `Write` per
  buffer. The session detects the carrier type once and coalesces the batch
  into a contiguous buffer (preallocated at the batch cap) for anything
  that is not exactly a TCP conn.
- **TLS:** one `tls.Conn.Write` per batch, but `crypto/tls` splits it into
  ≤16KB plaintext records, each written to the carrier separately — and
  dynamic record sizing (on by default) starts records near ~1.2KB until
  128KB have been sent on the connection. Batching still wins (one record
  per 16KB instead of one per small frame), but it is **not** one syscall
  per batch: chunk the coalesce at 16KB so no memcpy exceeds what a record
  carries, and document `DynamicRecordSizingDisabled: true` for bulk-TLS
  users. Zero-copy does not exist on TLS at all — crypto/tls copies and
  encrypts into its own record buffer regardless.

Contention plan (many-core): the batch mutex is a global serialization
point by construction. Mutex/block profiles are first-class benchmark
outputs, and the suite includes a GOMAXPROCS scaling curve (1→2→8→32) to
find the knee. If a knee appears, the named mitigations — in order — are
per-stream/per-P pre-staging (frames assembled outside the lock, admission
just links them in) and an MPSC admission queue with the flusher as
consumer. Discovering contention in M6 with no mitigation sketched would be
the project's biggest schedule risk.

### Carrier tuning

- **The session sets `TCP_NODELAY` itself** when the carrier is a
  `*net.TCPConn`. Group commit does Nagle's job in userspace, better; leaving
  the kernel's version on serializes our batches against delayed ACKs. This
  is the most common mux latency footgun in the wild and is not left to the
  caller to remember.
- Optional `Config` knobs applied via `TCPConn.SyscallConn()` and the frozen
  but stdlib `syscall` package (no x/sys): `SO_SNDBUF` / `SO_RCVBUF`, and
  `TCP_NOTSENT_LOWAT` (constant defined locally under build tags) to cap
  unsent kernel-buffered data so tail latency reflects our queue, not
  bufferbloat in the socket send buffer. All off by default.

### Receive path: pooled segments, zero-copy relay

One reader goroutine per session, with a **deliberately small parse buffer**
(header + small-frame threshold, ~8–16KB). Small payloads are copied
**once** into pooled, size-classed segments appended to the owning stream's
segment queue. When a frame's payload extends beyond what is buffered, the
buffered prefix is copied and the **remainder is `ReadFull`ed directly into
the segment** — kernel → segment for the majority of bulk bytes. A *large*
parse buffer would defeat this: during sustained bulk receive it slurps
payloads into itself, so a "read directly if not already buffered" path
would almost never fire, and stdlib offers no scatter read to resolve the
tension. The small-buffer copies-vs-syscalls tradeoff is settled by the M4
benchmark, not assumed.

- `Stream.Read` copies out of segments (the `io.Reader` contract requires it)
  and returns exhausted segments to the pool.
- `Stream.WriteTo` hands segments directly to the destination writer, so
  `io.Copy(dst, stream)` skips the intermediate copy. If `dst` is another mux
  stream, segments ride straight into its writev batch: the proxy/relay path
  is kernel → segment → writev with no additional copies. This is the
  headline feature for tunnel workloads, yamux's main habitat.
- `Stream.ReadFrom` is the mirror fast path for `io.Copy(stream, src)`.

One socket read yields many frames, often several for the same stream. The
dispatch loop parses the entire read while tracking a dirty-stream set, then
wakes each affected stream **once per read batch**, not once per frame. The
dirty set is batch-local (or flags are cleared before the woken stream's
final emptiness re-check), so the skip-if-already-marked optimization can
never become a lost wakeup.

Stream lookup is a copy-on-write map behind an `atomic.Pointer`, snapshotted
once per read batch: dispatch is single-consumer, so the per-frame cost is
one atomic load — no RWMutex RLock/RUnlock (two atomic RMWs) in the tightest
loop in the system. Open/close clones the map under a plain mutex;
churn is rare relative to frames.

Reader and relay invariants:

- **The reader goroutine never blocks on send-side resources and never
  performs carrier writes.** Everything it must originate (PING ACK, RST on
  protocol error, refused-stream RST) is bounded per-stream/per-session
  merged state that the flusher materializes into frames — there is no
  control queue that can fill and stall the read loop. A reader parked on a
  full send path is the classic two-sided gridlock: both peers stop
  reading, both flushers park in writev, forever.
- **No lock is held across a blocking operation.** `WriteTo` drains
  segments under the receive-queue lock, releases it, then feeds the
  destination; otherwise one slow relay target stalls the entire session's
  receive path behind a mutex. Cross-session work (segment release, credit
  return, flush triggering on the upstream session) runs strictly outside
  all of the downstream session's critical sections — the bidirectional
  tunnel topology otherwise ABBA-deadlocks the two batch mutexes.
- **Segments have exactly one owner at all times.** The ownership chain
  (reader → stream queue → `WriteTo` → downstream batch → flusher) is
  explicit, and every segment carries a release hook — return to pool plus
  upstream credit return — invoked exactly once on every exit path:
  flushed, scrubbed on reset, scrubbed on GOAWAY, dropped on teardown. A
  reset that drops relayed segments without firing the hook permanently
  stalls the upstream sender; a double release is silent pool corruption.
  Once a batch is swapped in-flight it belongs solely to the flusher;
  scrubbing only touches not-yet-swapped batches under the batch mutex.

### Passthrough relay (Linux, M4 experiment)

On Linux, `TCPConn.ReadFrom` uses splice(2) for TCP-socket sources and
sendfile(2) for files, and both unwrap `io.LimitReader`. The naive shape —
write a frame header promising `frameLen` bytes, then splice — is **fatally
broken**: the header commits to bytes that don't exist yet, so a source
that stalls holds exclusive carrier access indefinitely (freezing every
stream *and all control frames* — a self-inflicted stall worse than the TCP
HOL we disclaim), and a source EOF mid-frame desyncs framing with no
recovery short of killing the session.

The workable shape: **size each passthrough frame by bytes already queued**
in the source's receive buffer (`FIONREAD` via `SyscallConn` + the frozen
`syscall` package), capped at a per-chunk bound so other streams get
interleave points. The splice then moves only data guaranteed present: it
cannot stall waiting on the source and cannot come up short; any short
splice is a session-fatal protocol error, stated explicitly. Honesty notes:
the stdlib splice path moves data through an internal pipe (two splice
syscalls per chunk), so the claim is "payload never enters userspace," not
"syscall-free"; carrier hold time is bounded by the chunk cap plus the
session write timeout. Lands behind a benchmark in M4 with a transparent
portable fallback — an experiment, not a v1 promise.

### Flow control

- Per-stream credit windows enforced at the sender. **Window updates carry
  an absolute cumulative credit limit (u64), not a delta** — the choice
  both smux v2 and QUIC (MAX_STREAM_DATA) converged on. Absolute limits
  are idempotent: a lost, duplicated, or double-merged update self-heals,
  where any delta bug is a permanent credit leak or silent over-grant. The
  sender enforces `sent < limit`; internal arithmetic is 64-bit, and a
  received limit that shrinks is a protocol error.
- **Window updates are merged state, not queue entries**: each stream keeps
  one pending-limit cell that the flusher reads as it builds the iovec —
  and merging absolute limits is just `max()`, which eliminates the
  accumulator-zeroing race class entirely. A queue that can fill is a
  reader-goroutine stall waiting to happen. Updates trigger at half-window
  consumption. On a pure-receiver session each flush carries every pending
  stream's update in one writev — but it *is* a writev of its own there, so
  the "window updates cost no extra syscall" property is scoped to
  bidirectional traffic.
- **BDP autotuning with feedback discipline**: PINGs are timestamped at
  carrier write time by the flusher — never at enqueue, or queue delay
  inflates measured RTT and the estimator grows windows *because* the
  session is loaded: a bufferbloat spiral bounded only by `MaxWindow`. The
  estimator uses a min-filtered RTT and caps growth per RTT — the gRPC
  estimator's discipline, not just its trigger. The sampling mechanism is
  specified, not implied: **receiver-driven** — count bytes delivered
  between a flusher-timestamped PING and its ACK (that count *is* the BDP
  sample; RTT alone sets no window target), and gate growth on a
  bandwidth-sample plateau (grow only when the sample approaches the
  current window *and* measured bandwidth equals the max observed — the
  gate that actually rejects queue-inflated samples). The M5 fuzz/soak
  matrix includes window growth racing FIN/RST/GOAWAY: dynamic windows
  have caused correctness bugs, not just tuning bugs (grpc-go's
  ~150ms-link "unexpected EOF" race, grpc-go#5358).
- **No connection-level window on the wire, but a session receive budget in
  the implementation.** Autotune exists to push windows toward `MaxWindow`,
  so the naive bound (`MaxIncomingStreams × MaxWindow`) is gigabytes — an
  OOM shaped like "many briefly-fast consumers stall at once," not an
  exotic attack. The budget clamps *granted* credit locally: autotune
  distributes Σ(outstanding credit + queued unread bytes) ≤ budget, only
  ever declining to grow — never revoking granted credit — so it adds no
  deadlock surface and needs no wire change.
- Two open-time consequences of the budget. Streams **start at a small
  initial window** (slow-start; autotune provides all growth), so a SYN
  burst cannot violate the budget before it can decline — the negotiated
  initial window times `MaxIncomingStreams` must fit the budget by
  construction. And because per-session budgets don't compose across a
  10k-session proxy, `Config` accepts an **optional memory-accounting
  interface** (`ReserveMemory(n, prio) error` / `Release(n)`; nil = no-op;
  an atomic counter shared across sessions satisfies it) invoked at
  open/accept and window growth — the reason libp2p forked yamux rather
  than wrapping it (their resource manager landed after an
  exploited-in-the-wild memory advisory, CVE-2022-23492), gained here
  without the dependency.
- **Credit reservation invariants**: reserve strictly outside the batch
  mutex (a stream waiting on an empty window while holding it stalls every
  stream); unreserve on every failed-admission path — session death,
  admission deadline, batch scrub on reset or GOAWAY. Each missed path is a
  permanent per-stream stall.

### Wakeup and allocation discipline

- No per-frame channel handoffs anywhere. Blocked readers/writers park on
  per-stream single-slot channels or `sync.Cond`, woken only on actual state
  changes.
- Steady state allocates zero per frame: pooled segments, staging ring,
  preallocated header scratch. `testing.AllocsPerRun` is only a smoke gate —
  it forces GOMAXPROCS(1), collapsing `sync.Pool` to one P-shard — so the
  real CI gate is allocs/op thresholds on the *concurrent* `-benchmem`
  benchmarks.
- `sync.Pool` is drained across GC cycles (two-generation victim cache); the
  zero-alloc claim is therefore validated with GC active in the benchmark,
  and pools grow a small fixed free-list backstop only if measurement demands
  it.
- Per-stream costs are pooled too, not just per-frame: stream objects are
  reused with a generation counter (use-after-close becomes a detectable
  error, not pool corruption), wake channels are preallocated and reused
  across incarnations, and an allocs-per-open/close gate parallels the
  per-frame one. At proxy-grade churn the per-stream constant dominates the
  profile.
- Deliberate cache-line layout: read-path and send-path fields of hot
  structs live in separate cache lines, and `Stats()` counters are grouped
  by writing goroutine (reader-owned vs flusher-owned) and padded — shared
  lines under cross-core atomics are the classic silent 5–10% tax. Cheap at
  struct-definition time, expensive to retrofit.
- Pooled segments are never zeroed on reuse (that's the point), so a
  segment is exposed strictly up to its valid written length — handed out
  beyond it, it leaks another stream's prior payload: silent cross-stream
  data disclosure. The fuzzer asserts the bound.
- Deadline timers are lazy: no `time.Timer` exists until a deadline is set.

## Wire protocol (clean slate)

Big-endian. Fixed 8-byte header:

```
byte 0            bytes 1-4        bytes 5-7
+--------------+----------------+-------------+
| type | flags |  stream ID u32 | length u24  |
+--------------+----------------+-------------+
   bits 0-3: type   bits 4-7: flags
```

- 24-bit length caps a frame at 16MB; `MaxFrameSize` (negotiated, default
  64KB) is far below that.
- Stream ID 0 is the session. Client-initiated streams are odd, server even.
- Fixed-width fields over varints: the byte savings don't justify the branchy
  parse.

Frame types:

| Type | Name          | Payload                          | Flags     |
|------|---------------|----------------------------------|-----------|
| 0    | DATA          | application bytes                | SYN, FIN  |
| 1    | WINDOW_UPDATE | u64 absolute cumulative limit    | SYN       |
| 2    | RST           | u64 error code, u64 final size   |           |
| 3    | STOP_SENDING  | u64 error code                   |           |
| 4    | PING          | u64 opaque                       | ACK       |
| 5    | GOAWAY        | u32 last ID, u64 code, reason    |           |
| 6    | SETTINGS      | list of (u16 id, u64 value)      | ACK       |
| 7    | PADDING       | discardable bytes (reserved, v2) |           |

Semantics stolen from QUIC deliberately, so the quic-go adapter maps 1:1:

- **SYN piggybacks** on the first frame of a stream: `OpenStream` is purely
  local and costs zero syscalls; open + first byte is one batched write.
- **FIN** is half-close (`CloseWrite`). **RST** aborts our sending side with
  an application error code (QUIC RESET_STREAM). **STOP_SENDING** asks the
  peer to stop (`CancelRead`).
- **RST carries the final size** (as QUIC's RESET_STREAM does), so adding a
  connection-level window in v2 would not need a wire change.
- An RST flag bit is **reserved for a trailing u64 reliable size**: QUIC's
  Reliable Stream Reset extension (RESET_STREAM_AT — at the IESG as of
  mid-2026, already shipped in quic-go) resets a stream while guaranteeing
  delivery up to an offset. Implementation is v2; the bit is claimed now so
  the adapter can map it later without a wire break.
- **A stream's RST/STOP_SENDING is emitted only after its SYN has flushed.**
  Control frames sort first in the iovec and SYN rides a DATA frame, so a
  cancel racing the first flush would otherwise emit an RST for a stream
  the peer has never seen (which the peer must treat as either a
  session-killing protocol error or a leaked half-open stream — both bad).
  A cancel before the SYN flushes is handled locally: the stream's frames
  are scrubbed from the pending batch (segments and credit released) and
  nothing is sent. A cancel racing an in-flight batch containing the SYN
  defers the RST until that flush resolves (per-stream "SYN flushed" bit).
- **Receiver rules** (the protocol fuzzer's oracle): a non-SYN frame for an
  unknown ID at or below the peer's max-seen ID is dropped silently
  (post-RST stragglers); an unknown ID above max-seen without SYN is a
  protocol error; SYN for an existing stream is a protocol error; SYN on
  anything but a DATA frame is a protocol error.
- **SYN order is not ID order.** Because `OpenStream` assigns an ID locally
  and the stream only announces itself on its first frame, concurrent
  opens legitimately put SYNs on the wire out of ID order (stream 63 may
  announce before stream 1). A receiver must therefore accept a SYN for
  any not-currently-live ID, and must not treat "ID below max-seen" as
  reuse — the price of zero-syscall open, and a rule HTTP/2 can impose
  only because HEADERS both allocates and announces. Reusing a reaped ID
  is indistinguishable from opening a fresh one and costs an attacker
  exactly the same, so it is permitted rather than policed. (M2's
  implementation was caught by its own concurrency test here.)
  Straggler payloads are excluded from — or immediately refunded to — the
  session receive budget on the drop path: bytes counted against a shared
  budget need a refund on *every* exit path, including for streams that no
  longer exist (the x/net/http2 conn-window bug farm, go#40423 et al.).
- **Zero-cost frames are policed.** Every frame in the empty-frame-flood
  family (CVE-2019-9515/9516/9518) costs the victim parse+dispatch while
  consuming zero flow-control credit, so byte budgets never trigger. Rules:
  a WINDOW_UPDATE that grants nothing is a protocol error; SETTINGS more
  than once post-handshake is a protocol error; and a cheap
  no-op-frames-per-interval counter on the single-consumer read loop
  escalates to GOAWAY.
- **Control-first iovec sorting never reorders delivery-relevant frames
  within a stream.** FIN rides its DATA frame and RST waits for SYN flush
  (both above), which structurally excludes the smux-class bug where a
  prioritized FIN overtook the final data frame — silent truncation in
  ~1/500 sessions under load. The protocol fuzzer asserts per-stream order
  regardless.
- **PING doubles as keepalive**: jittered periodic PING with a liveness
  timeout — judged on receive-side silence, not on the ability to send,
  since both peers can be send-stalled simultaneously. Without this, dead
  carriers hold streams open until the kernel notices, minutes later.
- **PING ACK is single-slot, latest-wins by specification.** An opaque
  echo cannot be "merged state" and also unbounded — that contradiction is
  resolved explicitly: the owed-ACK state is one cell (latest inbound
  opaque), so a peer with more than one PING in flight gets degraded echo,
  which costs a conformant peer nothing (one outstanding probe is the
  protocol's own usage) and denies an attacker unbounded owed-pong memory
  (the rust-yamux OOM, CVE-2024-32984). Inbound PINGs are rate-policed
  (minimum interval advertised in SETTINGS; violations are strikes
  escalating to GOAWAY — gRPC's `too_many_pings` shape). Only our own
  flusher-timestamped PINGs feed the RTT estimator.
- A DATA flag bit is **reserved for piggybacked credit** (a u64 absolute
  credit limit ahead of the payload) — a v2 candidate that removes
  standalone WINDOW_UPDATE frames in steady bidirectional flows; the bit is
  claimed now because wire freezes are forever.
- **PADDING (type 7) and a SETTINGS feature bit for "peer requires
  padding" are reserved for v2.** Mux-level padding is the one feature
  tunnel operators actually shipped in the last two years (TLS-in-TLS
  fingerprinting falls >70% with mux+padding, USENIX Security '24; sing-box
  and mux.cool both grew it), and it must live at the mux layer to mask
  per-stream frame sizes. Reservation is free; retrofit is a wire break.
- **GOAWAY carries optional reason bytes, length-capped at 1KB and enforced
  on receive** — this is where the API's `CloseWithError(code, msg)` message
  goes (h2 ships bounded debug data on GOAWAY for the same reason: the
  operator at the far end needs the *why*). Discovered post-freeze, this
  would have been a v2 frame.
- **SETTINGS** is exchanged at session start (initial window, max frame
  size, **max concurrent streams**, feature bits, version). Advertising the
  stream limit is h2's lesson: `OpenStream` blocks context-aware at the
  advertised limit instead of discovering overflow one RST round-trip at a
  time; REFUSED remains the race-window fallback, and a documented
  session-pool example builds min/max-streams-per-session policy on top of
  REFUSED-is-retriable. This is the forward-evolvability yamux lacks — its
  version byte is frozen forever. Feature bits are reserved now for v2
  candidates: unidirectional streams first (WebTransport reaching browser
  Baseline has normalized them), connection striping (follow QUIC
  multipath's explicit path-ID model), and RFC 9218-shaped priorities
  (urgency + incremental fits the existing flags/SETTINGS space).

### Lifecycle rules

- **Accept overflow**: a SYN beyond `MaxIncomingStreams` (or the
  accept-queue cap) creates no state; the payload is dropped and an RST
  with a REFUSED code is sent. REFUSED is retriable at the opener, which
  releases its reserved credit and scrubs its batched frames.
- **RST before accept**: a stream reset while parked in the accept queue is
  removed and its buffered segments released — otherwise churn leaks queue
  slots against the accept bound.
- **GOAWAY drain**: `OpenStream` is local and zero-syscall, so "opened but
  the peer will never hear of it" is a normal state. On receiving
  GOAWAY(lastID), locally opened streams above lastID fail with a
  retriable error and their pending frames are scrubbed (segments and
  credit released). The API exposes graceful drain — `Session.Shutdown`:
  send GOAWAY, stop accepting, wait for streams to complete — because a
  GOAWAY frame is useless without an entry point that drives it.
- **Concurrent Read/Write**: `net.Conn` permits concurrent calls;
  per-stream read and write mutexes serialize them — which also makes the
  consumption accounting single-writer, so the half-window update trigger
  cannot be raced past and missed permanently.
- **Reset churn is bounded (Rapid Reset / MadeYouReset class).** An
  incoming stream occupies its `MaxIncomingStreams` slot until the
  *application* closes it — never freed early by peer RST, or reset churn
  recreates HTTP/2 Rapid Reset (CVE-2023-39325) one layer down; this
  design's zero-syscall open and retriable REFUSED make churn maximally
  cheap for an attacker, so the accounting must be maximally strict. A
  sliding-window budget of streams reset before useful work escalates to
  GOAWAY + close, and locally originated receiver-rule RSTs are
  rate-limited under the same budget so protocol-error responses cannot be
  farmed as a reset oracle (MadeYouReset, CVE-2025-8671). All local
  accounting; no wire change.
- **Zombie reaping**: after a local `Close()`, a per-stream linger timeout
  (lazy timer; configurable, minutes-order default) bounds how long a peer
  that never completes close can pin stream state — expiry emits RST and
  releases segments, credit, and the slot. yamux retrofitted exactly this
  (`StreamCloseTimeout`) after production leaks in Vault.
- **Stream-ID exhaustion is specified, not discovered**: near exhaustion
  of the u32 space, `OpenStream` returns a distinct retriable error and
  the session self-initiates GOAWAY drain so pools rotate carriers cleanly
  (smux's answer; yamux left it an open issue for a decade).
- **Remote GOAWAY observed by blocked calls**: `OpenStream` fails fast
  with a typed retriable error; `AcceptStream` keeps draining streams at
  or below lastID, then returns a terminal typed error. The QUIC adapter
  must match; the conformance suite covers it.
- **Teardown is self-contained**: no close path spawns or parks a
  goroutine waiting on an optional consumer, and `Close()`'s resource
  release never depends on the peer responding or on anyone reading
  afterward (the spdystream/Kubernetes leak decade). A dedicated leak test
  closes a stream with unread inbound data and an unresponsive peer.
  Reader and flusher panics are recovered and converted to session-fatal
  errors — a silent panic policy is the only wrong one.

## Public API

```go
package mux // github.com/giraffesyo/mux (module path TBD until remote exists)

// Transport-agnostic; satisfied by the native session and the QUIC adapter.
type Session interface {
    OpenStream(ctx context.Context) (Stream, error)
    AcceptStream(ctx context.Context) (Stream, error)
    Shutdown(ctx context.Context) error // GOAWAY, stop accepting, drain
    CloseWithError(code uint64, msg string) error
    Close() error
    LocalAddr() net.Addr
    RemoteAddr() net.Addr
}

type Stream interface {
    net.Conn                 // Read, Write, Close, deadlines, addrs
    CloseWrite() error       // send FIN, keep reading
    CancelRead(code uint64)  // STOP_SENDING
    CancelWrite(code uint64) // RST
    StreamID() uint64        // u64 so QUIC's 62-bit IDs fit
}

func Client(conn net.Conn, cfg *Config) (*NativeSession, error)
func Server(conn net.Conn, cfg *Config) (*NativeSession, error)
```

- Constructors return concrete types (idiomatic); the interfaces exist for
  transport-agnostic consumers and the adapter.
- Streams implement `io.ReaderFrom` and `io.WriterTo` for the zero-copy relay
  paths; `io.Copy` finds them automatically.
- `Stream.WriteBuffers(bufs net.Buffers)` admits multiple user slices in one
  batch turn (one mutex acquisition, one frame sequence) — RPC frameworks
  writing header+body otherwise pay two admissions or their own copy.
- A `Peek`/`Discard` borrowed-buffer read API (zero-copy consumption for
  parsers, bufio-style over the segment queue) is reserved for v2; the
  segment queue makes it nearly free to add later.
- Errors: `*StreamError{Code uint64, Remote bool}`, `*SessionError`; deadline
  failures satisfy `net.Error` with `Timeout() == true`. Terminal session
  errors returned from `AcceptStream` deliberately do **not** satisfy
  `net.Error`'s temporary/timeout conventions even when the underlying
  carrier error does (wrapping is stripped): yamux surfacing a carrier
  "connection reset" with `Temporary() == true` sent `http.Server` accept
  loops into permanent hot-loops (yamux#56). A regression test runs the
  session as a `net.Listener` under `http.Server`.
- `Stream.Close()` = `CloseWrite` + drain-or-cancel read, mirroring quic-go,
  documented explicitly.
- An optional **session liveness hook** (callback fed by the keepalive
  machinery: RTT updates, liveness loss). Apps otherwise learn of session
  death only when a blocked call errors, and polling `Stats()` is not an
  event. muxado's `Heartbeat` surviving three implementations across a
  decade says tunnel apps genuinely want this.
- A documented **first-frame type-prefix convention** for typed streams
  (muxado's `TypedStreamSession` pattern): SYN-on-first-DATA makes the
  prefix wire-free. Ships as documentation or a thin optional subpackage,
  not core API.
- `Config.DisableBatching` routes writes through the pre-M3 direct path,
  documented as diagnostic-only: libp2p shipped write coalescing and then
  disabled it network-wide when batching changed observable read
  boundaries and exposed peers' framing assumptions (go-libp2p#644). The
  escape hatch doubles as the interop-bisection tool and a permanent A/B.
- **Keepalive is policy, not just mechanism**: defaults sit under common
  middlebox idle floors (~15–25s jittered interval; liveness timeout ≥2–3
  intervals — the frp/chisel bug class was incoherent two-party settings),
  `Config` validation rejects incoherent combinations (timeout ≤ interval;
  liveness vs carrier write timeout), and the docs say plainly: let the
  mux own keepalive; don't layer app-level pings on top.

## Module layout

```
github.com/giraffesyo/mux            core module, zero deps
  mux.go session.go stream.go config.go errors.go
  internal/frame/                    encode/decode + fuzz targets
  internal/pool/                     size-classed segment pools
  internal/ring/                     staging ring for headers/small frames
  adapters/quic/                     separate go.mod; quic-go adapter (v1)
  bench/                             separate go.mod; yamux + smux baselines
```

The root `go.mod` never gains a dependency; adapters and benchmarks are
isolated modules so `go get github.com/giraffesyo/mux` stays pristine.

## Toolchain and observability

- **Profile-guided optimization — scoped honestly**: `-pgo=auto` reads
  `default.pgo` only from a *main* package's directory, so a profile shipped
  at a library root does nothing for consumers. Our bench/CI binaries use
  PGO locally to keep our own numbers representative, and the docs tell
  users to collect profiles in their own main packages. (The
  devirtualization upside is modest here anyway: hot paths run on concrete
  types; the interfaces exist for the adapter, not the hot loop.)
- **`Session.Stats()`**: lock-free atomic counters — frames per batch,
  batch-size histogram (per-bucket atomics, no seqlock on the flusher path),
  flush stalls, window stalls, RTT estimate. Batching that cannot be
  observed cannot be tuned in production.
- `runtime/trace` regions around flush and dispatch during development;
  production users get always-on lightweight diagnostics via Go 1.25's
  `trace.FlightRecorder`.

## Benchmark & correctness plan

Benchmarks (in `bench/`, compared with `benchstat`, run in CI):

- Single-stream bulk throughput; 64- and 1024-stream concurrent small
  messages; echo RPC latency (p50/p99); stream open/close churn; allocs/op.
- Latency runs are **open-loop** (fixed arrival rate, HDR-style percentile
  capture): closed-loop echo underreports tails by omitting the requests
  that would have been issued during a stall — coordinated omission — and
  the p99 claims would not survive scrutiny.
- Mutex and block profiles are first-class outputs, with a GOMAXPROCS
  scaling curve (1→2→8→32) guarding the send-mutex knee.
- At least one two-machine real-NIC run gates the final claims: loopback
  hides TSO/GRO, interrupt moderation, and memory-bandwidth effects that
  occasionally invert rankings.
- **Mixed-workload p99**: a small-message stream sharing a session with N
  saturating bulk streams (loopback + the WAN profile), vs yamux and smux.
  This is the number that decides tunnel adoption — the "disable mux for
  gaming/streaming" folklore traces to muxes that fail exactly this case —
  and it directly guards the round-robin fairness cap.
- **Keepalive correctness under sustained bulk at high RTT** is a named
  case in the netem/two-machine matrix: the field's keepalive bugs only
  reproduced on real networks under load.
- Carriers: loopback TCP, TLS-over-loopback, a latency-injecting pipe
  (exercises BDP autotune), and a netem-shaped stage on Linux CI (delay +
  loss qdisc) — loopback's giant MTU and zero loss flatter batching and hide
  BDP behavior. `net.Pipe` alone is avoided — its synchronous rendezvous
  distorts batching.
- One canonical WAN profile is pinned in the suite and gates releases:
  ~200ms RTT with a link capacity several times `window/RTT` for a fixed
  256KB window. This is the regime where real tunnels run and where a
  static-window mux plateaus near `window/RTT` (~1.3 MB/s at 200ms)
  regardless of link speed; mux must approach link rate here via autotune.
  It is the single most decision-relevant benchmark in the suite — CPU
  micro-wins are noise next to it on high-RTT paths.
- Baselines: raw TCP (ceiling), hashicorp/yamux, xtaci/smux — **tuned, not
  defaults** (max windows, buffered writers; smux explicitly on its v2
  protocol, its best mode). Beating a strawman config proves nothing.
- The bench toolchain is pinned (Go 1.26.x) and recorded in benchstat
  output: Go 1.26's Green Tea GC and size-specialized malloc make the
  baselines' alloc churn cheaper, so the claimed multiples must not ride an
  old-GC toolchain.
- Benchmarks use `testing.B.Loop` (Go 1.24+) so bodies cannot be optimized
  away; alloc measurements run with GC active so pool-refill cost is counted.

Correctness:

- Frame-parser fuzzing (`testing.F`) plus a protocol-level fuzzer driving
  random open/write/close/reset interleavings against a model checker of
  expected stream states.
- All tests race-enabled; soak test with thousands of churning streams;
  goroutine-leak assertions after every test — mechanized with the
  `goroutineleak` pprof profile (experimental in Go 1.26, GA in 1.27),
  which is purpose-built for the permanently-blocked-goroutine shapes the
  flusher exit protocol and credit accounting can produce.
- The in-memory latency-injecting carrier is built `testing/synctest`-safe
  (virtualized time), so deadline, keepalive, and autotune-growth tests run
  deterministically and instantly instead of sleeping through real timers.
- The hairiest correctness zones get the densest tests: group-commit ×
  deadlines × reset/close, flow-control-meets-flush (both deterministic
  deadlock shapes found in design review live there), and the cross-session
  relay — the soak matrix explicitly includes mid-relay resets and
  bidirectional tunnel topologies. (Write deadlines stay enforceable after
  admission because deadline streams use the copy path.)

## Milestones

1. **M1 — Wire.** `internal/frame`: header codec, SETTINGS, fuzz targets.
2. **M2 — Correct core.** Session lifecycle, read loop, stream states,
   simple locked writes (no batching yet), flow control with static windows.
   Full protocol test suite passes; race-clean.
3. **M3 — Group commit.** Batched writev send path, staging ring, zero-copy
   large writes, TLS coalescing. Benchmarks must show the win at each step.
4. **M4 — Receive fast paths.** Segment pools, small-parse-buffer direct
   reads, `WriteTo`/`ReadFrom` relay; FIONREAD-sized splice passthrough
   experiment (Linux) behind a benchmark.
5. **M5 — Adaptive flow control.** PING RTT, BDP window growth.
6. **M6 — Beat the targets.** `bench/` module, tune until ≥2× / ≥4× / 0 allocs
   hold on Linux and macOS. Generate and check in `default.pgo` from the
   winning profile.
7. **M7 — QUIC adapter.** `adapters/quic` wrapping quic-go (v0.60+ struct
   API — `Conn`/`Stream` are structs since v0.53, not interfaces) behind
   the same interfaces; minor version pinned in the adapter's go.mod, plus
   a CI job building against quic-go's latest release to catch its pre-1.0
   API churn early. Interface-level conformance suite runs against both
   implementations.
8. **M8 — Hardening.** Protocol fuzzer, soak, docs, README with honest
   QUIC/HOL positioning — including when to prefer h2/WebSocket instead (an
   L7 proxy that must parse the stream; a custom binary mux is safe
   precisely because nothing on-path parses it) — plus a documented and
   benched mux-over-WebSocket carrier recipe (exercises the non-TCP
   coalesce fallback) and the session-pool example.

Sequencing rule: correctness first (M2), then each performance change lands
with a benchmark proving it. No speculative optimization survives without a
benchstat delta.
