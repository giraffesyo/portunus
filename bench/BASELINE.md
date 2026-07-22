# Benchmark history

Go 1.26.1, darwin/arm64, loopback TCP, `-benchtime=2s`. **Both sides tuned**:
yamux at its 16MB max window with keepalive off, mux at the same 16MB window.
Comparing our default window against yamux's tuned one measures configuration,
not implementation — see "A note on windows" below.

## Single-stream bulk: 1.8x, and why the 2x target is not met

Profiled rather than guessed at. On the bulk path the CPU profile is 61% raw
syscalls and 22% netpoller, with essentially nothing in this library's own
code: no parsing, no copying, no contention. Throughput is set by how many
syscalls a byte costs, so that is the only lever.

Frame size is that lever, because a write spanning several frames costs one
syscall per frame. Measured with 1MB writes, mux alone:

| MaxFrameSize | throughput |
|---|---|
| 64KB | 10.6 GB/s |
| 128KB | 13.0 GB/s |
| 256KB | 13.6 GB/s |

128KB is the knee. But against a fair baseline the target still is not
reached, because yamux gains from large writes too:

| Workload | mux | yamux | ratio |
|---|---|---|---|
| 64KB writes (one frame) | 10.4 GB/s | 5.4 GB/s | 1.83x |
| 1MB writes (many frames) | 12.6 GB/s | 7.7 GB/s | 1.58x |

**The default stays at 64KB.** The gain is only available to bulk senders,
while the cost is paid by every latency-sensitive stream sharing the session:
frame size divided by link rate is how long a small message waits behind a
large one, so 128KB is about 10ms on 100mbit and about 29ms on the 36mbit
path this library was built for. A loopback benchmark shows the gain and
structurally cannot show that cost, which is the same trap the WAN harness
set earlier. The knob and both numbers are documented on Config.MaxFrameSize
so an operator with a fast link can take the throughput.

So the design's three targets stand at: many-stream small messages met many
times over, zero allocations per frame met, and single-stream bulk at 1.8x
against a goal of 2x, with the remaining distance understood and available by
configuration rather than unexplained.

## Allocations: the steady-state target is met

| Benchmark | allocs/op | bytes/op |
|---|---|---|
| Bulk 64KB writes | **0** | 7 |
| Small msgs, 64 streams | **0** | 1 |
| Relay (proxy) | **0** | 86 |
| Echo RTT | 1 | 48 |
| Stream open/close | 11 | 1253 |

DESIGN.md's target was zero amortized allocations per frame at steady state,
and the three data paths that carry frames now hit it exactly. Two escapes
were responsible for most of what remained, both the same mistake in
different clothes: taking the address of a local that a pointer-receiver
method then captures.

- `pool.Put` boxed a local slice to satisfy `sync.Pool`, allocating on every
  release — in the code whose only job is to avoid allocating.
- `writeOut` copied the iovec into a local before `WriteTo`, whose pointer
  receiver made the local escape, allocating on every flush.

Deadline channels were also allocated eagerly for every stream even though
most streams never set a deadline; they are now created when one is armed.
Together with the above, churn fell from 19 allocs/op to 11.

The remaining churn cost is 54% stream construction: the struct plus its two
notify channels, on each side. Recycling those is a design commitment not yet
taken, deliberately — see the note at the end of this file.

## WAN profile, kernel-shaped (the result that settles it)

Linux CI, loopback shaped with `tc netem delay 100ms rate 100mbit`, so a
200ms round trip over a link with a real bandwidth ceiling.

| | throughput | notes |
|---|---|---|
| portunus, autotuning from the 64KB floor | **4.75 MB/s** | found an 8MB window unaided; measured RTT 205ms against 200ms injected |
| yamux, 256KB shipped default | 1.19 MB/s | the configuration real deployments run |
| yamux, hand-tuned 64MB window | 5.21 MB/s | the best a static window can do, given an operator who knows the path |

**4.0× yamux as shipped, and within 9% of yamux hand-tuned — with no
configuration at all.** The window autotuning exists to make a long path stop
capping at window/RTT, and on a real path it does exactly that.

This also retires the caveat carried through M5 and M6. On the in-process
delay harness autotuning trailed a hand-tuned window by more than 5×, and
that harness was documented as untrustworthy for absolute numbers because it
has no bandwidth limit, leaving results dominated by chunk granularity. The
kernel-shaped run confirms the harness was the problem, not the estimator:
same code, same configuration, opposite conclusion. Trust the netem row; the
`net.Pipe` rows below are kept only to show the ramp shape.

## After the hardening pass (abuse limits, honest measurement, allocations)

Loopback TCP, both sides tuned to a 16MB window, `-benchtime=2s`.

| Benchmark | mux | yamux | ratio |
|---|---|---|---|
| Relay (proxy: stream→stream) | **5003 MB/s**, 2 allocs/op | 2151 MB/s, 6 allocs/op | **2.33× ahead** |
| Small msgs, 64 streams | **365 ns/op**, **0 allocs/op** | 7414 ns/op, 2 allocs/op | **20.3× ahead** |
| Bulk 64KB writes | **10720 MB/s**, 1 alloc/op | 6586 MB/s, 2 allocs/op | **1.63× ahead** |
| Stream open/close | 9.9 µs, 19 allocs/op | 27.5 µs, 38 allocs/op | **2.78× ahead** |
| Echo RTT 64B | 33.0 µs | 34.9 µs | 1.06× ahead |
| **Open-loop p99** under 4 bulk streams | **2.12 ms** | 3.11 ms | **1.47× better tail** |
| Open-loop p50 / max | 1.51 ms / 2.46 ms | 1.01 ms / 6.76 ms | worse p50, **2.7× better max** |

Allocation targets are now essentially met on the steady-state data paths:
**0 per operation for small messages, 1 for bulk, 2 for relay** (down from 1,
3, and 6). Two real bugs were behind the rest:

- `pool.Put` took the address of a local slice to satisfy `sync.Pool`'s
  interface, which made the slice header escape on **every release** — 88% of
  all allocations, in the code whose entire purpose is to avoid allocating.
  The pool now hands out and takes back the pointer it owns.
- `net.Buffers.WriteTo` **consumes** the slice it is given, advancing the
  header as it writes. Passing the reusable iovec directly left it empty but
  pointing at the end of its backing array, so every flush reallocated. It is
  now written through a copy of the header.

### Contention: measured, not assumed

The design named the shared send mutex as the project's biggest performance
risk and specified mitigations. Measuring first says which, if any, is
warranted. Sweeping GOMAXPROCS with one writer per core:

| GOMAXPROCS | ns/op | frames/flush |
|---|---|---|
| 1 | 1956 | 1.0 |
| 2 | 411 | 7.8 |
| **4** | **335** (peak) | 307 |
| 8 | 404 | 796 |
| 16 | 411 | 745 |
| 32 | 425 | 768 |

The knee is at four cores, after which throughput **degrades about 20% and
then plateaus** rather than collapsing: batching rises as contention does, and
the larger batches pay for the lock. A mutex profile at GOMAXPROCS=16
attributes 84% of contention to `appendData` — the writer mutex, exactly as
predicted, and *not* the session's stream map.

That measurement settled two planned optimizations by ruling them out:

- **Copy-on-write stream map**: not implemented. The mutex profile shows the
  session map is not contended at all; replacing it would optimize something
  no profile identifies.
- **Per-read-batch wakeup coalescing**: not implemented. `notify` does not
  appear in the top 30 nodes of a CPU profile on the receive-heavy path.

Both remain correct ideas for a different bottleneck. Implementing them now
would violate the rule the design sets for itself: no optimization without a
measured delta.

## After M6 (autotune convergence, socket tuning, mixed-workload fairness)

| Benchmark | mux | yamux | ratio |
|---|---|---|---|
| Relay (proxy: stream→stream) | **7923 MB/s** | 2715 MB/s | **2.92× ahead** |
| Small msgs, 64 streams | **283 ns/op** | 4781 ns/op | **16.9× ahead** |
| Bulk 64KB writes | **11779 MB/s** | 6406 MB/s | **1.84× ahead** |
| Stream open/close | 7.3 µs | 17.1 µs | **2.33× ahead** |
| Echo RTT 64B | 23.7 µs | 25.6 µs | 1.08× ahead |
| Mixed: small-msg RTT under 4 bulk streams | **1.11 ms** @ 4141 MB/s bulk | 1.67 ms @ 2511 MB/s bulk | **1.50× lower latency while carrying 1.65× more bulk** |
| WAN 200ms RTT (`-benchtime=10x`) | 3.72 MB/s | 1.31 MB/s (default window) | **2.84× ahead of the shipped config** |

Design targets: ≥2× bulk (**1.84×**, near), ≥4× many-stream small messages
(**16.9×**, met), 0 amortized allocations per frame (**not met** — 3/op on
bulk, 1/op on small messages).

### Mixed workload: measure both axes or the number is meaningless

The mixed benchmark now reports concurrent bulk throughput alongside latency,
because a mux that moves less data will always look better on latency alone.
Reported one-sided, the earlier version made this comparison unreadable:
runs where yamux happened to carry less bulk showed it "winning" latency by
4×. With both axes reported the result is stable across runs — mux is ahead
on latency *and* on throughput simultaneously.

Getting there ruled out three plausible causes by measurement, none of which
was responsible: batch size (shrinking `MaxBatchBytes` 6× changed nothing),
admission fairness (reserving batch headroom for small writes changed
nothing), and kernel send-buffer depth (`TCP_NOTSENT_LOWAT` applies cleanly
on macOS — verified — and changed nothing). What remained was that our own
higher throughput deepens every queue in the pipeline, including the
single-reader dispatch path that DESIGN.md already names as a known ceiling.

`TCP_NOTSENT_LOWAT` is kept regardless: it is correct, costs nothing, and
targets a real queue that this loopback benchmark cannot exercise. Note the
constant differs per platform (25 on Linux, 0x201 on macOS) — sharing one
value silently sets an unrelated option on the other platform.

### WAN autotuning: 2.8× the shipped yamux config, still short of hand-tuning

| | throughput |
|---|---|
| mux, autotuning from a 64KB floor | 3.72 MB/s |
| yamux, 256KB default window | 1.31 MB/s |
| yamux, hand-tuned 16MB window | 19.8 MB/s |

Convergence was made reliable in M6. Growth had been tied to a BDP probe
completing, but a probe's ACK travels back through the same direction as the
bulk data it measures, so under load it can be delayed indefinitely — which
froze the window at its initial size on roughly half of otherwise identical
runs. Growth now also triggers on a per-stream flow-control stall, which
needs no round trip, and a lost probe can no longer wedge the estimator.
After that, convergence is consistent across runs.

A hand-tuned static window still wins here, and the harness (`net.Pipe` plus
a delay queue, no bandwidth limit) remains too crude to trust for absolute
numbers. The real gate stays a netem stage on Linux CI. Run the WAN row with
`-benchtime=10x`; at the default the measurement is all ramp-up.

## After M5 (BDP autotune, keepalive)

Loopback numbers are unchanged from M4 — autotuning costs nothing when the
window is already large enough:

| Benchmark | mux | yamux | ratio |
|---|---|---|---|
| Bulk 64KB writes | 11883 MB/s | 6497 MB/s | 1.83× ahead |
| Small msgs, 64 streams | 248.9 ns/op | 4910 ns/op | 19.7× ahead |
| Relay | 7845 MB/s | 2724 MB/s | 2.88× ahead |
| Echo RTT 64B | 24.1 µs | 26.9 µs | 1.12× ahead |

### WAN profile (200ms RTT) — autotuning works, but does not yet win

| Benchmark | throughput |
|---|---|
| mux, autotuning from a 64KB floor | 1.55 MB/s |
| yamux, 256KB default window | 1.31 MB/s |
| yamux, hand-tuned 16MB window | 19.8 MB/s |

**This is an open problem, not a win.** Autotuning does what it claims —
`TestBDPWindowGrowsOnHighLatencyPath` proves the window grows and the RTT
estimate is accurate to within a millisecond of the injected 200ms — and it
beats the out-of-the-box yamux configuration that real tunnels actually run.
But a hand-tuned static window still beats it by an order of magnitude,
because the climb from 64KB to a full BDP takes one doubling per round trip
and the benchmark is not long enough to amortize that ramp.

The harness is also a poor model and its absolute numbers should not be
trusted: `net.Pipe` plus a delay queue has no bandwidth limit, so results are
dominated by chunk granularity rather than by the protocol. The design's
stated gate is a netem-shaped stage on Linux CI, which this machine (macOS)
cannot run. Treat the WAN row as directional only.

Two candidate fixes for M6: start the climb from a larger initial window when
the measured RTT is high (a 200ms path justifies a big window immediately),
and allow more than one doubling per RTT while the sender is continuously
stalling.

## After M4 (segment pools, zero-copy relay)

| Benchmark | mux | yamux | ratio |
|---|---|---|---|
| Relay (proxy: stream→stream across sessions) | **8072 MB/s** | 2713 MB/s | **2.98× ahead** |
| Small msgs, 64 streams | **202.8 ns/op** (1263 MB/s) | 4618 ns/op (55 MB/s) | **22.8× ahead** |
| Bulk 64KB writes | **11886 MB/s** | 6440 MB/s | **1.85× ahead** |
| Stream open/close | 7.4 µs | 17.3 µs | **2.3× ahead** |
| Echo RTT 64B | 25.0 µs | 25.8 µs | 1.03× ahead |
| Mixed: small-msg RTT under 4 saturating bulk streams | 1.11 ms | 1.66 ms | 1.50× ahead |
| Bulk allocs | 126 B/op, 3/op | 1247 B/op, 2/op | ahead on bytes |

Design targets: ≥2× bulk (**1.85×**, near), ≥4× many-stream small messages
(**22.8×**, met), 0 amortized allocations per frame (**not yet** — 3/op).

The relay number is the one that matters most for tunnel workloads, and it is
where the receive path pays off: `io.Copy` finds `WriteTo`, which hands pooled
segments straight to the destination stream instead of copying through a
caller buffer.

### Known gap: mixed-workload latency

1.11 ms for a 64-byte echo while four bulk streams saturate is poor in
absolute terms even though it beats yamux. Adding the per-stream per-batch
fairness cap did **not** move it (1.112 ms vs 1.110 ms before), which locates
the queue: it is not in our batch, it is in the kernel socket send buffer.
Bulk writers fill it and the small message waits behind that drain.
`TCP_NOTSENT_LOWAT` — already specified in DESIGN.md as an optional carrier
knob — is the fix, and belongs to M6 tuning.

## After M3 (group-commit batched writev)

Measured before window tuning was matched, so bulk is not comparable to the
rows above.

| Benchmark | mux | yamux | ratio |
|---|---|---|---|
| Small msgs, 64 streams | 150.6 ns/op | 4825 ns/op | 32× ahead |
| Stream open/close | 7.7 µs | 17.6 µs | 2.3× ahead |
| Echo RTT 64B | 25.0 µs | 26.2 µs | 1.05× ahead |
| Bulk 64KB writes | 3289 MB/s | 6581 MB/s | 0.50× behind |

Group commit delivered the small-message win: one writev now carries many
frames across many streams instead of two syscalls per frame plus a channel
round-trip. Bulk stayed behind because it was receive-bound — the reader
still allocated a fresh buffer per frame (66KB/op) — which M4 fixed.

## M2 baseline (correct core, no optimizations)

| Benchmark | mux | yamux | ratio |
|---|---|---|---|
| Bulk 64KB writes | 3161 MB/s | 6349 MB/s | 0.50× behind |
| Echo RTT 64B | 25.7 µs | 26.7 µs | 1.04× ahead |
| Stream open/close | 11.4 µs | 19.5 µs | 1.71× ahead |
| Bulk allocs | 65592 B/op | 1018 B/op | far behind |

M2 shipped none of the performance machinery: two `conn.Write` calls per DATA
frame and a fresh allocation per received frame. Stream churn was already
ahead on structure alone — zero-syscall open versus yamux's synchronous SYN.

## Two machines and a real NIC (the first numbers here that are not loopback)

Every measurement above this line ran both peers in one process, on loopback
or under netem. Loopback has no driver, no interrupt coalescing, no MTU, no
segmentation offload, and a round trip roughly forty times shorter than any
real path. This section is the first that crosses a network interface.

Client on darwin/arm64, server on linux/amd64 in GCP, `GOMAXPROCS=8` pinned on
both, 55ms round trip, both machines otherwise idle. Six passes over the whole
matrix, interleaved so every implementation meets the same link conditions
rather than each one owning its own window of time. The table reports the
median with the range behind it, because the ranges are wide enough here to
decide what may honestly be claimed: two configurations whose ranges overlap
have not been shown to differ. Bare TCP is measured too, since without a
ceiling there is no way to tell "the library is slow" from "the link is".

A LAN run between two hosts 1.2ms apart was attempted first and discarded: the
second host turned out to be a CI builder sitting at a load average between 90
and 175, and at that load the numbers describe the builder. It is recorded
here so the attempt is not repeated in the belief it was never made.

| scenario | portunus | yamux (default) | yamux (8MB window) | bare TCP |
|---|---|---|---|---|
| 4MB, one stream | 5.3 (4.6-5.6) | 3.0 (2.8-3.1) | 7.8 (6.8-8.2) | 9.1 (7.3-10.0) |
| 64MB, one stream | 19.0 (16.9-19.9) | 3.6 (3.4-3.7) | 19.0 (17.3-20.0) | 25.0 (22.2-26.9) |
| 8 streams, 8MB each | 24.4 (19.3-26.9) | 19.6 (4.9-20.2) | 23.8 (17.6-27.5) | 26.6 (11.2-36.1) |
| 64 streams, 1MB each | 22.2 (19.8-25.5) | 24.3 (20.9-29.2) | 22.3 (19.9-25.8) | 21.4 (14.6-23.1) |

Throughput in MB/s. Latency, from the same passes:

| measure | portunus | yamux (default) | yamux (8MB window) | bare TCP |
|---|---|---|---|---|
| 64B round trip, p50 | 39.2ms | 39.0ms | - | 39.0ms |
| under bulk load, p50 | 104ms | 48ms | 41ms | - |
| under bulk load, p99 | 207ms | 122ms | 381ms | - |
| under bulk load, requests/s | 8.8 | 17.7 | 7.9 | - |
| under bulk load, bulk rate | 15.9 MB/s | 15.1 | 15.5 | - |

**Autotuning arrives where hand-tuning would, without the hand.** On the 64MB
single-stream transfer portunus with no configuration reaches 5.3x stock
yamux. Against yamux hand-tuned to an 8MB window it is 19.0 against 19.0 —
identical medians on overlapping ranges. The claim worth making is not that it
wins but that it ties, having found the window itself. This is the case the
library was built for, and the one where a fixed default is most obviously
wrong: yamux's 256KB over a 55ms path cannot exceed 4.7 MB/s no matter how
fast the link is, and measured 3.6.

**And it charges for that on a short transfer.** The same comparison at 4MB
inverts: 5.3 against 7.8, ranges 4.6-5.6 and 6.8-8.2, so the gap is real
rather than noise. The receiver's own counters say why — the window reaches
2MB on the 4MB transfer and 8MB on the 64MB one. Starting at 256KB and
doubling once per round trip, 8MB takes five round trips, about 275ms here,
and a 4MB transfer is over before that completes. Anything sized in
single-digit megabytes on a long path spends most of its life ramping, and
pre-configuring the window beats autotuning there.

**Multi-stream is level, not a win.** At eight streams portunus leads on the
median, 24.4 against 19.6 and 23.8, but the ranges overlap and one yamux pass
dropped to 4.9; the separation is suggestive and not established. By 64
streams every implementation including bare TCP sits between 21 and 25: the
link is saturated and nothing about the multiplexer is visible. Worth noting
that yamux gains nothing from its larger window once eight streams are open,
because eight default windows already exceed this path's bandwidth-delay
product.

**Idle latency is the carrier's, not ours.** A 64-byte round trip costs 39.2ms
against bare TCP's 39.0ms. On a real path the multiplexer adds nothing
measurable to an unloaded round trip.

**Latency under load is the honest weakness, and the window is only half of
it.** Small requests sharing a session with four bulk streams see p50 rise to
104ms against 48ms for stock yamux, with the request rate halved. A first pass
here called this purely the window's fault and told operators to cap
MaxWindow; a follow-up sweep of portunus's own window, holding everything else
fixed, showed that was too simple and is corrected below.

Window size is a latency/throughput dial, and only below about 1MB. Sweeping
portunus's MaxWindow on the same 55ms path, three reps, median:

| MaxWindow | p50 | bulk load |
|---|---|---|
| 256KB | 50ms | 11 MB/s |
| 512KB | 69ms | 14 MB/s |
| 1MB | 106ms | 15 MB/s |
| 2MB | 96ms | 21 MB/s |
| 4MB | 86ms | 21 MB/s |
| 8MB | 111ms | 18 MB/s |
| 16MB | 105ms | 19 MB/s |

Two things the first pass got wrong. Capping the window does lower latency,
but not for free: at 256KB, p50 falls to 50ms and bulk load falls with it to
11 MB/s. The earlier claim that throughput was unchanged compared portunus at
its autotuned window against yamux at 256KB and mistook a cross-implementation
tie for a within-implementation one — portunus at 256KB moves 11 MB/s, not the
15 yamux moves there. And the effect saturates: from 2MB to 16MB, p50 and load
are both flat, so the 1-to-16MB range the first sweep tested showed nothing
and led to the wrong "window doesn't matter" conclusion before the sub-1MB
points were filled in.

What the window cannot explain is the rest of the gap. yamux at a 256KB window
reaches ~50ms p50 at ~15 MB/s; portunus at 256KB reaches the same latency but
only 11 MB/s — worse on the throughput axis at the same latency, at the same
window. That residual is not window depth, since the window is equal. The
leading suspect is sender-side scheduling — a small request waiting behind
bulk frames in group commit, and behind echoed bulk on the return path — which
would point at prioritizing small frames ahead of bulk in the batch as the
lever to try. That is a hypothesis the measurement here motivates but does not
yet confirm, and a later change to test. What is settled is the narrower
point: shrinking the window only trades one axis for the other, so it is not
the fix.

## Small-frame prioritization: a partial fix, and where the rest of the gap lives

The hypothesis above got built and measured. `Stream.SetPriority` (experimental)
routes a stream's DATA frames into a priority lane that the flusher writes
ahead of all bulk in every batch they share, so a small request is not
transmitted behind a batch's worth of another stream's bulk. The rrload
harness marks the request stream on both legs: the client on its send, the
server on its echo, which it classifies by message size the way a real proxy
would.

WAN path, 55ms, four bulk streams plus the request stream, five interleaved
reps, median:

| | p50 | p99 | requests/s | bulk load |
|---|---|---|---|---|
| priority off | 88.7ms | 225ms | 10.4 | 25.6 MB/s |
| priority on | 66.3ms | 220ms | 12.0 | 25.5 MB/s |

**p50 falls 25%, and unlike shrinking the window it costs no throughput** —
bulk load is 25.5 against 25.6, a tie, where dropping to a 256KB window to buy
the same latency would have halved it. Request rate rises 15%. This is a
strictly better operating point than the autotuned window alone: same
throughput, lower median latency.

**It does not close the gap to a small window, and the tail does not move.**
yamux at 256KB still reaches ~43ms p50; priority gets portunus to 66ms, not
there. And p99 is unchanged, 220 against 225. Both facts point at the same
cause: the priority lane reorders frames within *our* group-commit batch, but
once bytes are handed to the kernel they sit in TCP's send buffer and
congestion window ahead of anything enqueued after them. A large autotuned
window keeps ~1MB of bulk in flight on the shared connection, and a small
request handed to the kernel after it waits behind that on the wire no matter
what order we wrote it in. That is TCP head-of-line blocking on one flow — the
known ceiling this library documents as QUIC's genuine advantage — and no
userspace reordering reaches past it.

So the honest scope: prioritization is a real, throughput-free win on the
median for a session that mixes a latency-sensitive stream with bulk, and it is
worth having for exactly that shape of traffic. It is not a substitute for a
smaller window when the tail is what matters, and it cannot make a shared TCP
carrier behave like separate connections. Effect size also tracks how much
queue there is to reorder: on loopback, where the batch is the only queue, a
co-batched request's p99 roughly halved; on the fast low-RTT LAN, where the
batch drains before it builds, there was no measurable change.

**The window overshoots, and that costs memory rather than throughput.** This
path's true bandwidth-delay product is about 1MB; the estimator settles at
8MB and stops, because the utilization test stops being satisfied. The surplus
credit is never turned into data in flight, since TCP's own congestion control
remains the binding constraint. Memory stays bounded by MaxReceiveBudget,
which the 64-stream run confirms: 128MB granted, exactly the cap.

**What this harness cannot measure.** Stream open rate came out at 22.9/s for
every implementation, which is 1/RTT: the benchmark makes each open wait for a
reply, so on a 55ms path it measures the path and nothing else. The
zero-syscall open is a syscall-count property and only shows on loopback.

## Read buffer: the default was starving the receiver of syscalls

The two-machine harness made the receive path measurable for the first time. A
CPU profile of the server during a saturated single-stream transfer on the
10GbE LAN put 60% of the time in the socket-read syscall, 14% in scheduler
futex churn, 3% in memmove, and almost nothing in this library's own parsing
or dispatch. The receive side is one goroutine, and at these rates it is the
bottleneck, so where that goroutine spends its time is the whole question.

The read buffer defaulted to 16KB, smaller than a 64KB frame. That was
deliberate — a small buffer lets a large frame's payload go straight from the
kernel into its pooled segment with no intermediate copy — but it forces a read
syscall roughly per frame, over a hundred thousand a second at 15 Gbit/s. A
buffer spanning many frames reads them in one syscall and pays one extra copy
per payload instead. The profile said syscalls dominate; the measurement
confirmed it.

Single stream, 512MB, GOMAXPROCS=8, five interleaved reps per size so machine
drift is shared rather than attributed to one setting:

| read buffer | median Gbit/s | range |
|---|---|---|
| 16KB (old default) | 15.9 | 15.4-16.4 |
| 64KB | 16.4 | 16.1-17.3 |
| 128KB | 17.2 | 14.8-18.6 |
| 256KB (new default) | 17.6 | 17.0-20.0 |
| 512KB | 17.0 | 16.7-17.1 |
| 1MB | 15.7 | 15.5-17.3 |

The climb is monotonic to 256KB, about 11% over the old default, then flattens
into noise while the memory keeps growing. 256KB is the knee. The gain is
larger when the receive core is more contended — an earlier run on a busier
machine showed 13% — which is the regime a loaded tunnel server is actually in.

Verified not to cost elsewhere: on the 55ms WAN path, where a slow link rarely
buffers 256KB and the direct read never fired anyway, throughput was unchanged
(within noise of the 14 MB/s default), and 64-byte round-trip latency was
unaffected. The read buffer only ever mattered under bandwidth a quiet session
never reaches.

The cost is one buffer per session rather than per stream. A host terminating
thousands of mostly-idle sessions holds 256KB for each whether or not it ever
runs fast enough to benefit, and should lower it; the knob is
Config.ReadBufferSize.

## A note on windows

At M4 an experiment showed bulk throughput at 0.50× yamux with mux on its
256KB default versus yamux tuned to 16MB, and 1.85× **ahead** once both used
16MB. The implementation was never the bulk bottleneck at that point; the
flow-control budget was. Two conclusions:

1. Benchmarks must tune both sides or they measure configuration.
2. A 256KB default is too small for bulk transfer on a fast link, and picking
   a large default instead would waste memory on idle streams. That is the
   argument for BDP autotuning (M5) — the window should find its own size.


## Note: stream objects are not pooled, and the measurement says not to

DESIGN.md calls for reusing stream structs with a generation counter so that
use-after-close is a detectable error rather than pool corruption. It would
remove roughly half of what stream churn allocates: construction is 54% of
the eleven allocations per open-and-close cycle.

It was deferred once on risk, with the stated condition that the protocol
state fuzzer should first be trustworthy enough to act as a net. That
condition is now met — the fuzzer covers relay paths, in-flight writes,
graceful drain, and operations on already-closed streams, and passes 1.3M
interleavings — so the question was reopened and answered with a profile
instead.

**Allocation does not appear in the top forty CPU nodes of the churn
benchmark.** Background GC marking accounts for 0.58% and `mallocgc`,
`newobject`, and `makechan` do not register at all. Pooling stream structs
would therefore recover well under one percent of the time an open-and-close
cycle takes, because that cycle is dominated by the syscalls carrying its
frames, exactly like the bulk path.

That is not worth what it costs. Recycling an object the application still
holds a reference to is the classic use-after-free hazard, its mitigation is
a generation check on every exported method, and the failure it risks is
silent cross-stream corruption — one stream reading another's bytes. Every
lifecycle defect this package has actually shipped has been of that flavour.
Under one percent does not buy that risk.

Churn is already 2.8x faster than yamux with a third of the allocations. The
work is not deferred any longer; it is declined, with a number attached.
