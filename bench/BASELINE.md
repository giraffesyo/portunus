# Benchmark history

Go 1.26.1, darwin/arm64, loopback TCP, `-benchtime=2s`. **Both sides tuned**:
yamux at its 16MB max window with keepalive off, mux at the same 16MB window.
Comparing our default window against yamux's tuned one measures configuration,
not implementation — see "A note on windows" below.

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

## A note on windows

At M4 an experiment showed bulk throughput at 0.50× yamux with mux on its
256KB default versus yamux tuned to 16MB, and 1.85× **ahead** once both used
16MB. The implementation was never the bulk bottleneck at that point; the
flow-control budget was. Two conclusions:

1. Benchmarks must tune both sides or they measure configuration.
2. A 256KB default is too small for bulk transfer on a fast link, and picking
   a large default instead would waste memory on idle streams. That is the
   argument for BDP autotuning (M5) — the window should find its own size.
