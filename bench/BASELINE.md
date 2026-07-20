# Benchmark history

Go 1.26.1, darwin/arm64, loopback TCP, `-benchtime=2s`. **Both sides tuned**:
yamux at its 16MB max window with keepalive off, mux at the same 16MB window.
Comparing our default window against yamux's tuned one measures configuration,
not implementation — see "A note on windows" below.

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
