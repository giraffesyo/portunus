# M2 baseline (pre-optimization)

Recorded 2026-07-20 after M2 (correct, unbatched core). Go 1.26.1,
darwin/arm64, loopback TCP, `-benchtime=2s`. yamux tuned (16MB max window,
keepalive off), not defaulted.

| Benchmark            | mux         | yamux       | ratio         |
|----------------------|-------------|-------------|---------------|
| Bulk 64KB writes     | 3161 MB/s   | 6349 MB/s   | **0.50× (behind)** |
| Echo RTT 64B         | 25.7 µs     | 26.7 µs     | 1.04× |
| Stream open/close    | 11.4 µs     | 19.5 µs     | **1.71× (ahead)** |
| Bulk allocs          | 65592 B/op  | 1018 B/op   | far behind |
| Churn allocs         | 1499 B/op, 16/op | 4191 B/op, 38/op | ahead |

## Reading this

Losing on bulk is expected and diagnostic, not alarming — M2 deliberately
ships none of the performance machinery:

- **Two `conn.Write` calls per DATA frame** (header, then payload), the exact
  cost the design faults yamux for. M3's group commit replaces this with one
  batched writev across streams.
- **A fresh payload allocation per received frame** (the 64KB/op): the reader
  does `make([]byte, h.Length)` per frame. M4's pooled size-classed segments
  and direct large-frame reads remove it.
- yamux's bulk number benefits from a `bytes.Buffer` receive path that
  amortizes into an already-sized buffer; our per-frame allocation is
  strictly worse until the pool lands.

Already ahead where the design's structural choices pay off with no
optimization at all:

- **Stream churn 1.71× faster with 2.4× fewer allocations** — zero-syscall
  open (SYN rides the first DATA frame) versus yamux's synchronous SYN
  round-trip.
- Echo RTT at parity, where loopback network cost dominates both.

## Gate for M3/M4

These numbers are the bisection baseline. Each optimization must show a
benchstat delta against this table, and the targets from DESIGN.md
(≥2× bulk, ≥4× many-stream small messages, 0 amortized allocs/frame) are
measured from here.
