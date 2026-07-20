# Benchmark history

Go 1.26.1, darwin/arm64, loopback TCP, `-benchtime=2s`. yamux tuned (16MB
max window, keepalive off), not defaulted.

## After M3 (group-commit batched writev)

| Benchmark            | mux        | yamux      | ratio          |
|----------------------|------------|------------|----------------|
| Small msgs, 64 streams | **150.6 ns/op** (1700 MB/s) | 4825 ns/op (53 MB/s) | **32× ahead** |
| Stream open/close    | 7.7 µs     | 17.6 µs    | **2.3× ahead** |
| Echo RTT 64B         | 25.0 µs    | 26.2 µs    | 1.05× ahead |
| Bulk 64KB writes     | 3289 MB/s  | 6581 MB/s  | **0.50× behind** |

Small-message throughput is where group commit was supposed to pay, and it
does: one writev now carries many frames across many streams instead of two
syscalls per frame plus a channel round-trip. The design target was ≥4×; the
measured result is 32×.

Bulk is unchanged because it is **receive-bound, not send-bound**: the reader
still allocates a fresh buffer per frame (66KB/op at 64KB frames) and copies
into it. M4's pooled size-classed segments and direct large-frame reads are
what move this number; the send side is already one writev per batch.

## M2 baseline (correct core, no optimizations)

| Benchmark            | mux         | yamux       | ratio         |
|----------------------|-------------|-------------|---------------|
| Bulk 64KB writes     | 3161 MB/s   | 6349 MB/s   | 0.50× behind  |
| Echo RTT 64B         | 25.7 µs     | 26.7 µs     | 1.04× ahead   |
| Stream open/close    | 11.4 µs     | 19.5 µs     | 1.71× ahead   |
| Bulk allocs          | 65592 B/op  | 1018 B/op   | far behind    |
| Churn allocs         | 1499 B/op, 16/op | 4191 B/op, 38/op | ahead |

M2 shipped none of the performance machinery: two `conn.Write` calls per DATA
frame and a fresh allocation per received frame. Stream churn was already
ahead on structure alone — zero-syscall open (SYN rides the first frame)
versus yamux's synchronous SYN.

## Gates

Every optimization must show a benchstat delta against the previous row.
DESIGN.md targets: ≥2× single-stream bulk, ≥4× many-stream small messages
(**met at M3**), 0 amortized allocations per frame at steady state.
