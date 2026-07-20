# mux

A stream multiplexer for Go: many logical streams over one reliable
byte-stream carrier (TCP, TLS, a Unix socket, an SSH channel — anything
satisfying `net.Conn`).

**Status: feature-complete, not yet released.** Everything through the
hardening pass is built and tested; the version has not been tagged and the
API may still move. See [DESIGN.md](DESIGN.md) for the architecture and
[bench/BASELINE.md](bench/BASELINE.md) for measured numbers, including what
does not yet win.

Against tuned yamux on loopback TCP: **2.9× on relay** (the proxy workload),
**17× on many-stream small messages**, **1.8× on single-stream bulk**, 2.3×
on stream churn, and — on a mixed workload — **1.5× lower small-message
latency while carrying 1.65× more bulk**. On a high-RTT path, autotuning
reaches 2.8× yamux's shipped configuration but still trails a hand-tuned
static window, an open problem documented in the baseline.

- **Zero dependencies** outside the standard library, forever. Benchmarks and
  the QUIC adapter live in separate modules so `go get` stays clean.
- **Streams are `net.Conn`s**, so they drop into existing code — plus the
  QUIC-shaped controls `net.Conn` lacks: half-close, and read/write
  cancellation carrying an application error code.
- **Zero-syscall stream open**: `OpenStream` is purely local; a stream
  announces itself on its first frame.
- **One API over two transports**: the same `Session` and `Stream` interfaces
  are satisfied by the native TCP session and by the QUIC adapter, and a
  shared conformance suite runs against both, so they behave the same and not
  merely compile the same.

```go
sess, err := mux.Client(conn, nil)
st, err := sess.OpenStream(ctx)

st.Write(request)
st.CloseWrite()              // peer sees EOF; we can still read the reply
io.Copy(os.Stdout, st)
```

Server side:

```go
sess, err := mux.Server(conn, nil)
for {
    st, err := sess.AcceptStream(ctx)
    if err != nil {
        return err
    }
    go handle(st)            // st is a net.Conn
}
```

## Why another multiplexer

yamux is the incumbent and it is beatable on structure, not micro-tuning: it
does a channel round-trip and an allocation per frame, two syscalls per data
frame with no cross-stream batching, a copy into a growing `bytes.Buffer` on
receive, and a fixed 256KB window with no BDP awareness. On a 200ms path that
last one alone caps a stream near 1.3 MB/s regardless of how fast the link is.

This library targets those four things directly: group-commit batching into a
single `writev` across streams, pooled segments with a zero-copy relay path,
wakeup and allocation elimination on the hot paths, and BDP-autotuned windows.
Targets are ≥2× single-stream bulk, ≥4× many-stream small messages, and zero
amortized allocations per frame at steady state — each one gated by a
benchmark against tuned (not default) baselines.

## Choosing a transport

| You need | Use |
|---|---|
| Streams over an existing byte stream (TCP, TLS, a Unix socket, an SSH channel) | `mux.Client` / `mux.Server` |
| No head-of-line blocking, and UDP reaches your peer | `adapters/quic` |
| An on-path L7 proxy to understand your traffic | HTTP/2 or WebSocket, not this |

The first two are the same API — the `Session` and `Stream` interfaces, with
a shared conformance suite run against both — so code can be written once and
the transport chosen at the edges.

The third is a real limitation, not a hedge. A custom binary wire format is
safe precisely *because* nothing on the path parses it: it rides under TLS as
opaque bytes. That is also why nothing on the path can route, inspect, or
load-balance it. Kubernetes moved its streaming from SPDY to WebSockets for
exactly this reason. If a proxy in the middle has to terminate and understand
your protocol, use one it already speaks.

Where UDP is blocked but you still want QUIC semantics, this library over a
TLS carrier is the pragmatic answer — the stream API is deliberately the same
shape, so the migration is a constructor change.

## What's honest about it

- **TCP head-of-line blocking is not solved.** One lost packet stalls every
  stream on the carrier. That is physics, and it is QUIC's genuine advantage;
  no userspace mux fixes it. The QUIC adapter exists for when you need that.
- **A session's receive side is one goroutine**, so it is bounded by one
  core's parse-and-copy throughput. Run multiple sessions to scale past it.
- **The wire format is clean-slate**, so both ends must run this library. It
  is not yamux-compatible and never will be.
- Prefer HTTP/2 or WebSocket when an on-path L7 proxy has to parse your
  traffic. A custom binary mux is safe precisely because nothing on-path
  parses it — which is also why nothing on-path can route it.

## Status by milestone

| Milestone | What | State |
|-----------|------|-------|
| M1 | Wire format, frame codec, fuzz targets | done |
| M2 | Correct core: lifecycle, flow control, receiver rules, tests | done |
| M3 | Group commit: batched writev send path | done |
| M4 | Receive fast paths: segment pools, zero-copy relay | done |
| M5 | BDP-autotuned flow control, keepalive | done |
| M6 | Performance targets, socket tuning, mixed-workload fairness | done |
| M7 | QUIC adapter (`adapters/quic`) + shared conformance suite | done |
| M8 | Fuzzing, soak, hardening, docs | done |

## Development

```bash
go test -race ./...                  # correctness, race-clean
go test ./internal/frame -fuzz=Fuzz  # wire-format fuzzing
cd bench && go test -bench=. .       # vs tuned yamux
```

## License

TBD.
