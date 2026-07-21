// Command remote benchmarks a multiplexer between two machines over a real
// network interface.
//
// Every other benchmark in this repository runs both peers in one process, on
// loopback or under netem. That measures the library but not the system it
// runs on: loopback has no driver, no interrupt coalescing, no MTU, no
// segmentation offload, and a round trip roughly forty times shorter than a
// datacenter LAN. Effects that only appear on a real NIC — a batch that no
// longer fits one segment, a window too small for the true bandwidth-delay
// product, a syscall rate the loopback path never charged for — are invisible
// there.
//
// Run one process per machine:
//
//	server$ remote -mode server -listen :7777
//	client$ remote -mode client -addr server:7777 -impl portunus -scenario bulk
//
// One server serves every implementation and scenario: the client states what
// it wants in a fixed-size preamble before either side starts speaking a
// multiplexer, so a whole matrix runs against a single long-lived server.
//
// Results are printed as one JSON object per run, so a driver script can
// collect them without parsing prose.
package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/giraffesyo/portunus"
	"github.com/hashicorp/yamux"
)

// preambleSize is fixed and read with io.ReadFull rather than delimited and
// read through a bufio.Reader. Buffering would consume bytes past the
// delimiter, and those bytes are the peer's first multiplexer frame.
//
// Oversized preambles are rejected rather than truncated, so a field added
// later fails at the first run instead of silently arriving as zero.
const preambleSize = 128

type preamble struct {
	Impl        string `json:"impl"`
	Scenario    string `json:"scenario"`
	Streams     int    `json:"streams"`
	Size        int    `json:"size"`
	YamuxWindow int    `json:"yamux_window"`
}

func main() {
	var (
		mode      = flag.String("mode", "client", "server or client")
		listen    = flag.String("listen", ":7777", "server listen address")
		addr      = flag.String("addr", "", "server address to dial")
		impl      = flag.String("impl", "portunus", "portunus, yamux, or raw")
		scenario  = flag.String("scenario", "bulk", "bulk, rr, rrload, or open")
		streams   = flag.Int("streams", 1, "concurrent streams")
		bytesFlag = flag.Int64("bytes", 256<<20, "payload bytes per stream for bulk")
		size      = flag.Int("size", 64, "request size in bytes for rr")
		count     = flag.Int("count", 2000, "round trips for rr, or opens for open")
		ywin      = flag.Int("yamux-window", 256<<10, "yamux receive window")
		procs     = flag.Int("procs", 0, "GOMAXPROCS; 0 leaves it alone")
		label     = flag.String("label", "", "free-form label copied into the result")
	)
	flag.Parse()

	// The two machines need not have the same core count, and throughput
	// numbers are not comparable across different values, so pin both.
	if *procs > 0 {
		runtime.GOMAXPROCS(*procs)
	}

	switch *mode {
	case "server":
		if err := runServer(*listen); err != nil {
			log.Fatal(err)
		}
	case "client":
		if *addr == "" {
			log.Fatal("client mode needs -addr")
		}
		p := preamble{
			Impl: *impl, Scenario: *scenario,
			Streams: *streams, Size: *size, YamuxWindow: *ywin,
		}
		res, err := runClient(*addr, p, *bytesFlag, *count)
		if err != nil {
			log.Fatal(err)
		}
		res.Label = *label
		res.Procs = runtime.GOMAXPROCS(0)
		if *impl == "yamux" {
			res.YamuxWindow = *ywin
		}
		enc := json.NewEncoder(os.Stdout)
		if err := enc.Encode(res); err != nil {
			log.Fatal(err)
		}
	case "aggregate":
		if err := aggregate(os.Stdin, os.Stdout); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown mode %q", *mode)
	}
}

// ---------------------------------------------------------------- aggregate

// aggregate turns the raw result stream into one row per configuration,
// reporting the median of the repeats.
//
// The median rather than the best: these machines are shared, and the best of
// N is a measure of how quiet the host happened to be, not of the library.
func aggregate(in io.Reader, out io.Writer) error {
	type key struct {
		label  string
		impl   string
		window int
	}
	groups := map[key][]Result{}
	var order []key

	dec := json.NewDecoder(in)
	for {
		var r Result
		if err := dec.Decode(&r); err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		if r.Scenario == "" {
			continue // a failed run, recorded so the gap is visible
		}
		k := key{r.Label, r.Impl, r.YamuxWindow}
		if _, seen := groups[k]; !seen {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}

	fmt.Fprintf(out, "| scenario | impl | runs | MB/s | Gbit/s | ops/s | p50 us | p99 us | load MB/s |\n")
	fmt.Fprintf(out, "|---|---|---|---|---|---|---|---|---|\n")
	for _, k := range order {
		g := groups[k]
		name := k.impl
		if k.window > 0 {
			name = fmt.Sprintf("%s (%dKB win)", k.impl, k.window/1024)
		}
		fmt.Fprintf(out, "| %s | %s | %d | %s | %s | %s | %s | %s | %s |\n",
			k.label, name, len(g),
			med(g, func(r Result) float64 { return r.MBPerSec }),
			med(g, func(r Result) float64 { return r.GbitPerSec }),
			med(g, func(r Result) float64 { return r.OpsPerSec }),
			med(g, func(r Result) float64 { return r.P50us }),
			med(g, func(r Result) float64 { return r.P99us }),
			med(g, func(r Result) float64 { return r.LoadMBPerSec }),
		)
	}
	return nil
}

// med returns the median of one field, or a dash when the scenario did not
// measure it. An omitted metric is left blank rather than printed as zero,
// which would read as "measured, and it was nothing".
func med(g []Result, f func(Result) float64) string {
	vals := make([]float64, 0, len(g))
	for _, r := range g {
		if v := f(r); v != 0 {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return "-"
	}
	sort.Float64s(vals)
	return fmt.Sprintf("%.1f", vals[len(vals)/2])
}

// ---------------------------------------------------------------- transport

// A session is the little of a multiplexer this benchmark needs, so that
// portunus, yamux, and bare TCP can be driven by one body of measurement code
// instead of three near-copies that could drift apart and quietly measure
// different things.
type session interface {
	Open(ctx context.Context) (stream, error)
	Accept(ctx context.Context) (stream, error)
	Close() error
}

// stream adds the half-close that bulk transfer needs to signal "that was all
// of it" without tearing down the reverse direction.
//
// The three implementations spell it differently, and getting this wrong
// would silently bias the comparison: portunus has an explicit CloseWrite and
// reserves Close for tearing down both directions, whereas yamux's Close is
// itself a half-close. Calling Close on a portunus stream where yamux gets a
// half-close would charge portunus for a reset frame yamux never sends.
type stream interface {
	io.ReadWriteCloser
	CloseWrite() error
}

type portunusSession struct{ s *portunus.NativeSession }

// portunus.Stream already has Read, Write, Close, and CloseWrite.
func (p portunusSession) Open(ctx context.Context) (stream, error) {
	return p.s.OpenStream(ctx)
}
func (p portunusSession) Accept(ctx context.Context) (stream, error) {
	return p.s.AcceptStream(ctx)
}
func (p portunusSession) Close() error { return p.s.Close() }

type yamuxSession struct{ s *yamux.Session }

func (y yamuxSession) Open(context.Context) (stream, error) {
	st, err := y.s.OpenStream()
	return yamuxStream{st}, err
}
func (y yamuxSession) Accept(context.Context) (stream, error) {
	st, err := y.s.AcceptStream()
	return yamuxStream{st}, err
}
func (y yamuxSession) Close() error { return y.s.Close() }

// yamuxStream maps the half-close onto Close, which is what yamux's Close
// does from an established state: it sends FIN and leaves the stream
// readable.
type yamuxStream struct{ *yamux.Stream }

func (y yamuxStream) CloseWrite() error { return y.Stream.Close() }

// rawSession carries exactly one stream: the TCP connection itself. It is the
// ceiling every multiplexer is measured against, and the only way to tell
// "the library is slow" apart from "the network is".
type rawSession struct {
	c    net.Conn
	once sync.Once
	used chan struct{}
}

func newRaw(c net.Conn) *rawSession {
	r := &rawSession{c: c, used: make(chan struct{}, 1)}
	r.used <- struct{}{}
	return r
}

func (r *rawSession) take() (stream, error) {
	select {
	case <-r.used:
		return rawStream{r.c}, nil
	default:
		return nil, fmt.Errorf("raw carries one stream per connection")
	}
}
func (r *rawSession) Open(context.Context) (stream, error)   { return r.take() }
func (r *rawSession) Accept(context.Context) (stream, error) { return r.take() }
func (r *rawSession) Close() error {
	r.once.Do(func() { r.c.Close() })
	return nil
}

// rawStream is the TCP connection presented as a stream. Its Close is a no-op
// because the connection belongs to the enclosing session, which matches what
// closing one multiplexed stream does to its carrier.
type rawStream struct{ c net.Conn }

func (r rawStream) Read(p []byte) (int, error)  { return r.c.Read(p) }
func (r rawStream) Write(p []byte) (int, error) { return r.c.Write(p) }
func (r rawStream) Close() error                { return nil }
func (r rawStream) CloseWrite() error {
	if c, ok := r.c.(*net.TCPConn); ok {
		return c.CloseWrite()
	}
	return nil
}

func portunusConfig() *portunus.Config {
	// Deliberately the shipped defaults. A benchmark that hand-tunes the
	// library it is promoting and not the one it compares against is a
	// press release.
	return nil
}

func dialSession(c net.Conn, p preamble) (session, error) {
	switch p.Impl {
	case "portunus":
		s, err := portunus.Client(c, portunusConfig())
		return portunusSession{s}, err
	case "yamux":
		cfg := yamuxConfig(p.YamuxWindow)
		s, err := yamux.Client(c, cfg)
		return yamuxSession{s}, err
	case "raw":
		return newRaw(c), nil
	}
	return nil, fmt.Errorf("unknown impl %q", p.Impl)
}

func serveSession(c net.Conn, p preamble) (session, error) {
	switch p.Impl {
	case "portunus":
		s, err := portunus.Server(c, portunusConfig())
		return portunusSession{s}, err
	case "yamux":
		cfg := yamuxConfig(p.YamuxWindow)
		s, err := yamux.Server(c, cfg)
		return yamuxSession{s}, err
	case "raw":
		return newRaw(c), nil
	}
	return nil, fmt.Errorf("unknown impl %q", p.Impl)
}

func yamuxConfig(window int) *yamux.Config {
	cfg := yamux.DefaultConfig()
	if window > 0 {
		cfg.MaxStreamWindowSize = uint32(window)
	}
	// yamux logs to stderr by default, which would interleave with results.
	cfg.LogOutput = io.Discard
	// Match portunus, whose keepalive default is also on but which does not
	// fail a session for a slow response under full load.
	cfg.ConnectionWriteTimeout = 30 * time.Second
	return cfg
}

// ------------------------------------------------------------------- server

func runServer(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	log.Printf("listening on %s with GOMAXPROCS=%d", ln.Addr(), runtime.GOMAXPROCS(0))
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		go func() {
			if err := handle(c); err != nil && err != io.EOF {
				log.Printf("connection from %s: %v", c.RemoteAddr(), err)
			}
		}()
	}
}

func handle(c net.Conn) error {
	defer c.Close()

	buf := make([]byte, preambleSize)
	// A stalled or port-scanning peer must not hold a slot forever, but the
	// deadline has to be cleared before the multiplexer starts: a deadline
	// left on the carrier would expire mid-benchmark and look like a
	// protocol failure.
	c.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(c, buf); err != nil {
		return err
	}
	c.SetReadDeadline(time.Time{})

	var p preamble
	if err := json.Unmarshal(trimNul(buf), &p); err != nil {
		return fmt.Errorf("bad preamble: %w", err)
	}

	sess, err := serveSession(c, p)
	if err != nil {
		return err
	}
	defer sess.Close()

	ctx := context.Background()
	switch p.Scenario {
	case "bulk":
		return serveBulk(ctx, sess, p.Streams)
	case "rr", "open":
		return serveEcho(ctx, sess)
	case "rrload":
		return serveEcho(ctx, sess)
	}
	return fmt.Errorf("unknown scenario %q", p.Scenario)
}

// serveBulk drains each stream and reports the byte count back, so the client
// times delivery rather than the speed at which it can fill a local buffer.
func serveBulk(ctx context.Context, sess session, streams int) error {
	var wg sync.WaitGroup
	errs := make(chan error, streams)
	for range streams {
		st, err := sess.Accept(ctx)
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			n, err := io.Copy(io.Discard, st)
			if err != nil {
				errs <- err
				return
			}
			var ack [8]byte
			binary.BigEndian.PutUint64(ack[:], uint64(n))
			if _, err := st.Write(ack[:]); err != nil {
				errs <- err
			}
			st.CloseWrite()
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}

	// Writing the last ack is not the same as delivering it. Closing the
	// session here would discard frames still queued, and the client would
	// report a broken carrier instead of a result. Wait for the client to
	// hang up, which it does only after every ack has arrived.
	awaitPeerHangup(ctx, sess)
	return nil
}

// serveEcho answers every stream for as long as the client keeps them open.
func serveEcho(ctx context.Context, sess session) error {
	var wg sync.WaitGroup
	for {
		st, err := sess.Accept(ctx)
		if err != nil {
			// For a multiplexer this means the session ended. For bare TCP
			// it means only that the one stream a connection carries is
			// already handed out, so the echo it is running must be allowed
			// to finish rather than being closed out from under itself.
			wg.Wait()
			return nil
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer st.Close()
			io.Copy(st, st)
		}()
	}
}

// awaitPeerHangup blocks until the peer ends the session, using the only
// signal available through the session interface.
func awaitPeerHangup(ctx context.Context, sess session) {
	for {
		if _, err := sess.Accept(ctx); err != nil {
			return
		}
	}
}

// ------------------------------------------------------------------- client

// Result is one benchmark run. Absent fields are those the scenario does not
// measure.
type Result struct {
	Label    string `json:"label,omitempty"`
	Impl     string `json:"impl"`
	Scenario string `json:"scenario"`
	Streams  int    `json:"streams"`
	Procs    int    `json:"procs"`
	// YamuxWindow distinguishes stock yamux from a hand-tuned one, which
	// otherwise produce identical-looking rows.
	YamuxWindow int     `json:"yamux_window,omitempty"`
	Bytes       int64   `json:"bytes,omitempty"`
	Seconds     float64 `json:"seconds"`
	MBPerSec    float64 `json:"mb_per_sec,omitempty"`
	GbitPerSec  float64 `json:"gbit_per_sec,omitempty"`
	OpsPerSec   float64 `json:"ops_per_sec,omitempty"`
	P50us       float64 `json:"p50_us,omitempty"`
	P99us       float64 `json:"p99_us,omitempty"`
	MaxUs       float64 `json:"max_us,omitempty"`
	// LoadMBPerSec is the throughput of the background transfer during a
	// latency-under-load run: latency numbers mean nothing without knowing
	// how loaded the link actually was.
	LoadMBPerSec float64 `json:"load_mb_per_sec,omitempty"`
}

func runClient(addr string, p preamble, payload int64, count int) (Result, error) {
	res := Result{Impl: p.Impl, Scenario: p.Scenario, Streams: p.Streams}

	// raw has no multiplexing, so a multi-stream run means multiple
	// connections. Every other scenario is single-session.
	if p.Impl == "raw" && p.Scenario == "bulk" && p.Streams > 1 {
		return runRawBulkFanout(addr, p, payload)
	}

	if p.Impl == "raw" && (p.Scenario == "open" || p.Scenario == "rrload") {
		return res, fmt.Errorf("scenario %q needs multiplexing, which raw has none of", p.Scenario)
	}

	sess, err := dial(addr, p)
	if err != nil {
		return res, err
	}
	defer sess.Close()

	ctx := context.Background()
	switch p.Scenario {
	case "bulk":
		return clientBulk(ctx, sess, p, payload)
	case "rr":
		return clientRR(ctx, sess, p, count)
	case "rrload":
		return clientRRLoad(ctx, sess, p, payload, count)
	case "open":
		return clientOpen(ctx, sess, p, count)
	}
	return res, fmt.Errorf("unknown scenario %q", p.Scenario)
}

func dial(addr string, p preamble) (session, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	enc, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	if len(enc) > preambleSize {
		return nil, fmt.Errorf("preamble of %d bytes exceeds %d", len(enc), preambleSize)
	}
	buf := make([]byte, preambleSize)
	copy(buf, enc)
	if _, err := c.Write(buf); err != nil {
		return nil, err
	}
	return dialSession(c, p)
}

func clientBulk(ctx context.Context, sess session, p preamble, payload int64) (Result, error) {
	res := Result{Impl: p.Impl, Scenario: "bulk", Streams: p.Streams}

	// One shared buffer: the point is the network, and giving each stream
	// its own megabytes would measure the allocator too.
	block := make([]byte, 1<<20)
	for i := range block {
		block[i] = byte(i)
	}

	var wg sync.WaitGroup
	errs := make(chan error, p.Streams)
	start := time.Now()
	for range p.Streams {
		st, err := sess.Open(ctx)
		if err != nil {
			return res, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sendAndConfirm(st, block, payload); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	if err := <-errs; err != nil {
		return res, err
	}

	total := payload * int64(p.Streams)
	fillRate(&res, total, elapsed)
	return res, nil
}

// sendAndConfirm writes payload bytes, half-closes, and waits for the peer's
// count. Timing the write alone would report how fast the kernel accepted
// bytes into a socket buffer, which on a fast NIC is most of a short run.
func sendAndConfirm(st stream, block []byte, payload int64) error {
	remaining := payload
	for remaining > 0 {
		n := int64(len(block))
		if n > remaining {
			n = remaining
		}
		if _, err := st.Write(block[:n]); err != nil {
			return err
		}
		remaining -= n
	}
	if err := st.CloseWrite(); err != nil { // peer sees EOF, we can still read
		return err
	}
	var ack [8]byte
	if _, err := io.ReadFull(st, ack[:]); err != nil {
		return fmt.Errorf("reading delivery confirmation: %w", err)
	}
	if got := int64(binary.BigEndian.Uint64(ack[:])); got != payload {
		return fmt.Errorf("peer received %d bytes, sent %d", got, payload)
	}
	return nil
}

func clientRR(ctx context.Context, sess session, p preamble, count int) (Result, error) {
	res := Result{Impl: p.Impl, Scenario: "rr", Streams: p.Streams}
	lat, elapsed, err := roundTrips(ctx, sess, p, count, nil)
	if err != nil {
		return res, err
	}
	res.Seconds = elapsed.Seconds()
	res.OpsPerSec = float64(len(lat)) / elapsed.Seconds()
	fillLatency(&res, lat)
	return res, nil
}

// clientRRLoad measures small-request latency while bulk transfers saturate
// the same session. A multiplexer that is fast at bulk and fast at latency
// separately can still be useless at both together, which is the case a
// tunnel actually runs in.
func clientRRLoad(ctx context.Context, sess session, p preamble, payload int64, count int) (Result, error) {
	res := Result{Impl: p.Impl, Scenario: "rrload", Streams: p.Streams}

	block := make([]byte, 1<<20)
	stop := make(chan struct{})
	var loadBytes int64
	var loadMu sync.Mutex
	var wg sync.WaitGroup

	// Background load on its own streams, echoed back by the server.
	for range p.Streams {
		st, err := sess.Open(ctx)
		if err != nil {
			return res, err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer st.Close()
			go io.Copy(io.Discard, st)
			var sent int64
			for sent < payload {
				select {
				case <-stop:
				default:
					if _, err := st.Write(block); err != nil {
						return
					}
					sent += int64(len(block))
					continue
				}
				break
			}
			loadMu.Lock()
			loadBytes += sent
			loadMu.Unlock()
		}()
	}

	loadStart := time.Now()
	lat, elapsed, err := roundTrips(ctx, sess, p, count, nil)
	close(stop)
	wg.Wait()
	loadElapsed := time.Since(loadStart)
	if err != nil {
		return res, err
	}

	res.Seconds = elapsed.Seconds()
	res.OpsPerSec = float64(len(lat)) / elapsed.Seconds()
	res.LoadMBPerSec = float64(loadBytes) / (1 << 20) / loadElapsed.Seconds()
	fillLatency(&res, lat)
	return res, nil
}

// roundTrips runs count request/response exchanges on a single stream and
// returns each one's duration.
func roundTrips(ctx context.Context, sess session, p preamble, count int, _ []byte) ([]time.Duration, time.Duration, error) {
	st, err := sess.Open(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer st.Close()

	req := make([]byte, p.Size)
	resp := make([]byte, p.Size)
	lat := make([]time.Duration, 0, count)

	// A few unmeasured exchanges first: the first request on a fresh stream
	// pays for its announcement and for TCP's initial congestion window, and
	// folding that into the distribution would misreport the steady state.
	for range 10 {
		if _, err := st.Write(req); err != nil {
			return nil, 0, err
		}
		if _, err := io.ReadFull(st, resp); err != nil {
			return nil, 0, err
		}
	}

	start := time.Now()
	for range count {
		t0 := time.Now()
		if _, err := st.Write(req); err != nil {
			return nil, 0, err
		}
		if _, err := io.ReadFull(st, resp); err != nil {
			return nil, 0, err
		}
		lat = append(lat, time.Since(t0))
	}
	return lat, time.Since(start), nil
}

// clientOpen measures how fast streams can be created and used, which is the
// cost a proxy pays per connection it accepts.
func clientOpen(ctx context.Context, sess session, p preamble, count int) (Result, error) {
	res := Result{Impl: p.Impl, Scenario: "open", Streams: p.Streams}
	req := make([]byte, p.Size)
	resp := make([]byte, p.Size)

	start := time.Now()
	for range count {
		st, err := sess.Open(ctx)
		if err != nil {
			return res, err
		}
		// Open and never use is not a fair measure: portunus deliberately
		// sends nothing until the first write, so the syscall it saves would
		// show up as an unrealistic win.
		if _, err := st.Write(req); err != nil {
			return res, err
		}
		if _, err := io.ReadFull(st, resp); err != nil {
			return res, err
		}
		// Half-close and drain off the measured path: the peer needs the EOF
		// to release its side, but making each iteration wait for that would
		// turn an open-rate benchmark into a round-trip one.
		st.CloseWrite()
		go io.Copy(io.Discard, st)
	}
	elapsed := time.Since(start)
	res.Seconds = elapsed.Seconds()
	res.OpsPerSec = float64(count) / elapsed.Seconds()
	return res, nil
}

// runRawBulkFanout gives bare TCP the same aggregate work as a multi-stream
// multiplexed run, using one connection per stream.
func runRawBulkFanout(addr string, p preamble, payload int64) (Result, error) {
	res := Result{Impl: p.Impl, Scenario: "bulk", Streams: p.Streams}
	block := make([]byte, 1<<20)

	single := p
	single.Streams = 1

	sessions := make([]session, 0, p.Streams)
	defer func() {
		for _, s := range sessions {
			s.Close()
		}
	}()
	streams := make([]stream, 0, p.Streams)
	for range p.Streams {
		s, err := dial(addr, single)
		if err != nil {
			return res, err
		}
		sessions = append(sessions, s)
		st, err := s.Open(context.Background())
		if err != nil {
			return res, err
		}
		streams = append(streams, st)
	}

	var wg sync.WaitGroup
	errs := make(chan error, p.Streams)
	start := time.Now()
	for _, st := range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sendAndConfirm(st, block, payload); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	if err := <-errs; err != nil {
		return res, err
	}

	fillRate(&res, payload*int64(p.Streams), elapsed)
	return res, nil
}

// ------------------------------------------------------------------ helpers

func fillRate(res *Result, total int64, elapsed time.Duration) {
	res.Bytes = total
	res.Seconds = elapsed.Seconds()
	res.MBPerSec = float64(total) / (1 << 20) / elapsed.Seconds()
	res.GbitPerSec = float64(total) * 8 / 1e9 / elapsed.Seconds()
}

func fillLatency(res *Result, lat []time.Duration) {
	if len(lat) == 0 {
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	us := func(d time.Duration) float64 { return float64(d.Nanoseconds()) / 1000 }
	res.P50us = us(lat[len(lat)*50/100])
	res.P99us = us(lat[min(len(lat)*99/100, len(lat)-1)])
	res.MaxUs = us(lat[len(lat)-1])
}

func trimNul(b []byte) []byte {
	for i, c := range b {
		if c == 0 {
			return b[:i]
		}
	}
	return b
}
