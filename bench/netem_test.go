//go:build linux

package bench

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/giraffesyo/mux"
	"github.com/hashicorp/yamux"
)

// Benchmarks over a kernel-shaped path.
//
// The in-process delay harness in wan_test.go is a poor model: it has no
// bandwidth limit, so its results are dominated by chunk granularity rather
// than by the protocol, and its absolute numbers should not be trusted. A
// netem qdisc on the loopback interface shapes delay, loss, and rate in the
// kernel, which is the closest thing to a real long path that fits in CI.
//
// Set up before running (root required):
//
//	tc qdisc add dev lo root netem delay 100ms rate 100mbit
//	MUX_NETEM=1 go test -bench=Netem -benchtime=5x ./bench
//	tc qdisc del dev lo root
//
// The benchmarks skip unless MUX_NETEM is set, so an unshaped machine reports
// a skip rather than a meaningless number that looks like a measurement.
const netemEnv = "MUX_NETEM"

func requireNetem(b *testing.B) {
	b.Helper()
	if os.Getenv(netemEnv) == "" {
		b.Skipf("set %s=1 with a netem qdisc on lo; see netem_test.go", netemEnv)
	}
}

const netemBytes = 8 << 20

// BenchmarkNetemMux measures a single stream with autotuning from the 64KB
// protocol floor: every byte of window has to be earned from the path.
func BenchmarkNetemMux(b *testing.B) {
	requireNetem(b)
	a, z := tcpPair(b)
	cfg := &mux.Config{
		InitialWindow:     64 << 10,
		MaxWindow:         64 << 20,
		KeepaliveInterval: 100 * time.Millisecond,
		KeepaliveTimeout:  60 * time.Second,
	}
	ctx := context.Background()

	type res struct {
		s   *mux.NativeSession
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := mux.Server(z, cfg)
		ch <- res{s, err}
	}()
	cs, err := mux.Client(a, cfg)
	if err != nil {
		b.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		b.Fatal(r.err)
	}
	defer cs.Close()
	defer r.s.Close()

	go func() {
		for {
			st, err := r.s.AcceptStream(ctx)
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	st, err := cs.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(netemBytes)
	b.ResetTimer()
	for b.Loop() {
		for sent := 0; sent < netemBytes; sent += len(buf) {
			if _, err := st.Write(buf); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	// The window autotuning actually reached, which is the point of the run.
	b.ReportMetric(float64(r.s.Stats().WindowTarget)/1024, "windowKB")
	b.ReportMetric(float64(r.s.Stats().RTT.Microseconds()), "rtt-us")
}

// BenchmarkNetemYamuxDefault is yamux as shipped: a fixed 256KB window, the
// configuration real deployments are running.
func BenchmarkNetemYamuxDefault(b *testing.B) { benchNetemYamux(b, 256*1024) }

// BenchmarkNetemYamuxTuned is the best a static window can do, given an
// operator who knows the path in advance.
func BenchmarkNetemYamuxTuned(b *testing.B) { benchNetemYamux(b, 64*1024*1024) }

func benchNetemYamux(b *testing.B, window uint32) {
	requireNetem(b)
	a, z := tcpPair(b)
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.MaxStreamWindowSize = window
	cfg.LogOutput = io.Discard

	type res struct {
		s   *yamux.Session
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := yamux.Server(z, cfg)
		ch <- res{s, err}
	}()
	cs, err := yamux.Client(a, cfg)
	if err != nil {
		b.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		b.Fatal(r.err)
	}
	defer cs.Close()
	defer r.s.Close()

	go func() {
		for {
			st, err := r.s.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	st, err := cs.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(netemBytes)
	b.ResetTimer()
	for b.Loop() {
		for sent := 0; sent < netemBytes; sent += len(buf) {
			if _, err := st.Write(buf); err != nil {
				b.Fatal(err)
			}
		}
	}
}
