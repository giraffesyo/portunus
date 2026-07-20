package bench

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/giraffesyo/mux"
	"github.com/hashicorp/yamux"
)

// The mixed workload: a small request/response stream sharing a session with
// four saturating bulk streams. This is the number that decides whether a mux
// is usable for interactive traffic, and the source of the "disable mux for
// gaming" folklore.
//
// Both latency and the concurrent bulk throughput are reported, because
// latency under load is only comparable between implementations moving the
// same amount of data — a mux that carries less bulk will always look better
// on latency alone.

func BenchmarkMixedWorkloadMux(b *testing.B) {
	a, z := tcpPair(b)
	cfg := &mux.Config{InitialWindow: 16 << 20}
	cs, err := mux.Client(a, cfg)
	if err != nil {
		b.Fatal(err)
	}
	ss, err := mux.Server(z, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cs.Close()
	defer ss.Close()
	ctx := context.Background()
	go func() {
		for {
			st, err := ss.AcceptStream(ctx)
			if err != nil {
				return
			}
			go io.Copy(st, st)
		}
	}()
	var bulkBytes atomic.Int64
	stop := make(chan struct{})
	for range 4 {
		bulk, err := cs.OpenStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			buf := make([]byte, 64<<10)
			for {
				select {
				case <-stop:
					return
				default:
				}
				n, err := bulk.Write(buf)
				if err != nil {
					return
				}
				bulkBytes.Add(int64(n))
			}
		}()
		go io.Copy(io.Discard, bulk)
	}
	st, err := cs.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	msg := make([]byte, 64)
	reply := make([]byte, 64)
	start := time.Now()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(msg); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(st, reply); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(stop)
	b.ReportMetric(float64(bulkBytes.Load())/time.Since(start).Seconds()/1e6, "bulkMB/s")
}

func BenchmarkMixedWorkloadYamux(b *testing.B) {
	a, z := tcpPair(b)
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = false
	cfg.MaxStreamWindowSize = 16 * 1024 * 1024
	cfg.LogOutput = io.Discard
	cs, err := yamux.Client(a, cfg)
	if err != nil {
		b.Fatal(err)
	}
	ss, err := yamux.Server(z, cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer cs.Close()
	defer ss.Close()
	go func() {
		for {
			st, err := ss.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(st, st)
		}
	}()
	var bulkBytes atomic.Int64
	stop := make(chan struct{})
	for range 4 {
		bulk, err := cs.OpenStream()
		if err != nil {
			b.Fatal(err)
		}
		go func() {
			buf := make([]byte, 64<<10)
			for {
				select {
				case <-stop:
					return
				default:
				}
				n, err := bulk.Write(buf)
				if err != nil {
					return
				}
				bulkBytes.Add(int64(n))
			}
		}()
		go io.Copy(io.Discard, bulk)
	}
	st, err := cs.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	msg := make([]byte, 64)
	reply := make([]byte, 64)
	start := time.Now()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(msg); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(st, reply); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(stop)
	b.ReportMetric(float64(bulkBytes.Load())/time.Since(start).Seconds()/1e6, "bulkMB/s")
}
