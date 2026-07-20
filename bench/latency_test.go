package bench

import (
	"context"
	"io"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/giraffesyo/portunus"
	"github.com/hashicorp/yamux"
)

// Open-loop latency measurement.
//
// A closed-loop benchmark — send, wait for the reply, send again — cannot
// measure latency under load honestly. When the system stalls, the harness
// stalls with it and simply issues fewer requests, so the requests that
// *would* have been delayed are never sent and never counted. That is
// coordinated omission, and it makes a system look best exactly when it is
// behaving worst.
//
// These benchmarks instead issue requests on a fixed schedule regardless of
// whether earlier ones have completed, and measure each from its intended
// send time. A request delayed because the previous one was still in flight
// carries that delay in its own measurement, which is the whole point.

// latencyResult holds one run's percentiles.
type latencyResult struct {
	p50, p99, p999, max time.Duration
	sent, completed     int
	err                 error
}

func (r latencyResult) report(b *testing.B) {
	b.ReportMetric(float64(r.p50.Microseconds()), "p50-us")
	b.ReportMetric(float64(r.p99.Microseconds()), "p99-us")
	b.ReportMetric(float64(r.p999.Microseconds()), "p99.9-us")
	b.ReportMetric(float64(r.max.Microseconds()), "max-us")
	// A run that could not keep up with its own schedule is reporting
	// latency for a load the system never actually sustained.
	b.ReportMetric(float64(r.completed)/float64(r.sent)*100, "%completed")
	if r.err != nil {
		b.Logf("first failed request: %v", r.err)
	}
}

func percentiles(samples []time.Duration, sent int) latencyResult {
	if len(samples) == 0 {
		return latencyResult{sent: sent}
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	at := func(q float64) time.Duration {
		i := int(math.Ceil(q*float64(len(samples)))) - 1
		return samples[max(0, min(i, len(samples)-1))]
	}
	return latencyResult{
		p50:       at(0.50),
		p99:       at(0.99),
		p999:      at(0.999),
		max:       samples[len(samples)-1],
		sent:      sent,
		completed: len(samples),
	}
}

// openLoop issues a fixed, pre-planned number of requests on a fixed
// schedule, measuring each from its scheduled time rather than its actual
// send time.
//
// The count is planned up front rather than driven by a wall-clock deadline.
// A deadline-driven loop that falls behind stops sleeping and spins, spawning
// unbounded requests and reporting a completion rate that describes the
// harness rather than the system.
func openLoop(rate int, dur time.Duration, do func() error) latencyResult {
	interval := time.Second / time.Duration(rate)
	total := rate * int(dur/time.Second)
	start := time.Now()

	var (
		mu       sync.Mutex
		samples  []time.Duration
		sent     int
		firstErr error
		wg       sync.WaitGroup
	)

	for i := range total {
		// The schedule is absolute: falling behind does not push later
		// requests back, it makes them late, which is what we want to see.
		due := start.Add(time.Duration(i) * interval)
		if d := time.Until(due); d > 0 {
			time.Sleep(d)
		}
		sent++
		wg.Add(1)
		go func(due time.Time) {
			defer wg.Done()
			if err := do(); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			samples = append(samples, time.Since(due))
			mu.Unlock()
		}(due)
	}
	wg.Wait()
	res := percentiles(samples, sent)
	res.err = firstErr
	return res
}

const (
	// The arrival rate must be one the system can sustain, or the run
	// measures how fast a queue grows rather than how long a request takes.
	// At saturation both implementations report multi-second latencies that
	// say nothing except "overloaded".
	latencyRate     = 200 // requests per second
	latencyDuration = 3 * time.Second
	bulkStreams     = 4
)

// BenchmarkOpenLoopMux measures small-request latency at a fixed arrival rate
// while bulk streams saturate the same session.
func BenchmarkOpenLoopMux(b *testing.B) {
	cs, ss := muxPair(b)
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

	stop := startMuxBulk(b, cs, ctx)
	defer close(stop)

	// One dedicated request stream, serialized by a mutex so concurrent
	// scheduled requests queue rather than interleave on the wire.
	st, err := cs.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	var reqMu sync.Mutex
	msg := make([]byte, 64)

	b.ResetTimer()
	res := openLoop(latencyRate, latencyDuration, func() error {
		reqMu.Lock()
		defer reqMu.Unlock()
		if _, err := st.Write(msg); err != nil {
			return err
		}
		reply := make([]byte, 64)
		_, err := io.ReadFull(st, reply)
		return err
	})
	b.StopTimer()
	res.report(b)
}

func BenchmarkOpenLoopYamux(b *testing.B) {
	cs, ss := yamuxPair(b)

	go func() {
		for {
			st, err := ss.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(st, st)
		}
	}()

	stop := startYamuxBulk(b, cs)
	defer close(stop)

	st, err := cs.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	var reqMu sync.Mutex
	msg := make([]byte, 64)

	b.ResetTimer()
	res := openLoop(latencyRate, latencyDuration, func() error {
		reqMu.Lock()
		defer reqMu.Unlock()
		if _, err := st.Write(msg); err != nil {
			return err
		}
		reply := make([]byte, 64)
		_, err := io.ReadFull(st, reply)
		return err
	})
	b.StopTimer()
	res.report(b)
}

func startMuxBulk(b *testing.B, cs *portunus.NativeSession, ctx context.Context) chan struct{} {
	b.Helper()
	stop := make(chan struct{})
	for range bulkStreams {
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
				if _, err := bulk.Write(buf); err != nil {
					return
				}
			}
		}()
		go io.Copy(io.Discard, bulk)
	}
	return stop
}

func startYamuxBulk(b *testing.B, cs *yamux.Session) chan struct{} {
	b.Helper()
	stop := make(chan struct{})
	for range bulkStreams {
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
				if _, err := bulk.Write(buf); err != nil {
					return
				}
			}
		}()
		go io.Copy(io.Discard, bulk)
	}
	return stop
}
