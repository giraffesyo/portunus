package bench

import (
	"context"
	"io"
	"runtime"
	"strconv"
	"sync"
	"testing"
)

// Contention measurement.
//
// Group commit funnels every write on a session through one mutex. That is
// the right shape at low core counts and the design's stated biggest risk at
// high ones: past some number of cores the lock becomes a serialization point
// and throughput stops scaling. The design names mitigations — per-P
// pre-staging, an MPSC admission queue — but committing to one before
// measuring where the knee is would be guessing.
//
// BenchmarkScalingMux sweeps GOMAXPROCS so the knee is visible as a shape
// rather than inferred from a single data point. Run with -mutexprofile to
// attribute it:
//
//	go test -bench=ScalingMux -mutexprofile=mu.out ./bench
//	go tool pprof -top mu.out

func BenchmarkScalingMux(b *testing.B) {
	for _, procs := range []int{1, 2, 4, 8, 16, 32} {
		if procs > runtime.NumCPU()*2 {
			continue
		}
		b.Run("GOMAXPROCS="+strconv.Itoa(procs), func(b *testing.B) {
			old := runtime.GOMAXPROCS(procs)
			defer runtime.GOMAXPROCS(old)
			runScaling(b, procs)
		})
	}
}

// runScaling drives one writer goroutine per core over its own stream, which
// is the workload that puts maximum pressure on the shared send mutex.
func runScaling(b *testing.B, writers int) {
	cs, ss := muxPair(b)
	ctx := context.Background()

	go func() {
		for {
			st, err := ss.AcceptStream(ctx)
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	streams := make([]interface{ Write([]byte) (int, error) }, writers)
	for i := range streams {
		st, err := cs.OpenStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		streams[i] = st
	}

	const msgSize = 512
	msg := make([]byte, msgSize)
	b.SetBytes(msgSize)
	b.ReportAllocs()
	b.ResetTimer()

	var (
		wg   sync.WaitGroup
		next = make(chan int, writers)
	)
	for i := range writers {
		next <- i
	}
	b.RunParallel(func(pb *testing.PB) {
		i := <-next
		st := streams[i%writers]
		wg.Add(1)
		defer wg.Done()
		for pb.Next() {
			if _, err := st.Write(msg); err != nil {
				b.Error(err)
				return
			}
		}
	})
	b.StopTimer()
	wg.Wait()

	// Frames per flush says directly whether batching survived the
	// contention: a ratio near 1 means every frame paid for its own
	// syscall, which is the cost this design exists to avoid.
	b.ReportMetric(cs.Stats().FramesPerFlush, "frames/flush")
}
