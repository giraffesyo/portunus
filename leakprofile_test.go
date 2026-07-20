package portunus

import (
	"bytes"
	"context"
	"io"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// TestGoroutineLeakProfile checks the runtime's own leak detector rather than
// counting goroutines by hand.
//
// The stack-counting check in leak_test.go can only say "more goroutines than
// before", which is noisy and misses a goroutine that leaked but was replaced.
// This profile reports goroutines the runtime can prove are permanently
// blocked — exactly the shape the flusher exit protocol and credit accounting
// produce when they go wrong.
//
// It requires GOEXPERIMENT=goroutineleakprofile; CI runs it that way.
func TestGoroutineLeakProfile(t *testing.T) {
	p := pprof.Lookup("goroutineleak")
	if p == nil {
		t.Skip("run with GOEXPERIMENT=goroutineleakprofile to enable")
	}

	before := p.Count()
	runWorkloadForLeakCheck(t)

	// The detector is GC-reachability based, so it needs a cycle to run.
	runtime.GC()
	runtime.GC()

	if got := p.Count(); got > before {
		var buf bytes.Buffer
		_ = p.WriteTo(&buf, 1)
		t.Fatalf("goroutine leak profile grew from %d to %d:\n%s", before, got, buf.String())
	}
}

// runWorkloadForLeakCheck exercises the paths where a leak would hide:
// blocked readers and writers, resets, deadline expiries, and teardown with
// data still queued.
func runWorkloadForLeakCheck(t *testing.T) {
	t.Helper()
	for range 20 {
		client, server := pair(t, &Config{KeepaliveInterval: -1})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		go func() {
			for {
				st, err := server.AcceptStream(ctx)
				if err != nil {
					return
				}
				go func(st Stream) { io.Copy(st, st); st.Close() }(st)
			}
		}()

		for i := range 8 {
			st, err := client.OpenStream(ctx)
			if err != nil {
				break
			}
			switch i % 4 {
			case 0:
				st.Write(make([]byte, 128<<10))
				st.CloseWrite()
				io.ReadAll(st)
			case 1:
				st.Write([]byte("x"))
				st.CancelWrite(CodeApp)
			case 2:
				st.SetDeadline(time.Now().Add(time.Millisecond))
				st.Write(make([]byte, 64<<10))
				io.ReadAll(st)
			default:
				// Leave a reader blocked, then tear the session down
				// underneath it.
				go st.Read(make([]byte, 16))
			}
			st.Close()
		}

		client.Close()
		server.Close()
		client.Wait()
		server.Wait()
		cancel()
	}
}
