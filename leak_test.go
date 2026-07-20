package portunus

import (
	"context"
	"io"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// checkLeaks asserts that the goroutines running at test start are the only
// ones left when it ends. Sessions spawn a reader per session, so a leak here
// means a teardown path failed to release one.
func checkLeaks(t *testing.T) {
	t.Helper()
	before := goroutineStacks()
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for {
			after := goroutineStacks()
			if len(after) <= len(before) {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("goroutine leak: %d before, %d after\n%s",
					len(before), len(after), strings.Join(after, "\n---\n"))
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}

func goroutineStacks() []string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for g := range strings.SplitSeq(string(buf), "\n\n") {
		// Ignore runtime-internal and test-harness goroutines.
		if strings.Contains(g, "runtime.gopark") && strings.Contains(g, "testing.(*T).Run") {
			continue
		}
		if strings.Contains(g, "created by testing.") || g == "" {
			continue
		}
		out = append(out, g)
	}
	sort.Strings(out)
	return out
}

func TestNoGoroutineLeakOnSessionClose(t *testing.T) {
	checkLeaks(t)
	client, server := pair(t, nil)
	c := ctx(t)

	go func() {
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go func(st Stream) { io.Copy(st, st); st.CloseWrite() }(st)
		}
	}()

	for range 16 {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		st.Write([]byte("payload"))
		st.CloseWrite()
		io.ReadAll(st)
		st.Close()
	}
	client.Close()
	server.Close()
	client.Wait()
	server.Wait()
}

// A blocked reader and a blocked writer must both be released by Close, with
// no goroutine left parked.
func TestNoGoroutineLeakWithBlockedCalls(t *testing.T) {
	checkLeaks(t)
	client, server := pair(t, nil)
	cs, _ := openAccept(t, client, server)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); cs.Read(make([]byte, 8)) }()
	go func() { defer wg.Done(); cs.Write(make([]byte, 8<<20)) }()

	time.Sleep(50 * time.Millisecond)
	client.Close()
	server.Close()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("blocked stream calls did not return after session close")
	}
	client.Wait()
	server.Wait()
}

// Stream churn must not accumulate session state: the map and the accept-slot
// counter both have to return to zero.
func TestStreamChurnReleasesState(t *testing.T) {
	checkLeaks(t)
	client, server := pair(t, nil)
	c := ctx(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			io.Copy(io.Discard, st)
			st.Close()
		}
	}()

	const rounds = 200
	for range rounds {
		st, err := client.OpenStream(c)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := st.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		st.Close()
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		cn := len(client.streams)
		client.mu.Unlock()
		server.mu.Lock()
		sn, inc := len(server.streams), server.incoming
		server.mu.Unlock()
		if cn == 0 && sn == 0 && inc == 0 {
			client.Close()
			server.Close()
			<-done
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	client.mu.Lock()
	cn := len(client.streams)
	client.mu.Unlock()
	server.mu.Lock()
	sn, inc := len(server.streams), server.incoming
	server.mu.Unlock()
	t.Fatalf("state retained after %d closed streams: client=%d server=%d incoming=%d", rounds, cn, sn, inc)
}

// Interleaved writes, resets, cancels, and closes across many streams, run
// under -race, is the densest correctness zone in the design.
func TestStressInterleavedLifecycles(t *testing.T) {
	if testing.Short() {
		t.Skip("stress test")
	}
	checkLeaks(t)
	client, server := pair(t, nil)
	c, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		for {
			st, err := server.AcceptStream(c)
			if err != nil {
				return
			}
			go func(st Stream, id uint64) {
				switch id % 4 {
				case 0:
					io.Copy(st, st)
					st.CloseWrite()
				case 1:
					st.CancelWrite(CodeApp + 1)
				case 2:
					st.CancelRead(CodeApp + 2)
					st.Write([]byte("late"))
					st.CloseWrite()
				default:
					io.Copy(io.Discard, st)
					st.Close()
				}
			}(st, st.StreamID())
		}
	}()

	var wg sync.WaitGroup
	const streams = 128
	for i := range streams {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			st, err := client.OpenStream(c)
			if err != nil {
				return
			}
			payload := make([]byte, 1<<10*(1+i%16))
			switch i % 5 {
			case 0:
				st.Write(payload)
				st.CloseWrite()
				io.ReadAll(st)
			case 1:
				st.Write(payload)
				st.CancelWrite(CodeApp + 3)
			case 2:
				st.SetDeadline(time.Now().Add(50 * time.Millisecond))
				st.Write(payload)
				io.ReadAll(st)
			case 3:
				st.Write(payload[:1])
				st.CancelRead(CodeApp + 4)
				st.CloseWrite()
			default:
				st.Close()
			}
			st.Close()
		}(i)
	}
	wg.Wait()

	client.Close()
	server.Close()
	<-srvDone
	client.Wait()
	server.Wait()
}
