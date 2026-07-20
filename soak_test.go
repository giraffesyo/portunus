package portunus

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSoak runs sustained mixed traffic — bulk transfers, small round trips,
// resets, cancels, and deadline expiries all interleaved — and verifies that
// every byte that was supposed to arrive did, that no goroutines leak, and
// that session state returns to empty.
//
// The stress test covers a single burst; this covers duration. Bugs in
// pooling, credit accounting, and stream reaping only surface after state has
// cycled many times, which is exactly what the field reports about every
// multiplexer that shipped one.
func TestSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak test")
	}
	checkLeaks(t)

	client, server := pair(t, &Config{
		InitialWindow:     64 << 10, // small, so autotuning is exercised
		MaxWindow:         4 << 20,
		KeepaliveInterval: 50 * time.Millisecond,
		KeepaliveTimeout:  10 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	srvDone := make(chan struct{})
	go func() {
		defer close(srvDone)
		for {
			st, err := server.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func(st Stream) {
				io.Copy(st, st)
				st.CloseWrite()
				st.Close()
			}(st)
		}
	}()

	const (
		duration = 8 * time.Second
		workers  = 16
	)
	var (
		completed atomic.Int64
		mismatch  atomic.Int64
		wg        sync.WaitGroup
	)
	deadline := time.Now().Add(duration)

	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for round := 0; time.Now().Before(deadline); round++ {
				if err := soakRound(ctx, client, w, round); err != nil {
					if errors.Is(err, errPayloadMismatch) {
						mismatch.Add(1)
					}
					// Any other error is a legitimate outcome of the
					// resets and deadlines this test deliberately causes.
					continue
				}
				completed.Add(1)
			}
		}(w)
	}
	wg.Wait()

	if n := mismatch.Load(); n > 0 {
		t.Fatalf("%d echo round trips returned corrupted payloads", n)
	}
	// A floor scaled to the worker count, not an absolute number: under the
	// race detector throughput drops by orders of magnitude, and a fixed
	// threshold tuned without it fails for reasons that have nothing to do
	// with the code under test. The real assertions are payload integrity,
	// no leaks, and state draining to empty.
	if n := completed.Load(); n < workers {
		t.Fatalf("only %d round trips completed across %d workers; the soak did not exercise much", n, workers)
	}
	t.Logf("%d clean round trips", completed.Load())

	// State must drain back to empty once the traffic stops.
	deadline = time.Now().Add(10 * time.Second)
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
			<-srvDone
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	client.mu.Lock()
	cn := len(client.streams)
	client.mu.Unlock()
	server.mu.Lock()
	sn, inc := len(server.streams), server.incoming
	server.mu.Unlock()
	t.Fatalf("state retained after the soak: client=%d server=%d incoming=%d", cn, sn, inc)
}

var errPayloadMismatch = errors.New("payload mismatch")

// soakRound performs one randomized exchange. Its shape varies by round so
// the mix covers clean transfers, half-closes, resets from either side,
// cancelled reads, and deadline expiries.
func soakRound(ctx context.Context, s *NativeSession, worker, round int) error {
	st, err := s.OpenStream(ctx)
	if err != nil {
		return err
	}
	defer st.Close()

	// Sizes straddle the copy/zero-copy boundary and the frame size.
	sizes := []int{0, 1, 100, 4 << 10, 64 << 10, 200 << 10}
	size := sizes[(worker+round)%len(sizes)]
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(worker + round + i)
	}

	switch round % 6 {
	case 0, 1, 2: // clean echo: the only case that verifies bytes
		errCh := make(chan error, 1)
		go func() {
			_, err := st.Write(payload)
			if err == nil {
				err = st.CloseWrite()
			}
			errCh <- err
		}()
		got, err := io.ReadAll(st)
		if werr := <-errCh; werr != nil {
			return werr
		}
		if err != nil {
			return err
		}
		if !bytes.Equal(got, payload) {
			return errPayloadMismatch
		}
		return nil

	case 3: // abandon the send side mid-transfer
		go st.Write(payload)
		st.CancelWrite(CodeApp + uint64(round))
		return nil

	case 4: // abandon the read side, then finish sending
		st.CancelRead(CodeApp + uint64(round))
		st.Write(payload)
		return st.CloseWrite()

	default: // a deadline that will expire mid-flight
		st.SetDeadline(time.Now().Add(time.Millisecond))
		st.Write(payload)
		io.ReadAll(st)
		return nil
	}
}
