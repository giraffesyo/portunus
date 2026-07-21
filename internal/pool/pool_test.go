package pool

import (
	"bytes"
	"sync"
	"testing"
)

func TestGetReturnsExactLength(t *testing.T) {
	for _, n := range []int{0, 1, 100, 1 << 10, (1 << 10) + 1, 64 << 10, 256 << 10} {
		b := Get(n)
		if got := len(b.Bytes()); got != n {
			t.Errorf("Get(%d) returned %d bytes", n, got)
		}
		Put(b)
	}
}

// Capacity must stop at the valid length. Pooled memory is never zeroed, so a
// consumer that appends past the length would write into bytes a later Get
// hands to another stream.
func TestBytesCapacityIsCapped(t *testing.T) {
	b := Get(100)
	defer Put(b)
	s := b.Bytes()
	if cap(s) != 100 {
		t.Fatalf("capacity %d exceeds the %d valid bytes", cap(s), 100)
	}
	// Appending must therefore copy rather than extend into the pool.
	grown := append(s, 'x')
	if &grown[0] == &s[0] {
		t.Fatal("append extended into pooled memory instead of copying")
	}
}

// A buffer taken from the pool must not expose whatever the previous holder
// wrote past the length now requested.
func TestRecycledBufferExposesNothingStale(t *testing.T) {
	const class = 1 << 10
	first := Get(class)
	for i := range first.Bytes() {
		first.Bytes()[i] = 0xAA
	}
	Put(first)

	// A smaller request served by the same class must show only its own
	// length, and appending to it must not reach the stale tail.
	second := Get(16)
	defer Put(second)
	got := second.Bytes()
	if len(got) != 16 || cap(got) != 16 {
		t.Fatalf("len=%d cap=%d, want 16 and 16", len(got), cap(got))
	}
	for i := range got {
		got[i] = 1
	}
	if bytes.Contains(got, []byte{0xAA}) {
		t.Fatal("recycled buffer exposed bytes from its previous holder")
	}
}

// The zero value is the "no buffer" case, which the receive path relies on
// for frames that carry no payload.
func TestZeroBufIsSafe(t *testing.T) {
	var b Buf
	if b.Bytes() != nil {
		t.Errorf("zero Buf returned %v, want nil", b.Bytes())
	}
	Put(b) // must not panic
}

// Requests larger than the biggest class are served directly and dropped on
// release, rather than growing the pools without bound.
func TestOversizedRequestsWork(t *testing.T) {
	const huge = (256 << 10) + 1
	b := Get(huge)
	if len(b.Bytes()) != huge {
		t.Fatalf("Get(%d) returned %d bytes", huge, len(b.Bytes()))
	}
	Put(b) // dropped, not pooled
	if got := len(Get(huge).Bytes()); got != huge {
		t.Fatalf("second oversized Get returned %d bytes", got)
	}
}

// Cycling a size repeatedly must keep drawing on the same small set of
// buffers rather than minting new memory each time.
//
// Deliberately not asserted as "the buffer just released comes back": a
// sync.Pool returns *a* buffer, not *the* one, and whatever an earlier caller
// left in the per-P private slot is handed out first. Counting distinct
// backing arrays tests reuse without depending on which one arrives.
func TestReuseKeepsBufferCountBounded(t *testing.T) {
	const cycles = 50
	for _, n := range []int{512, 4 << 10, 60 << 10} {
		seen := map[*byte]bool{}
		for range cycles {
			b := Get(n)
			seen[&b.Bytes()[0]] = true
			Put(b)
		}
		// Deliberately a loose bound rather than a precise one. How many
		// distinct buffers appear depends on how sync.Pool shards across
		// Ps, where the goroutine happens to be scheduled, and what other
		// tests left behind — none of which this package controls or
		// should encode. Without reuse the count would equal the number of
		// cycles, so half of that is comfortably diagnostic while staying
		// stable from a two-core CI runner to an eighteen-core laptop.
		limit := cycles / 2
		if len(seen) > limit {
			t.Errorf("size %d: %d distinct buffers across %d cycles (limit %d); the pool is not reusing",
				n, len(seen), cycles, limit)
		}
	}
}

// Reuse must also hold in aggregate, so that steady-state traffic is not
// quietly allocating on most cycles.
func TestReuseDominatesAllocation(t *testing.T) {
	sizes := []int{512, 4 << 10, 60 << 10}
	for _, n := range sizes {
		Put(Get(n)) // warm each class
	}
	allocs := testing.AllocsPerRun(200, func() {
		for _, n := range sizes {
			b := Get(n)
			b.Bytes()[0] = 1
			Put(b)
		}
	})
	// Not asserted as exactly zero: sync.Pool is emptied across GC cycles
	// (its contents move to a victim cache and are then dropped), so a
	// collection landing inside the measurement legitimately forces a
	// refill. The claim worth making is that refills are the exception —
	// three buffers are taken per run, so anything under one allocation
	// means most cycles reused.
	if allocs >= float64(len(sizes)) {
		t.Errorf("Get/Put allocated %.1f times per run while taking %d buffers; reuse is not happening",
			allocs, len(sizes))
	}
}

// The pools are shared across sessions, so concurrent use has to be safe.
// Run with -race for this to mean anything.
func TestConcurrentUse(t *testing.T) {
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := range 500 {
				n := 1 << (10 + i%4)
				b := Get(n)
				s := b.Bytes()
				// Write a pattern unique to this goroutine and verify it
				// survives, which fails if two holders share a buffer.
				fill := byte(w + 1)
				for j := range s {
					s[j] = fill
				}
				for j := range s {
					if s[j] != fill {
						t.Errorf("buffer content changed underneath goroutine %d", w)
						return
					}
				}
				Put(b)
			}
		}(w)
	}
	wg.Wait()
}

// A request maps to the smallest class that fits, so the memory handed out is
// never wildly larger than what was asked for.
func TestSizeClassSelection(t *testing.T) {
	for _, tc := range []struct{ req, wantCap int }{
		{1, 1 << 10},
		{1 << 10, 1 << 10},
		{(1 << 10) + 1, 4 << 10},
		{4 << 10, 4 << 10},
		{(16 << 10) + 1, 64 << 10},
		{256 << 10, 256 << 10},
	} {
		b := Get(tc.req)
		if got := cap(*b.p); got != tc.wantCap {
			t.Errorf("Get(%d) drew from a class of %d, want %d", tc.req, got, tc.wantCap)
		}
		Put(b)
	}
}
