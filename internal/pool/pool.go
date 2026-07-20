// Package pool provides size-classed buffer pools for receive segments.
//
// The receive path copies each frame payload exactly once, into a pooled
// segment that is released when the application has consumed it. Pooling is
// what keeps steady-state receive allocation-free; sync.Pool's per-P sharding
// keeps it contention-free.
package pool

import "sync"

// Size classes in bytes. A request is served by the smallest class that fits;
// anything larger than the biggest class is allocated directly and dropped on
// release rather than growing the pools without bound.
var classes = [...]int{
	1 << 10,
	4 << 10,
	16 << 10,
	64 << 10,
	256 << 10,
}

var pools [len(classes)]sync.Pool

func init() {
	for i := range pools {
		size := classes[i]
		pools[i].New = func() any {
			b := make([]byte, size)
			return &b
		}
	}
}

// Buf is a pooled buffer. It is a pointer type because sync.Pool stores
// interface values: putting a []byte back would box the slice header, and
// taking the address of a local slice to avoid that boxing makes it escape —
// an allocation on every release, which is exactly what pooling exists to
// avoid. Holding the pointer the pool handed out keeps release allocation
// free.
//
// The zero Buf is valid and releases to nothing.
type Buf struct {
	p    *[]byte
	size int
}

// Bytes returns the usable slice, exactly the length that was requested.
//
// Pooled memory is never zeroed on reuse — that is the point — so a caller
// must expose only what it has written. Handing out bytes past the written
// length would surface another stream's prior payload.
func (b Buf) Bytes() []byte {
	if b.p == nil {
		return nil
	}
	return (*b.p)[:b.size]
}

// Get returns a buffer of exactly n bytes.
func Get(n int) Buf {
	for i, c := range classes {
		if n <= c {
			return Buf{p: pools[i].Get().(*[]byte), size: n}
		}
	}
	// Larger than any class: allocate directly and let Put drop it.
	b := make([]byte, n)
	return Buf{p: &b, size: n}
}

// Put returns a buffer obtained from Get. Buffers outside the size classes
// are dropped for the garbage collector.
func Put(b Buf) {
	if b.p == nil {
		return
	}
	c := cap(*b.p)
	for i, size := range classes {
		if c == size {
			pools[i].Put(b.p)
			return
		}
	}
}
