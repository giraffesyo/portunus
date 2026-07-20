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

// Get returns a buffer of exactly n bytes, backed by a pooled allocation of
// the smallest class that fits. Put returns it.
func Get(n int) []byte {
	for i, c := range classes {
		if n <= c {
			bp := pools[i].Get().(*[]byte)
			return (*bp)[:n]
		}
	}
	return make([]byte, n)
}

// Put returns a buffer obtained from Get. Buffers larger than the biggest
// class, or resliced beyond their original capacity, are dropped.
//
// Pooled buffers are never zeroed on reuse — that is the point — so callers
// must only ever expose a segment up to its valid written length. Handing out
// bytes beyond it would leak another stream's prior payload.
func Put(b []byte) {
	c := cap(b)
	for i, size := range classes {
		if c == size {
			b = b[:size]
			pools[i].Put(&b)
			return
		}
	}
}
