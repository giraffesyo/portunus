package bench

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"

	"github.com/giraffesyo/portunus"
)

// Embedding the interface keeps writes observable by a wrapper and prevents
// net.Buffers from reaching the socket's private vectored-write method.
type wrappedCarrier struct{ net.Conn }

func carrierPair(tb testing.TB, carrier string) (*portunus.NativeSession, *portunus.NativeSession) {
	tb.Helper()
	if carrier == "tls" {
		return tlsMuxPair(tb, plainConn)
	}
	if carrier == "tls-batched" {
		return tlsMuxPair(tb, portunus.Batched)
	}
	var a, b net.Conn
	if carrier == "unix" {
		dir, err := os.MkdirTemp("", "prtns-bench")
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { os.RemoveAll(dir) })
		ln, err := net.Listen("unix", filepath.Join(dir, "s"))
		if err != nil {
			tb.Skipf("Unix sockets unavailable: %v", err)
		}
		defer ln.Close()
		type result struct {
			conn net.Conn
			err  error
		}
		ch := make(chan result, 1)
		go func() {
			c, err := ln.Accept()
			ch <- result{c, err}
		}()
		a, err = net.Dial("unix", ln.Addr().String())
		if err != nil {
			tb.Fatal(err)
		}
		r := <-ch
		if r.err != nil {
			tb.Fatal(r.err)
		}
		b = r.conn
	} else {
		a, b = tcpPair(tb)
		if carrier == "wrapped" {
			a, b = wrappedCarrier{a}, wrappedCarrier{b}
		}
	}
	cs, err := portunus.Client(a, muxConfig())
	if err != nil {
		tb.Fatal(err)
	}
	ss, err := portunus.Server(b, muxConfig())
	if err != nil {
		cs.Close()
		tb.Fatal(err)
	}
	tb.Cleanup(func() { cs.Close(); ss.Close() })
	return cs, ss
}

// Run this unchanged against both library revisions. Unix tests vectored
// writes; wrapped tests coalescing; TLS checks record and batch behavior.
func BenchmarkCarrierMux(b *testing.B) {
	for _, carrier := range []string{"tcp", "wrapped", "unix", "tls", "tls-batched"} {
		b.Run(carrier, func(b *testing.B) {
			for _, size := range []struct {
				name string
				n    int
			}{{"64K", 64 << 10}, {"1M", 1 << 20}} {
				b.Run(size.name, func(b *testing.B) {
					cs, ss := carrierPair(b, carrier)
					ctx := context.Background()
					go func() {
						st, err := ss.AcceptStream(ctx)
						if err == nil {
							io.Copy(io.Discard, st)
						}
					}()
					st, err := cs.OpenStream(ctx)
					if err != nil {
						b.Fatal(err)
					}
					buf := make([]byte, size.n)
					b.SetBytes(int64(size.n))
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						if _, err := st.Write(buf); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		})
	}
}
