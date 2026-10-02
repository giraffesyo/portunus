package bench

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"

	"github.com/giraffesyo/portunus"
)

// TLS is the carrier most deployments actually run, and it has its own cost
// structure: crypto/tls writes one record per syscall, so the syscall count
// the TCP benchmarks measure says little about it. These rows put the plain
// TLS carrier beside one whose transport is wrapped in portunus.Batched.

// tlsMuxPair builds a session pair over TLS on loopback. wrap is applied to
// each TCP connection before TLS is layered on it.
func tlsMuxPair(tb testing.TB, wrap func(net.Conn) net.Conn) (*portunus.NativeSession, *portunus.NativeSession) {
	tb.Helper()
	cfg := selfSignedTLS(tb)
	a, b := tcpPair(tb)
	ca := tls.Client(wrap(a), cfg)
	cb := tls.Server(wrap(b), cfg)

	// Both constructors write SETTINGS and so drive the handshake, each
	// needing the other: build them together.
	type res struct {
		s   *portunus.NativeSession
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := portunus.Server(cb, muxConfig())
		ch <- res{s, err}
	}()
	cs, err := portunus.Client(ca, muxConfig())
	if err != nil {
		tb.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		tb.Fatal(r.err)
	}
	tb.Cleanup(func() { cs.Close(); r.s.Close() })
	return cs, r.s
}

func plainConn(c net.Conn) net.Conn { return c }

func benchBulkTLS(b *testing.B, wrap func(net.Conn) net.Conn, size int) {
	cs, ss := tlsMuxPair(b, wrap)
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
	st, err := cs.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, size)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBulkTLSMux(b *testing.B)        { benchBulkTLS(b, plainConn, 64<<10) }
func BenchmarkBulkTLSBatchedMux(b *testing.B) { benchBulkTLS(b, portunus.Batched, 64<<10) }

func benchSmallMsgsTLS(b *testing.B, wrap func(net.Conn) net.Conn) {
	cs, ss := tlsMuxPair(b, wrap)
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
	streams := make([]portunus.Stream, parStreams)
	for i := range streams {
		st, err := cs.OpenStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		streams[i] = st
	}
	msg := make([]byte, msgSize)
	b.SetBytes(msgSize)
	b.ReportAllocs()
	b.ResetTimer()

	var idx atomicCounter
	b.RunParallel(func(pb *testing.PB) {
		st := streams[idx.next()%parStreams]
		for pb.Next() {
			if _, err := st.Write(msg); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkSmallMsgsTLSMux(b *testing.B)        { benchSmallMsgsTLS(b, plainConn) }
func BenchmarkSmallMsgsTLSBatchedMux(b *testing.B) { benchSmallMsgsTLS(b, portunus.Batched) }
