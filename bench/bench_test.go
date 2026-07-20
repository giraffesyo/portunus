// Package bench compares mux against tuned baselines. It is a separate
// module so the core stays dependency-free.
//
// Baselines are configured at their best, not their defaults: comparing
// against a strawman proves nothing.
package bench

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/giraffesyo/mux"
	"github.com/hashicorp/yamux"
)

// tcpPair returns a connected pair of loopback TCP conns.
func tcpPair(tb testing.TB) (net.Conn, net.Conn) {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	defer ln.Close()
	type res struct {
		c   net.Conn
		err error
	}
	ch := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		ch <- res{c, err}
	}()
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		tb.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		tb.Fatal(r.err)
	}
	return a, r.c
}

// muxConfig matches the tuning applied to the yamux baseline: the same
// window, so the comparison measures the implementations rather than two
// different flow-control budgets. (Until BDP autotune lands in M5, a static
// window is all either side has.)
func muxConfig() *mux.Config {
	return &mux.Config{InitialWindow: 16 << 20}
}

// muxPair builds a mux client/server session pair.
func muxPair(tb testing.TB) (*mux.NativeSession, *mux.NativeSession) {
	a, b := tcpPair(tb)
	cs, err := mux.Client(a, muxConfig())
	if err != nil {
		tb.Fatal(err)
	}
	ss, err := mux.Server(b, muxConfig())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { cs.Close(); ss.Close() })
	return cs, ss
}

// yamuxConfig returns yamux tuned rather than defaulted: its maximum
// window, and keepalive off so timers do not pollute the measurement.
func yamuxConfig() *yamux.Config {
	c := yamux.DefaultConfig()
	c.EnableKeepAlive = false
	c.MaxStreamWindowSize = 16 * 1024 * 1024
	c.LogOutput = io.Discard
	return c
}

func yamuxPair(tb testing.TB) (*yamux.Session, *yamux.Session) {
	a, b := tcpPair(tb)
	cs, err := yamux.Client(a, yamuxConfig())
	if err != nil {
		tb.Fatal(err)
	}
	ss, err := yamux.Server(b, yamuxConfig())
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { cs.Close(); ss.Close() })
	return cs, ss
}

// --- single-stream bulk throughput ---

func BenchmarkBulkMux(b *testing.B) {
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

	st, err := cs.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBulkYamux(b *testing.B) {
	cs, ss := yamuxPair(b)

	go func() {
		for {
			st, err := ss.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	st, err := cs.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}

// --- many-stream small messages (the batching-sensitive workload) ---

const (
	parStreams = 64
	msgSize    = 256
)

func BenchmarkSmallMsgsMux(b *testing.B) {
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

	streams := make([]mux.Stream, parStreams)
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

func BenchmarkSmallMsgsYamux(b *testing.B) {
	cs, ss := yamuxPair(b)

	go func() {
		for {
			st, err := ss.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()

	streams := make([]net.Conn, parStreams)
	for i := range streams {
		st, err := cs.OpenStream()
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

// --- echo round-trip latency ---

func BenchmarkEchoRTTMux(b *testing.B) {
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

	st, err := cs.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	msg := make([]byte, 64)
	reply := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(msg); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(st, reply); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEchoRTTYamux(b *testing.B) {
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

	st, err := cs.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	msg := make([]byte, 64)
	reply := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(msg); err != nil {
			b.Fatal(err)
		}
		if _, err := io.ReadFull(st, reply); err != nil {
			b.Fatal(err)
		}
	}
}

// --- stream open/close churn ---

func BenchmarkStreamChurnMux(b *testing.B) {
	cs, ss := muxPair(b)
	ctx := context.Background()

	go func() {
		for {
			st, err := ss.AcceptStream(ctx)
			if err != nil {
				return
			}
			go func(st mux.Stream) { io.Copy(io.Discard, st); st.Close() }(st)
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		st, err := cs.OpenStream(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := st.Write([]byte("x")); err != nil {
			b.Fatal(err)
		}
		st.Close()
	}
}

func BenchmarkStreamChurnYamux(b *testing.B) {
	cs, ss := yamuxPair(b)

	go func() {
		for {
			st, err := ss.AcceptStream()
			if err != nil {
				return
			}
			go func(st net.Conn) { io.Copy(io.Discard, st); st.Close() }(st)
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		st, err := cs.OpenStream()
		if err != nil {
			b.Fatal(err)
		}
		if _, err := st.Write([]byte("x")); err != nil {
			b.Fatal(err)
		}
		st.Close()
	}
}

// --- relay: the tunnel/proxy workload the receive fast path targets ---

// BenchmarkRelayMux copies a stream from one session to a stream on another,
// which is what a mux-based proxy does for every connection it carries.
func BenchmarkRelayMux(b *testing.B) {
	front, frontSrv := muxPair(b)
	back, backSrv := muxPair(b)
	ctx := context.Background()

	go func() { // backend sink
		for {
			st, err := backSrv.AcceptStream(ctx)
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()
	go func() { // proxy
		for {
			in, err := frontSrv.AcceptStream(ctx)
			if err != nil {
				return
			}
			out, err := back.OpenStream(ctx)
			if err != nil {
				return
			}
			go func() { io.Copy(out, in); out.CloseWrite() }()
		}
	}()

	st, err := front.OpenStream(ctx)
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRelayYamux(b *testing.B) {
	front, frontSrv := yamuxPair(b)
	back, backSrv := yamuxPair(b)

	go func() {
		for {
			st, err := backSrv.AcceptStream()
			if err != nil {
				return
			}
			go io.Copy(io.Discard, st)
		}
	}()
	go func() {
		for {
			in, err := frontSrv.AcceptStream()
			if err != nil {
				return
			}
			out, err := back.OpenStream()
			if err != nil {
				return
			}
			go func() { io.Copy(out, in); out.Close() }()
		}
	}()

	st, err := front.OpenStream()
	if err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 64<<10)
	b.SetBytes(int64(len(buf)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := st.Write(buf); err != nil {
			b.Fatal(err)
		}
	}
}

// atomicCounter hands each parallel worker a distinct stream index.
type atomicCounter struct {
	mu sync.Mutex
	n  int
}

func (c *atomicCounter) next() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return c.n - 1
}

// selfSignedTLS is used by the TLS-carrier benchmarks (M3+); kept here so
// the carrier matrix is in one place.
func selfSignedTLS(tb testing.TB) *tls.Config {
	tb.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		tb.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "bench"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		tb.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		tb.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		tb.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true, MinVersion: tls.VersionTLS13}
}
