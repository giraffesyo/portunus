package portunus_test

import (
	"bytes"
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
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/giraffesyo/portunus"
	"github.com/giraffesyo/portunus/conformance"
)

// Carriers other than an exact *net.TCPConn take a different send path.
//
// net.Buffers only becomes writev on a concrete *net.TCPConn — the enabling
// interface inside package net is unexported, so no wrapper can implement it.
// Everything else is coalesced into one contiguous write and chunked at the
// TLS record size. That covers TLS, any wrapped connection, and every write
// on Windows, which is a large share of real deployments running a code path
// the TCP tests never reach.

// tlsPair returns two connected TLS connections whose handshake has already
// completed.
//
// The handshakes run concurrently because each side needs the other to make
// progress. A tls.Listener's Accept returns before its handshake, which is
// driven lazily by the first read or write, so dialing and only then reading
// the accepted connection leaves the dialer waiting for a peer that nobody
// has touched.
func tlsPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	serverCfg, clientCfg := testTLSConfigs(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	type res struct {
		c   net.Conn
		err error
	}
	accepted := make(chan res, 1)
	go func() {
		c, err := ln.Accept()
		accepted <- res{c, err}
	}()
	rawClient, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	r := <-accepted
	if r.err != nil {
		t.Fatal(r.err)
	}

	cc := tls.Client(rawClient, clientCfg)
	sc := tls.Server(r.c, serverCfg)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	errs := make(chan error, 2)
	go func() { errs <- cc.HandshakeContext(ctx) }()
	go func() { errs <- sc.HandshakeContext(ctx) }()
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("TLS handshake: %v", err)
		}
	}
	t.Cleanup(func() { cc.Close(); sc.Close() })
	return cc, sc
}

// TestConformanceOverTLS runs the shared suite with a real tls.Conn on both
// ends, which is how this library is most often deployed: multiplexing over a
// connection that is already encrypted.
func TestConformanceOverTLS(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (portunus.Session, portunus.Session) {
		cc, sc := tlsPair(t)
		return startPair(t, cc, sc)
	})
}

// TestConformanceOverUnix covers a stream carrier that is neither TCP nor
// TLS: the local-socket case, where the coalesce path also applies.
func TestConformanceOverUnix(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Modern Windows has AF_UNIX, but path handling and cleanup differ
		// enough that a failure here would say more about the test than the
		// library. The coalesce path it exercises is covered there by TLS.
		t.Skip("unix sockets behave differently on Windows")
	}
	conformance.Run(t, func(t *testing.T) (portunus.Session, portunus.Session) {
		// Socket paths are length-limited on some platforms, so keep it
		// short rather than using the full test name.
		dir, err := os.MkdirTemp("", "prtns")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		addr := filepath.Join(dir, "s")

		ln, err := net.Listen("unix", addr)
		if err != nil {
			t.Fatal(err)
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
		cc, err := net.Dial("unix", addr)
		if err != nil {
			t.Fatal(err)
		}
		r := <-ch
		if r.err != nil {
			t.Fatal(r.err)
		}

		return startPair(t, cc, r.c)
	})
}

// TestTLSCarrierMovesBulkIntact is the volume case for the coalesce path:
// enough data to span many TLS records and many batches, checked byte for
// byte. Chunking that splits or duplicates a record boundary shows up here
// and nowhere else.
func TestTLSCarrierMovesBulkIntact(t *testing.T) {
	cc, sc := tlsPair(t)
	client, server := startPair(t, cc, sc)

	// Several times the 16KB record size and the default window, so record
	// chunking, batching, and window updates all cycle repeatedly.
	payload := make([]byte, 3<<20)
	for i := range payload {
		payload[i] = byte(i*7 + i/1021)
	}
	echoAndCompare(t, client, server, payload)
}

// startPair builds a session pair over an already-connected carrier.
func startPair(t *testing.T, cc, sc net.Conn) (portunus.Session, portunus.Session) {
	t.Helper()
	type res struct {
		s   *portunus.NativeSession
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := portunus.Server(sc, nil)
		ch <- res{s, err}
	}()
	client, err := portunus.Client(cc, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() { client.Close(); r.s.Close() })
	return client, r.s
}

// testTLSConfigs returns matched configs using a throwaway self-signed
// certificate.
func testTLSConfigs(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "portunus-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
	)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	return &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS13,
		}, &tls.Config{
			RootCAs:    pool,
			ServerName: "127.0.0.1",
			MinVersion: tls.VersionTLS13,
		}
}

// echoAndCompare sends payload through an echo on the peer and checks every
// byte returns intact.
func echoAndCompare(t *testing.T, client, server portunus.Session, payload []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	go func() {
		st, err := server.AcceptStream(ctx)
		if err != nil {
			return
		}
		io.Copy(st, st)
		st.CloseWrite()
	}()

	st, err := client.OpenStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		st.Write(payload)
		st.CloseWrite()
	}()

	got, err := io.ReadAll(st)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(payload) {
		t.Fatalf("echoed %d bytes, want %d", len(got), len(payload))
	}
	if !bytes.Equal(got, payload) {
		for i := range got {
			if got[i] != payload[i] {
				t.Fatalf("first difference at byte %d: got %d want %d", i, got[i], payload[i])
			}
		}
	}
}
