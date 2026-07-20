package quic_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/giraffesyo/mux"
	quicadapter "github.com/giraffesyo/mux/adapters/quic"
	"github.com/giraffesyo/mux/conformance"
	quicgo "github.com/quic-go/quic-go"
)

// The QUIC adapter runs the same conformance suite as the native TCP
// session. Both satisfying the interfaces at compile time proves nothing
// about behavior; this is what makes them genuinely interchangeable.
func TestConformanceQUIC(t *testing.T) {
	conformance.Run(t, quicPair)
}

func quicPair(t *testing.T) (mux.Session, mux.Session) {
	t.Helper()
	serverTLS, clientTLS := testTLS(t)

	ln, err := quicadapter.Listen("127.0.0.1:0", serverTLS, &quicgo.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	type res struct {
		s   *quicadapter.Session
		err error
	}
	ch := make(chan res, 1)
	go func() {
		s, err := ln.Accept(context.Background())
		ch <- res{s, err}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := quicadapter.Dial(ctx, ln.Addr().String(), clientTLS, &quicgo.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	// A QUIC connection carries no streams until one is opened, so the
	// server side only materializes once the handshake completes.
	if _, err := client.Conn().OpenUniStream(); err != nil {
		t.Fatal(err)
	}
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	t.Cleanup(func() { client.Close(); r.s.Close() })
	return client, r.s
}

// testTLS returns a matched server/client config using a throwaway
// self-signed certificate.
func testTLS(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "mux-conformance"},
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
	pool := x509.NewCertPool()
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool.AddCert(leaf)

	return &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"mux-conformance"},
			MinVersion:   tls.VersionTLS13,
		}, &tls.Config{
			RootCAs:    pool,
			ServerName: "127.0.0.1",
			NextProtos: []string{"mux-conformance"},
			MinVersion: tls.VersionTLS13,
		}
}
