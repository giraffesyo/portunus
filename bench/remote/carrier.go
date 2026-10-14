package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	"github.com/giraffesyo/portunus"
)

// A wrapper keeps the socket's internal vectored-write method hidden, as
// production tracing and metering wrappers commonly do.
type wrappedCarrier struct{ net.Conn }

func (c wrappedCarrier) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return fmt.Errorf("carrier %T cannot half-close", c.Conn)
}

var benchmarkCertificate = sync.OnceValues(func() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
})

// The preamble travels on plain TCP, then both ends wrap before the mux
// starts. TLS uses a throwaway certificate and no peer authentication: this
// is a benchmark on a trusted test network, not a production TLS setup.
func wrapCarrier(c net.Conn, carrier string, server bool) (net.Conn, error) {
	switch carrier {
	case "", "tcp":
		return c, nil
	case "wrapped":
		return wrappedCarrier{c}, nil
	case "tls", "tls-batched":
		if carrier == "tls-batched" {
			c = portunus.Batched(c)
		}
		cfg := &tls.Config{MinVersion: tls.VersionTLS13}
		var tc *tls.Conn
		if server {
			cert, err := benchmarkCertificate()
			if err != nil {
				return nil, err
			}
			cfg.Certificates = []tls.Certificate{cert}
			tc = tls.Server(c, cfg)
		} else {
			cfg.InsecureSkipVerify = true // Ephemeral benchmark certificate.
			tc = tls.Client(c, cfg)
		}
		if err := tc.SetDeadline(time.Now().Add(30 * time.Second)); err != nil {
			return nil, err
		}
		if err := tc.Handshake(); err != nil {
			return nil, err
		}
		if err := tc.SetDeadline(time.Time{}); err != nil {
			return nil, err
		}
		return tc, nil
	default:
		return nil, fmt.Errorf("unknown carrier %q", carrier)
	}
}
