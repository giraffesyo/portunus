package portunus_test

import (
	"net"
	"testing"

	"github.com/giraffesyo/portunus"
	"github.com/giraffesyo/portunus/conformance"
)

// The native TCP session must satisfy the same shared semantics as the QUIC
// adapter; adapters/quic runs this identical suite against QUIC.
func TestConformanceNative(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (portunus.Session, portunus.Session) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
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
		cc, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		r := <-ch
		if r.err != nil {
			t.Fatal(r.err)
		}

		client, err := portunus.Client(cc, nil)
		if err != nil {
			t.Fatal(err)
		}
		server, err := portunus.Server(r.c, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close(); server.Close() })
		return client, server
	})
}
