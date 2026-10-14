package main

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestAggregateSeparatesCarriers(t *testing.T) {
	input := strings.NewReader(`{"carrier":"wrapped","label":"bulk","impl":"yamux","scenario":"bulk","yamux_window":8388608,"mb_per_sec":100}
{"carrier":"tls","label":"bulk","impl":"yamux","scenario":"bulk","yamux_window":8388608,"mb_per_sec":50}
`)
	var out strings.Builder
	if err := aggregate(input, &out); err != nil {
		t.Fatal(err)
	}
	for _, carrier := range []string{"wrapped", "tls"} {
		if !strings.Contains(out.String(), "yamux/"+carrier+" (8192KB win) | 1 |") {
			t.Fatalf("carrier results were combined or mislabeled:\n%s", out.String())
		}
	}
}

// Delivery confirmation needs a real half-close even when raw TCP is
// wrapped: the peer must see EOF while the reply direction remains open.
func TestWrappedRawHalfClose(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	deadline := time.Now().Add(5 * time.Second)
	if err := client.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := server.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	st := rawStream{c: wrappedCarrier{client}}
	if _, err := st.Write([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := st.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(server)
	if err != nil || string(got) != "payload" {
		t.Fatalf("peer read %q, %v; want payload and EOF", got, err)
	}
	if _, err := server.Write([]byte("ack")); err != nil {
		t.Fatal(err)
	}
	var ack [3]byte
	if _, err := io.ReadFull(st, ack[:]); err != nil || string(ack[:]) != "ack" {
		t.Fatalf("reply %q, %v; want ack", ack, err)
	}
}
