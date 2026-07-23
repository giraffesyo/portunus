//go:build linux

package portunus

import (
	"bytes"
	"io"
	"testing"
)

// Selecting a congestion-control algorithm the kernel offers must apply
// cleanly and leave the session fully functional. "cubic" is the near
// universal Linux default, so it is available wherever this test runs; the
// point is that the TCP_CONGESTION passthrough sets it without disturbing the
// carrier, not that cubic is special.
func TestCongestionControlAppliesAndCarriesData(t *testing.T) {
	client, server := pair(t, &Config{CongestionControl: "cubic"})
	roundtripOneStream(t, client, server, []byte("congestion control is set on this carrier"))
}

// An algorithm the kernel does not offer must be ignored, not fatal: the
// option is an optimization, and a session that cannot get its preferred
// controller still has to work on the system default. A deliberately
// nonexistent name exercises the SetsockoptString error path.
func TestCongestionControlUnavailableIsNotFatal(t *testing.T) {
	client, server := pair(t, &Config{CongestionControl: "nonesuch-cc-algo"})
	roundtripOneStream(t, client, server, []byte("still works on the default controller"))
}

// roundtripOneStream opens a stream, sends payload, and checks it echoes back
// intact — enough to prove the carrier is healthy after a socket option was
// applied to it.
func roundtripOneStream(t *testing.T, client, server *NativeSession, payload []byte) {
	t.Helper()
	c := ctx(t)

	go func() {
		st, err := server.AcceptStream(c)
		if err != nil {
			return
		}
		io.Copy(st, st)
		st.CloseWrite()
	}()

	st, err := client.OpenStream(c)
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
	if !bytes.Equal(got, payload) {
		t.Fatalf("echoed %q, want %q", got, payload)
	}
}
