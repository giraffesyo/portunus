//go:build linux || darwin

package portunus

import (
	"net"
	"syscall"
)

// applyNotSentLowat caps how much unsent data the kernel will hold in the
// socket send buffer. Without it, bulk writers fill that buffer and a small
// message queued behind them waits for the whole thing to drain: tail
// latency then reflects kernel bufferbloat rather than our own queue, which
// is the one queue our batching and fairness caps cannot reach.
//
// Failures are reported but are never fatal to the caller — the option is an
// optimization, and a kernel without it still works correctly, just with
// deeper queues.
func applyNotSentLowat(conn net.Conn, bytes int) error {
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return nil
	}
	raw, err := tcp.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpNotSentLowat, bytes)
	}); err != nil {
		return err
	}
	return setErr
}
