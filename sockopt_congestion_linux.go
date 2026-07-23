//go:build linux

package portunus

import (
	"net"
	"syscall"
)

// tcpCongestion is TCP_CONGESTION. Like TCP_NOTSENT_LOWAT it is absent from
// the frozen syscall package, so it is declared here rather than taking a
// dependency on golang.org/x/sys.
const tcpCongestion = 13

// applyCongestionControl selects the carrier's congestion-control algorithm
// by name, for algorithms the kernel permits an unprivileged socket to set
// (those in net.ipv4.tcp_allowed_congestion_control).
//
// The motivation is bufferbloat. A loss-based controller such as CUBIC finds
// its rate by filling the bottleneck queue until it drops, so at a window
// large enough for full throughput it also runs with a full queue — and a
// small message sharing the connection waits behind that queue however well
// this library schedules its own frames. BASELINE.md measures the effect: our
// autotuned default trades tens of milliseconds of tail latency for almost no
// throughput once the queue is full. BBR paces to hold roughly a
// bandwidth-delay product in flight without filling the queue, which is the
// only way to keep throughput high and that queue shallow at the same time on
// a single connection.
//
// Best effort, like applyNotSentLowat: an unavailable algorithm (the name is
// not in the allowed list, or the module is not loaded) returns an error the
// caller may log, but the session runs correctly on whatever the system
// default is.
func applyCongestionControl(conn net.Conn, algo string) error {
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
		setErr = syscall.SetsockoptString(int(fd), syscall.IPPROTO_TCP, tcpCongestion, algo)
	}); err != nil {
		return err
	}
	return setErr
}
